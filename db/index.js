var crypto = require('crypto'),
    lazy = require('lazy'),
    levelup = require('levelup'),
    memdown = require('memdown'),
    sublevel = require('level-sublevel'),
    Lock = require('lock'),
    Big = require('big.js')

exports.MAX_SIZE = 409600 // TODO: get rid of this? or leave for backwards compat?
exports.create = create
exports.lazy = lazyStream
exports.validateKey = validateKey
exports.validateKeyPiece = validateKeyPiece
exports.validateKeyPaths = validateKeyPaths
exports.validateItem = validateItem
exports.traverseKey = traverseKey
exports.traverseIndexes = traverseIndexes
exports.toLexiStr = toLexiStr
exports.hashPrefix = hashPrefix
exports.itemCompare = itemCompare
exports.validationError = validationError
exports.checkConditional = checkConditional
exports.itemSize = itemSize
exports.capacityUnits = capacityUnits
exports.addConsumedCapacity = addConsumedCapacity
exports.matchesFilter = matchesFilter
exports.matchesExprFilter = matchesExprFilter
exports.compare = compare
exports.mapPaths = mapPaths
exports.mapPath = mapPath

function create(options) {
  options = options || {}
  if (options.createTableMs == null) options.createTableMs = 500
  if (options.deleteTableMs == null) options.deleteTableMs = 500
  if (options.updateTableMs == null) options.updateTableMs = 500
  if (options.maxItemSizeKb == null) options.maxItemSizeKb = exports.MAX_SIZE / 1024
  options.maxItemSize = options.maxItemSizeKb * 1024

  var db = levelup(options.path || '/does/not/matter', options.path ? {} : {db: memdown}),
      sublevelDb = sublevel(db),
      tableDb = sublevelDb.sublevel('table', {valueEncoding: 'json'}),
      subDbs = Object.create(null)

  tableDb.lock = new Lock()

  // XXX: Is there a better way to get this?
  tableDb.awsAccountId = (process.env.AWS_ACCOUNT_ID || '0000-0000-0000').replace(/[^\d]/g, '')
  tableDb.awsRegion = process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION || 'us-east-1'

  function getItemDb(name) {
    return getSubDb('item-' + name)
  }

  function deleteItemDb(name, cb) {
    deleteSubDb('item-' + name, cb)
  }

  function getSubDb(name) {
    if (!subDbs[name]) {
      subDbs[name] = sublevelDb.sublevel(name, {valueEncoding: 'json'})
      subDbs[name].lock = new Lock()
    }
    return subDbs[name]
  }

  function deleteSubDb(name, cb) {
    var subDb = getSubDb(name)
    delete subDbs[name]
    lazyStream(subDb.createKeyStream(), cb).join(function(keys) {
      subDb.batch(keys.map(function(key) { return {type: 'del', key: key} }), cb)
    })
  }

  function getTable(name, checkStatus, cb) {
    if (typeof checkStatus == 'function') cb = checkStatus

    tableDb.get(name, function(err, table) {
      if (!err && checkStatus && (table.TableStatus == 'CREATING' || table.TableStatus == 'DELETING')) {
        err = new Error('NotFoundError')
        err.name = 'NotFoundError'
      }
      if (err) {
        if (err.name == 'NotFoundError') {
          err.statusCode = 400
          err.body = {
            __type: 'com.amazonaws.dynamodb.v20120810#ResourceNotFoundException',
            message: 'Requested resource not found',
          }
          if (!checkStatus) err.body.message += ': Table: ' + name + ' not found'
        }
        return cb(err)
      }

      cb(null, table)
    })
  }

  function recreate() {
    var self = this, newStore = create(options)
    Object.keys(newStore).forEach(function(key) {
      self[key] = newStore[key]
    })
  }

  return {
    options: options,
    db: db,
    tableDb: tableDb,
    getItemDb: getItemDb,
    deleteItemDb: deleteItemDb,
    getTable: getTable,
    recreate: recreate,
  }
}

function lazyStream(stream, errHandler) {
  if (errHandler) stream.on('error', errHandler)
  var streamAsLazy = lazy(stream)
  if (stream.destroy) streamAsLazy.on('pipe', stream.destroy.bind(stream))
  return streamAsLazy
}

function validateKey(dataKey, table, keySchema) {
  if (keySchema == null) keySchema = table.KeySchema
  if (keySchema.length != Object.keys(dataKey).length) {
    return validationError('The provided key element does not match the schema')
  }

  var keyStr, err
  err = traverseKey(table, keySchema, function(attr, type, isHash) {
    var err = validateKeyPiece(dataKey, attr, type, isHash)
    if (err) return err
    if (!keyStr) keyStr = hashPrefix(dataKey[attr][type], type)
    keyStr += '~' + toLexiStr(dataKey[attr][type], type)
  })
  return err || keyStr
}

function validateKeyPiece(key, attr, type, isHash) {
  if (key[attr] == null || key[attr][type] == null) {
    return validationError('The provided key element does not match the schema')
  }
  return checkKeySize(key[attr][type], type, isHash)
}

function validateKeyPaths(nestedPaths, table) {
  return traverseKey(table, function(attr) {
    if (nestedPaths[attr]) {
      return validationError('Key attributes must be scalars; ' +
        'list random access \'[]\' and map lookup \'.\' are not allowed: Key: ' + attr)
    }
  }) || traverseIndexes(table, function(attr) {
    if (nestedPaths[attr]) {
      return validationError('Key attributes must be scalars; ' +
        'list random access \'[]\' and map lookup \'.\' are not allowed: IndexKey: ' + attr)
    }
  })
}

function validateItem(dataItem, table) {
  var keyStr, err
  err = traverseKey(table, function(attr, type, isHash) {
    if (dataItem[attr] == null) {
      return validationError('One or more parameter values were invalid: ' +
        'Missing the key ' + attr + ' in the item')
    }
    if (dataItem[attr][type] == null) {
      return validationError('One or more parameter values were invalid: ' +
        'Type mismatch for key ' + attr + ' expected: ' + type +
        ' actual: ' + Object.keys(dataItem[attr])[0])
    }
    err = checkKeySize(dataItem[attr][type], type, isHash)
    if (err) return err
    if (!keyStr) keyStr = hashPrefix(dataItem[attr][type], type)
    keyStr += '~' + toLexiStr(dataItem[attr][type], type)
  })
  if (err) return err
  err = traverseIndexes(table, function(attr, type, index) {
    if (dataItem[attr] != null && dataItem[attr][type] == null) {
      return validationError('One or more parameter values were invalid: ' +
        'Type mismatch for Index Key ' + attr + ' Expected: ' + type +
        ' Actual: ' + Object.keys(dataItem[attr])[0] + ' IndexName: ' + index.IndexName)
    }
  })
  return err || keyStr
}

function traverseKey(table, keySchema, visitKey) {
  if (typeof keySchema == 'function') { visitKey = keySchema; keySchema = table.KeySchema }
  var i, j, attr, type, found
  for (i = 0; i < keySchema.length; i++) {
    attr = keySchema[i].AttributeName
    for (j = 0; j < table.AttributeDefinitions.length; j++) {
      if (table.AttributeDefinitions[j].AttributeName != attr) continue
      type = table.AttributeDefinitions[j].AttributeType
      break
    }
    found = visitKey(attr, type, !i)
    if (found) return found
  }
}

function traverseIndexes(table, visitIndex) {
  var i, j, k, attr, type, found
  if (table.GlobalSecondaryIndexes) {
    for (i = table.GlobalSecondaryIndexes.length - 1; i >= 0; i--) {
      for (k = 0; k < table.GlobalSecondaryIndexes[i].KeySchema.length; k++) {
        attr = table.GlobalSecondaryIndexes[i].KeySchema[k].AttributeName
        for (j = 0; j < table.AttributeDefinitions.length; j++) {
          if (table.AttributeDefinitions[j].AttributeName != attr) continue
          type = table.AttributeDefinitions[j].AttributeType
          break
        }
        found = visitIndex(attr, type, table.GlobalSecondaryIndexes[i], true)
        if (found) return found
      }
    }
  }
  if (table.LocalSecondaryIndexes) {
    for (i = table.LocalSecondaryIndexes.length - 1; i >= 0; i--) {
      attr = table.LocalSecondaryIndexes[i].KeySchema[1].AttributeName
      for (j = 0; j < table.AttributeDefinitions.length; j++) {
        if (table.AttributeDefinitions[j].AttributeName != attr) continue
        type = table.AttributeDefinitions[j].AttributeType
        break
      }
      found = visitIndex(attr, type, table.LocalSecondaryIndexes[i], false)
      if (found) return found
    }
  }
}

function checkKeySize(keyPiece, type, isHash) {
  // Numbers are always fine
  if (type == 'N') return null
  if (type == 'B') keyPiece = new Buffer(keyPiece, 'base64')
  if (isHash && keyPiece.length > 2048)
    return validationError('One or more parameter values were invalid: ' +
      'Size of hashkey has exceeded the maximum size limit of2048 bytes')
  else if (!isHash && keyPiece.length > 1024)
    return validationError('One or more parameter values were invalid: ' +
      'Aggregated size of all range keys has exceeded the size limit of 1024 bytes')
}

// Creates lexigraphically sortable number strings
//  0     7c    009   = '07c009' = -99.1
// |-|   |--| |-----|
// sign  exp  digits
//
// Sign is 0 for negative, 1 for positive
// Exp is hex for the exponent modified by adding 130 if sign is positive or subtracting from 125 if negative
// Digits are unchanged if sign is positive, or added to 10 if negative
// Hence, in '07c009', the sign is negative, exponent is 125 - 124 = 1, digits are 10 + -0.09 = 9.91 => -9.91e1
//
function toLexiStr(keyPiece, type) {
  if (keyPiece == null) return ''
  if (type == 'B') return new Buffer(keyPiece, 'base64').toString('hex')
  if (type != 'N') return keyPiece
  var bigNum = new Big(keyPiece), digits,
      exp = !bigNum.c[0] ? 0 : bigNum.s == -1 ? 125 - bigNum.e : 130 + bigNum.e
  if (bigNum.s == -1) {
    bigNum.e = 0
    digits = new Big(10).plus(bigNum).toFixed().replace(/\./, '')
  } else {
    digits = bigNum.c.join('')
  }
  return (bigNum.s == -1 ? '0' : '1') + ('0' + exp.toString(16)).slice(-2) + digits
}

function hashPrefix(hashKey, hashType, rangeKey, rangeType) {
  if (hashType == 'S') {
    hashKey = new Buffer(hashKey, 'utf8')
  } else if (hashType == 'N') {
    hashKey = numToBuffer(hashKey)
  } else if (hashType == 'B') {
    hashKey = new Buffer(hashKey, 'base64')
  }
  if (rangeKey) {
    if (rangeType == 'S') {
      rangeKey = new Buffer(rangeKey, 'utf8')
    } else if (rangeType == 'N') {
      rangeKey = numToBuffer(rangeKey)
    } else if (rangeType == 'B') {
      rangeKey = new Buffer(rangeKey, 'base64')
    }
  } else {
    rangeKey = new Buffer(0)
  }
  // TODO: Can use the whole hash if we deem it important - for now just first six chars
  return crypto.createHash('md5').update('Outliers').update(hashKey).update(rangeKey).digest('hex').slice(0, 6)
}

function numToBuffer(num) {
  if (+num === 0) return new Buffer([-128])

  num = new Big(num)

  var scale = num.s, mantissa = num.c, exponent = num.e + 1, appendZero = exponent % 2 ? 1 : 0,
      byteArrayLengthWithoutExponent = Math.floor((mantissa.length + appendZero + 1) / 2),
      byteArray, appendedZero = false, mantissaIndex, byteArrayIndex

  if (byteArrayLengthWithoutExponent < 20 && scale == -1) {
    byteArray = new Array(byteArrayLengthWithoutExponent + 2)
    byteArray[byteArrayLengthWithoutExponent + 1] = 102
  } else {
    byteArray = new Array(byteArrayLengthWithoutExponent + 1)
  }

  byteArray[0] = Math.floor((exponent + appendZero) / 2) - 64
  if (scale == -1)
    byteArray[0] ^= 0xffffffff

  for (mantissaIndex = 0; mantissaIndex < mantissa.length; mantissaIndex++) {
    byteArrayIndex = Math.floor((mantissaIndex + appendZero) / 2) + 1
    if (appendZero && !mantissaIndex && !appendedZero) {
      byteArray[byteArrayIndex] = 0
      appendedZero = true
      mantissaIndex--
    } else if ((mantissaIndex + appendZero) % 2 === 0) {
      byteArray[byteArrayIndex] = mantissa[mantissaIndex] * 10
    } else {
      byteArray[byteArrayIndex] += mantissa[mantissaIndex]
    }
    if (((mantissaIndex + appendZero) % 2) || (mantissaIndex == mantissa.length - 1)) {
      if (scale == -1)
        byteArray[byteArrayIndex] = 101 - byteArray[byteArrayIndex]
      else
        byteArray[byteArrayIndex]++
    }
  }

  return new Buffer(byteArray)
}

function itemCompare(rangeKey, table) {
  return function(item1, item2) {
    var val1, val2, rangeType, tableHashKey, tableRangeKey, tableHashType, tableRangeType,
        hashVal1, rangeVal1, hashVal2, rangeVal2
    if (rangeKey) {
      rangeType = Object.keys(item1[rangeKey] || item2[rangeKey] || {})[0]
      rangeVal1 = (item1[rangeKey] || {})[rangeType]
      rangeVal2 = (item2[rangeKey] || {})[rangeType]
      val1 = toLexiStr(rangeVal1, rangeType)
      val2 = toLexiStr(rangeVal2, rangeType)
    }
    if (!rangeKey || val1 == val2) {
      tableHashKey = table.KeySchema[0].AttributeName
      tableRangeKey = (table.KeySchema[1] || {}).AttributeName
      tableHashType = Object.keys(item1[tableHashKey] || item2[tableHashKey] || {})[0]
      tableRangeType = Object.keys(item1[tableRangeKey] || item2[tableRangeKey] || {})[0]
      hashVal1 = item1[tableHashKey][tableHashType]
      rangeVal1 = (item1[tableRangeKey] || {})[tableRangeType]
      hashVal2 = item2[tableHashKey][tableHashType]
      rangeVal2 = (item2[tableRangeKey] || {})[tableRangeType]
      val1 = hashPrefix(hashVal1, tableHashType, rangeVal1, tableRangeType)
      val2 = hashPrefix(hashVal2, tableHashType, rangeVal2, tableRangeType)
    }
    return val1 < val2 ? -1 : val1 > val2 ? 1 : 0
  }
}

function checkConditional(data, existingItem) {
  existingItem = existingItem || {}

  if (data._condition) {
    if (!matchesExprFilter(existingItem, data._condition.expression)) {
      return conditionalError()
    }
    return null
  } else if (!data.Expected) {
    return null
  }
  if (!matchesFilter(existingItem, data.Expected, data.ConditionalOperator)) {
    return conditionalError()
  }
}

function validationError(msg) {
  var err = new Error(msg)
  err.statusCode = 400
  err.body = {
    __type: 'com.amazon.coral.validate#ValidationException',
    message: msg,
  }
  return err
}

function conditionalError(msg) {
  if (msg == null) msg = 'The conditional request failed'
  var err = new Error(msg)
  err.statusCode = 400
  err.body = {
    __type: 'com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException',
    message: msg,
  }
  return err
}

function itemSize(item, skipAttr) {
  return Object.keys(item).reduce(function(sum, attr) {
    return sum + (skipAttr ? 2 : attr.length) + valSize(item[attr], skipAttr)
  }, 0)
}

function valSize(itemAttr, skipAttr) {
  var type = Object.keys(itemAttr)[0]
  var val = itemAttr[type]
  switch (type) {
    case 'S':
      return val.length
    case 'B':
      return new Buffer(val, 'base64').length
    case 'N':
      val = new Big(val)
      return Math.ceil(val.c.length / 2) + (val.e % 2 ? 1 : 2)
    case 'SS':
      return val.reduce(function(sum, x) { return sum + x.length }, skipAttr ? val.length : 0) // eslint-disable-line no-loop-func
    case 'BS':
      return val.reduce(function(sum, x) { return sum + new Buffer(x, 'base64').length }, skipAttr ? val.length : 0) // eslint-disable-line no-loop-func
    case 'NS':
      return val.reduce(function(sum, x) { // eslint-disable-line no-loop-func
        x = new Big(x)
        return sum + Math.ceil(x.c.length / 2) + (x.e % 2 ? 1 : 2)
      }, skipAttr ? val.length : 0)
    case 'NULL':
      return 1
    case 'BOOL':
      return 1
    case 'L':
      return 3 + val.reduce(function(sum, val) { return sum + 1 + valSize(val, skipAttr) }, 0)
    case 'M':
      return 3 + Object.keys(val).length + itemSize(val, skipAttr)
  }
}

function capacityUnits(item, isRead, isConsistent) {
  var size = item ? Math.ceil(itemSize(item) / 1024 / (isRead ? 4 : 1)) : 1
  return size / (!isRead || isConsistent ? 1 : 2)
}

function addConsumedCapacity(data, isRead, newItem, oldItem) {
  if (~['TOTAL', 'INDEXES'].indexOf(data.ReturnConsumedCapacity)) {
    var capacity = capacityUnits(newItem, isRead, data.ConsistentRead)
    if (oldItem != null) {
      capacity = Math.max(capacity, capacityUnits(oldItem, isRead, data.ConsistentRead))
    }
    return {
      CapacityUnits: capacity,
      TableName: data.TableName,
      Table: data.ReturnConsumedCapacity == 'INDEXES' ? {CapacityUnits: capacity} : undefined,
    }
  }
}

function valsEqual(val1, val2) {
  if (Array.isArray(val1) && Array.isArray(val2)) {
    if (val1.length != val2.length) return false
    return val1.every(function(val) { return ~val2.indexOf(val) })
  } else {
    return val1 == val2
  }
}

function matchesFilter(val, filter, conditionalOperator) {
  for (var attr in filter) {
    var comp = filter[attr].Exists != null ? (filter[attr].Exists ? 'NOT_NULL' : 'NULL') :
      filter[attr].ComparisonOperator || 'EQ'
    var result = compare(comp, val[attr], filter[attr].AttributeValueList || filter[attr].Value)
    if (!result) {
      return false
    } else if (conditionalOperator == 'OR') {
      return true
    }
  }
  return true
}

function matchesExprFilter(item, expr) {
  if (expr.type == 'and') {
    return matchesExprFilter(item, expr.args[0]) && matchesExprFilter(item, expr.args[1])
  } else if (expr.type == 'or') {
    return matchesExprFilter(item, expr.args[0]) || matchesExprFilter(item, expr.args[1])
  } else if (expr.type == 'not') {
    return !matchesExprFilter(item, expr.args[0])
  }
  var args = expr.args.map(function(arg) { return resolveArg(arg, item) })
  return compare(expr.type == 'function' ? expr.name : expr.type, args[0], args.slice(1))
}

function resolveArg(arg, item) {
  if (Array.isArray(arg)) {
    return mapPath(arg, item)
  } else if (arg.type == 'function' && arg.name == 'size') {
    var args = arg.args.map(function(arg) { return resolveArg(arg, item) })
    var val = args[0], length
    if (!val) {
      return null
    } else if (val.S) {
      length = val.S.length
    } else if (val.B) {
      length = new Buffer(val.B, 'base64').length
    } else if (val.SS || val.BS || val.NS || val.L) {
      length = (val.SS || val.BS || val.NS || val.L).length
    } else if (val.M) {
      length = Object.keys(val.M).length
    }
    return length != null ? {N: length.toString()} : null
  } else {
    return arg
  }
}

function compare(comp, val, compVals) {
  if (!Array.isArray(compVals)) compVals = [compVals]

  var attrType = val ? Object.keys(val)[0] : null
  var attrVal = attrType ? val[attrType] : null
  var compType = compVals && compVals[0] ? Object.keys(compVals[0])[0] : null
  var compVal = compType ? compVals[0][compType] : null

  switch (comp) {
    case 'EQ':
    case '=':
      if (compType != attrType || !valsEqual(attrVal, compVal)) return false
      break
    case 'NE':
    case '<>':
      if (compType == attrType && valsEqual(attrVal, compVal)) return false
      break
    case 'LE':
    case '<=':
      if (compType != attrType ||
        (attrType == 'N' && !new Big(attrVal).lte(compVal)) ||
        (attrType != 'N' && toLexiStr(attrVal, attrType) > toLexiStr(compVal, attrType))) return false
      break
    case 'LT':
    case '<':
      if (compType != attrType ||
        (attrType == 'N' && !new Big(attrVal).lt(compVal)) ||
        (attrType != 'N' && toLexiStr(attrVal, attrType) >= toLexiStr(compVal, attrType))) return false
      break
    case 'GE':
    case '>=':
      if (compType != attrType ||
        (attrType == 'N' && !new Big(attrVal).gte(compVal)) ||
        (attrType != 'N' && toLexiStr(attrVal, attrType) < toLexiStr(compVal, attrType))) return false
      break
    case 'GT':
    case '>':
      if (compType != attrType ||
        (attrType == 'N' && !new Big(attrVal).gt(compVal)) ||
        (attrType != 'N' && toLexiStr(attrVal, attrType) <= toLexiStr(compVal, attrType))) return false
      break
    case 'NOT_NULL':
    case 'attribute_exists':
      if (attrVal == null) return false
      break
    case 'NULL':
    case 'attribute_not_exists':
      if (attrVal != null) return false
      break
    case 'CONTAINS':
    case 'contains':
      if (compType == 'S') {
        if (attrType != 'S' && attrType != 'SS') return false
        if (!~attrVal.indexOf(compVal)) return false
      }
      if (compType == 'N') {
        if (attrType != 'NS') return false
        if (!~attrVal.indexOf(compVal)) return false
      }
      if (compType == 'B') {
        if (attrType != 'B' && attrType != 'BS') return false
        if (attrType == 'B') {
          attrVal = new Buffer(attrVal, 'base64').toString()
          compVal = new Buffer(compVal, 'base64').toString()
        }
        if (!~attrVal.indexOf(compVal)) return false
      }
      break
    case 'NOT_CONTAINS':
      if (compType == 'S' && (attrType == 'S' || attrType == 'SS') &&
          ~attrVal.indexOf(compVal)) return false
      if (compType == 'N' && attrType == 'NS' &&
          ~attrVal.indexOf(compVal)) return false
      if (compType == 'B') {
        if (attrType == 'B') {
          attrVal = new Buffer(attrVal, 'base64').toString()
          compVal = new Buffer(compVal, 'base64').toString()
        }
        if ((attrType == 'B' || attrType == 'BS') &&
            ~attrVal.indexOf(compVal)) return false
      }
      break
    case 'BEGINS_WITH':
    case 'begins_with':
      if (compType != attrType) return false
      if (compType == 'B') {
        attrVal = new Buffer(attrVal, 'base64').toString()
        compVal = new Buffer(compVal, 'base64').toString()
      }
      if (attrVal.indexOf(compVal) !== 0) return false
      break
    case 'IN':
    case 'in':
      if (!attrVal) return false
      if (!compVals.some(function(compVal) {
        compType = Object.keys(compVal)[0]
        compVal = compVal[compType]
        return compType == attrType && valsEqual(attrVal, compVal)
      })) return false
      break
    case 'BETWEEN':
    case 'between':
      if (!attrVal || compType != attrType ||
        (attrType == 'N' && (!new Big(attrVal).gte(compVal) || !new Big(attrVal).lte(compVals[1].N))) ||
        (attrType != 'N' && (toLexiStr(attrVal, attrType) < toLexiStr(compVal, attrType) ||
          toLexiStr(attrVal, attrType) > toLexiStr(compVals[1][compType], attrType)))) return false
      break
    case 'attribute_type':
      if (!attrVal || !valsEqual(attrType, compVal)) return false
  }
  return true
}

function mapPaths(paths, item) {
  var returnItem = Object.create(null), toSquash = []
  for (var i = 0; i < paths.length; i++) {
    var path = paths[i]
    var resolved = mapPath(path, item)
    if (resolved == null) {
      continue
    }
    var curItem = {M: returnItem}
    for (var j = 0; j < paths[i].length; j++) {
      var piece = path[j]
      if (typeof piece == 'number') {
        curItem.L = curItem.L || []
        if (piece > curItem.L.length && !~toSquash.indexOf(curItem)) {
          toSquash.push(curItem)
        }
        if (j < paths[i].length - 1) {
          curItem.L[piece] = curItem.L[piece] || {}
          curItem = curItem.L[piece]
        } else {
          curItem.L[piece] = resolved
        }
      } else {
        curItem.M = curItem.M || {}
        if (j < paths[i].length - 1) {
          curItem.M[piece] = curItem.M[piece] || {}
          curItem = curItem.M[piece]
        } else {
          curItem.M[piece] = resolved
        }
      }
    }
  }
  toSquash.forEach(function(obj) { obj.L = obj.L.filter(Boolean) })
  return returnItem
}

function mapPath(path, item) {
  var resolved = {M: item}
  for (var i = 0; i < path.length; i++) {
    var piece = path[i]
    if (typeof piece == 'number' && resolved.L) {
      resolved = resolved.L[piece]
    } else if (resolved.M) {
      resolved = resolved.M[piece]
    } else {
      resolved = null
    }
    if (resolved == null) {
      break
    }
  }
  return resolved
}
