var crypto = require('crypto'),
    events = require('events'),
    async = require('async'),
    Lazy = require('lazy'),
    levelup = require('levelup'),
    memdown = require('memdown'),
    sublevel = require('level-sublevel'),
    Lock = require('lock'),
    Big = require('big.js')

exports.MAX_SIZE = 409600 // TODO: get rid of this? or leave for backwards compat?
exports.create = create
exports.lazy = lazyStream
exports.validateKey = validateKey
exports.validateItem = validateItem
exports.validateUpdates = validateUpdates
exports.validateKeyPiece = validateKeyPiece
exports.validateKeyPaths = validateKeyPaths
exports.createKey = createKey
exports.createIndexKey = createIndexKey
exports.traverseKey = traverseKey
exports.traverseIndexes = traverseIndexes
exports.toRangeStr = toRangeStr
exports.toLexiStr = toLexiStr
exports.hashPrefix = hashPrefix
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
exports.queryTable = queryTable
exports.updateIndexes = updateIndexes
exports.getIndexActions = getIndexActions

function create(options) {
  options = options || {}
  if (options.createTableMs == null) options.createTableMs = 500
  if (options.deleteTableMs == null) options.deleteTableMs = 500
  if (options.updateTableMs == null) options.updateTableMs = 500
  if (options.maxItemSizeKb == null) options.maxItemSizeKb = exports.MAX_SIZE / 1024
  options.maxItemSize = options.maxItemSizeKb * 1024

  var db = levelup(options.path, options.path ? {} : {db: memdown}),
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

  function getIndexDb(indexType, tableName, indexName) {
    return getSubDb('index-' + indexType.toLowerCase() + '~' + tableName + '~' + indexName)
  }

  function deleteIndexDb(indexType, tableName, indexName, cb) {
    deleteSubDb('index-' + indexType.toLowerCase() + '~' + tableName + '~' + indexName, cb)
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
    getIndexDb: getIndexDb,
    deleteIndexDb: deleteIndexDb,
    getTable: getTable,
    recreate: recreate,
  }
}

function lazyStream(stream, errHandler) {
  if (errHandler) stream.on('error', errHandler)
  var streamAsLazy = new Lazy(stream)
  if (stream.destroy) streamAsLazy.on('pipe', stream.destroy.bind(stream))
  return streamAsLazy
}

function validateKey(dataKey, table, keySchema) {
  if (keySchema == null) keySchema = table.KeySchema
  if (keySchema.length != Object.keys(dataKey).length) {
    return validationError('The provided key element does not match the schema')
  }
  return traverseKey(table, keySchema, function(attr, type, isHash) {
    return validateKeyPiece(dataKey, attr, type, isHash)
  })
}

function validateItem(dataItem, table) {
  return traverseKey(table, function(attr, type, isHash) {
    if (dataItem[attr] == null) {
      return validationError('One or more parameter values were invalid: ' +
        'Missing the key ' + attr + ' in the item')
    }
    if (dataItem[attr][type] == null) {
      return validationError('One or more parameter values were invalid: ' +
        'Type mismatch for key ' + attr + ' expected: ' + type +
        ' actual: ' + Object.keys(dataItem[attr])[0])
    }
    return checkKeySize(dataItem[attr][type], type, isHash)
  }) || traverseIndexes(table, function(attr, type, index) {
    if (dataItem[attr] != null && dataItem[attr][type] == null) {
      return validationError('One or more parameter values were invalid: ' +
        'Type mismatch for Index Key ' + attr + ' Expected: ' + type +
        ' Actual: ' + Object.keys(dataItem[attr])[0] + ' IndexName: ' + index.IndexName)
    }
  })
}

function validateUpdates(attributeUpdates, expressionUpdates, table) {
  if (attributeUpdates == null && expressionUpdates == null) return

  return traverseKey(table, function(attr) {
    var hasKey = false
    if (expressionUpdates) {
      var sections = expressionUpdates.sections
      for (var j = 0; j < sections.length; j++) {
        if (sections[j].path[0] == attr) {
          hasKey = true
          break
        }
      }
    } else {
      hasKey = attributeUpdates[attr] != null
    }
    if (hasKey) {
      return validationError('One or more parameter values were invalid: ' +
        'Cannot update attribute ' + attr + '. This attribute is part of the key')
    }
  }) || traverseIndexes(table, function(attr, type, index) {
    var actualType
    if (expressionUpdates) {
      var sections = expressionUpdates.sections
      for (var i = 0; i < sections.length; i++) {
        var section = sections[i]
        if (section.path.length == 1 && section.path[0] == attr) {
          actualType = section.attrType
          break
        }
      }
    } else {
      actualType = attributeUpdates[attr] && attributeUpdates[attr].Value ?
        Object.keys(attributeUpdates[attr].Value)[0] : null
    }
    if (actualType != null && actualType != type) {
      return validationError('One or more parameter values were invalid: ' +
        'Type mismatch for Index Key ' + attr + ' Expected: ' + type +
        ' Actual: ' + actualType + ' IndexName: ' + index.IndexName)
    }
  }) || validateKeyPaths((expressionUpdates || {}).nestedPaths, table)
}

function validateKeyPiece(key, attr, type, isHash) {
  if (key[attr] == null || key[attr][type] == null) {
    return validationError('The provided key element does not match the schema')
  }
  return checkKeySize(key[attr][type], type, isHash)
}

function validateKeyPaths(nestedPaths, table) {
  if (!nestedPaths) return
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

function createKey(item, table, keySchema) {
  if (keySchema == null) keySchema = table.KeySchema
  var keyStr
  traverseKey(table, keySchema, function(attr, type, isHash) {
    if (isHash) keyStr = hashPrefix(item[attr][type], type) + '/'
    keyStr += toRangeStr(item[attr][type], type) + '/'
  })
  return keyStr
}

function createIndexKey(item, table, keySchema) {
  var tableKeyPieces = []
  traverseKey(table, function(attr, type) { tableKeyPieces.push(item[attr][type], type) })
  return createKey(item, table, keySchema) + hashPrefix.apply(this, tableKeyPieces)
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

function toRangeStr(keyPiece, type) {
  if (type == null) {
    type = Object.keys(keyPiece)[0]
    keyPiece = keyPiece[type]
  }
  if (type == 'S') return new Buffer(keyPiece, 'utf8').toString('hex')
  return toLexiStr(keyPiece, type)
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
  if (type == null) {
    type = Object.keys(keyPiece)[0]
    keyPiece = keyPiece[type]
  }
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

function itemSize(item, compress, addMetaSize, rangeKey) {
  // Size of compressed item (for checking query/scan limit) seems complicated,
  // probably due to some internal serialization format.
  var rangeKeySize = 0
  var size = Object.keys(item).reduce(function(sum, attr) {
    var size = valSizeWithStorage(item[attr], compress && attr != rangeKey)
    if (compress && attr == rangeKey) {
      rangeKeySize = size
      return sum
    }
    return sum + size + (compress ? 1 : attr.length)
  }, 0)
  return !addMetaSize ? size : 2 + size + ((1 + Math.floor((1 + size) / 3072)) * (18 + rangeKeySize))
}

function valSizeWithStorage(itemAttr, compress) {
  var type = Object.keys(itemAttr)[0]
  var val = itemAttr[type]
  var size = valSize(val, type, compress)
  if (!compress) return size
  switch (type) {
    case 'S':
      return size + (size < 128 ? 1 : size < 16384 ? 2 : 3)
    case 'B':
      return size + 1
    case 'N':
      return size + 1
    case 'SS':
      return size + val.length + 1
    case 'BS':
      return size + val.length + 1
    case 'NS':
      return size + val.length + 1
    case 'NULL':
      return 0
    case 'BOOL':
      return 1
    case 'L':
      return size
    case 'M':
      return size
  }
}

function valSize(val, type, compress) {
  switch (type) {
    case 'S':
      return val.length
    case 'B':
      return new Buffer(val, 'base64').length
    case 'N':
      val = new Big(val)
      var numDigits = val.c.length
      if (numDigits == 1 && val.c[0] === 0) return 1
      return 1 + Math.ceil(numDigits / 2) + (numDigits % 2 || val.e % 2 ? 0 : 1) + (val.s == -1 ? 1 : 0)
    case 'SS':
      return val.reduce(function(sum, x) { return sum + valSize(x, 'S') }, 0) // eslint-disable-line no-loop-func
    case 'BS':
      return val.reduce(function(sum, x) { return sum + valSize(x, 'B') }, 0) // eslint-disable-line no-loop-func
    case 'NS':
      return val.reduce(function(sum, x) { return sum + valSize(x, 'N') }, 0) // eslint-disable-line no-loop-func
    case 'NULL':
      return 1
    case 'BOOL':
      return 1
    case 'L':
      return 3 + val.reduce(function(sum, val) { return sum + 1 + valSizeWithStorage(val, compress) }, 0)
    case 'M':
      return 3 + Object.keys(val).length + itemSize(val, compress)
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
    if (!Array.isArray(path)) path = [path]
    var resolved = mapPath(path, item)
    if (resolved == null) {
      continue
    }
    if (path.length == 1) {
      returnItem[path[0]] = resolved
      continue
    }
    var curItem = {M: returnItem}
    for (var j = 0; j < path.length; j++) {
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
  if (path.length == 1) {
    return item[path[0]]
  }
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

function queryTable(store, table, data, opts, isLocal, fetchFromItemDb, startKeyNames, cb) {
  var itemDb = store.getItemDb(data.TableName), vals

  if (data.IndexName) {
    var indexDb = store.getIndexDb(isLocal ? 'local' : 'global', data.TableName, data.IndexName)
    vals = lazyStream(indexDb.createValueStream(opts), cb)
  } else {
    vals = lazyStream(itemDb.createValueStream(opts), cb)
  }

  var tableCapacity = 0, indexCapacity = 0,
    calculateCapacity = ~['TOTAL', 'INDEXES'].indexOf(data.ReturnConsumedCapacity)

  if (fetchFromItemDb) {
    var em = new events.EventEmitter
    var queue = async.queue(function(key, cb) {
      if (!key) {
        em.emit('end')
        return cb()
      }
      itemDb.get(key, function(err, item) {
        if (err) {
          em.emit('error', err)
          return cb(err)
        }
        if (calculateCapacity) tableCapacity += itemSize(item)
        em.emit('data', item)
        cb()
      })
    })
    var oldVals = vals
    vals = new Lazy(em)

    oldVals.map(function(item) {
      if (calculateCapacity) indexCapacity += itemSize(item)
      queue.push(createKey(item, table))
    }).once('pipe', queue.push.bind(queue, ''))
  }

  var size = 0, count = 0, rangeKey = table.KeySchema[1] && table.KeySchema[1].AttributeName

  vals = vals.takeWhile(function(val) {
    if (count >= data.Limit || size >= 1024 * 1024) {
      return false
    }

    if (calculateCapacity && !fetchFromItemDb) {
      var capacitySize = itemSize(val)
      if (data.IndexName) {
        indexCapacity += capacitySize
      } else {
        tableCapacity += capacitySize
      }
    }

    count++

    size += itemSize(val, true, true, rangeKey)

    return true
  })

  var queryFilter = data.QueryFilter || data.ScanFilter

  if (data._filter) {
    vals = vals.filter(function(val) { return matchesExprFilter(val, data._filter.expression) })
  } else if (queryFilter) {
    vals = vals.filter(function(val) { return matchesFilter(val, queryFilter, data.ConditionalOperator) })
  }

  var paths = data._projection ? data._projection.paths : data.AttributesToGet
  if (paths) {
    vals = vals.map(mapPaths.bind(this, paths))
  }

  vals.join(function(items) {
    var result = {ScannedCount: count}
    if (count >= data.Limit || size >= 1024 * 1024) {
      if (data.Limit) items.splice(data.Limit)
      if (items.length) {
        result.LastEvaluatedKey = startKeyNames.reduce(function(key, attr) {
          key[attr] = items[items.length - 1][attr]
          return key
        }, {})
      }
    }
    result.Count = items.length
    if (data.Select != 'COUNT') result.Items = items
    if (calculateCapacity) {
      var tableUnits = Math.ceil(tableCapacity / 1024 / 4) * (data.ConsistentRead ? 1 : 0.5)
      var indexUnits = Math.ceil(indexCapacity / 1024 / 4) * (data.ConsistentRead ? 1 : 0.5)
      result.ConsumedCapacity = {
        CapacityUnits: tableUnits + indexUnits,
        TableName: data.TableName,
      }
      if (data.ReturnConsumedCapacity == 'INDEXES') {
        result.ConsumedCapacity.Table = {CapacityUnits: tableUnits}
        if (data.IndexName) {
          var indexAttr = isLocal ? 'LocalSecondaryIndexes' : 'GlobalSecondaryIndexes'
          result.ConsumedCapacity[indexAttr] = {}
          result.ConsumedCapacity[indexAttr][data.IndexName] = {CapacityUnits: indexUnits}
        }
      }
    }
    cb(null, result)
  })
}

function updateIndexes(store, table, existingItem, item, cb) {
  if (!existingItem && !item) return cb()
  var puts = [], deletes = []
  ;['Local', 'Global'].forEach(function(indexType) {
    var indexes = table[indexType + 'SecondaryIndexes'] || []
    var actions = getIndexActions(indexes, existingItem, item, table)
    puts = puts.concat(actions.puts.map(function(action) {
      var indexDb = store.getIndexDb(indexType, table.TableName, action.index)
      return indexDb.put.bind(indexDb, action.key, action.item)
    }))
    deletes = deletes.concat(actions.deletes.map(function(action) {
      var indexDb = store.getIndexDb(indexType, table.TableName, action.index)
      return indexDb.del.bind(indexDb, action.key)
    }))
  })

  async.parallel(deletes, function(err) {
    if (err) return cb(err)
    async.parallel(puts, cb)
  })
}

function getIndexActions(indexes, existingItem, item, table) {
  var puts = [], deletes = [], tableKeys = table.KeySchema.map(function(key) { return key.AttributeName })
  indexes.forEach(function(index) {
    var indexKeys = index.KeySchema.map(function(key) { return key.AttributeName }), key = null, itemPieces = item

    if (item && indexKeys.every(function(key) { return item[key] != null })) {
      if (index.Projection.ProjectionType != 'ALL') {
        var indexAttrs = indexKeys.concat(tableKeys, index.Projection.NonKeyAttributes || [])
        itemPieces = indexAttrs.reduce(function(obj, attr) {
          obj[attr] = item[attr]
          return obj
        }, Object.create(null))
      }

      key = createIndexKey(itemPieces, table, index.KeySchema)
      puts.push({index: index.IndexName, key: key, item: itemPieces})
    }

    if (existingItem && indexKeys.every(function(key) { return existingItem[key] != null })) {
      var existingKey = createIndexKey(existingItem, table, index.KeySchema)
      if (existingKey != key) {
        deletes.push({index: index.IndexName, key: existingKey})
      }
    }
  })
  return {puts: puts, deletes: deletes}
}
