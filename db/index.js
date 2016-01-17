var crypto = require('crypto'),
    lazy = require('lazy'),
    levelup = require('levelup'),
    memdown = require('memdown'),
    sublevel = require('level-sublevel'),
    Lock = require('lock'),
    Big = require('big.js')

exports.MAX_SIZE = 409600
exports.create = create
exports.lazy = lazyStream
exports.validateKey = validateKey
exports.validateItem = validateItem
exports.toLexiStr = toLexiStr
exports.hashPrefix = hashPrefix
exports.itemCompare = itemCompare
exports.validationError = validationError
exports.checkConditional = checkConditional
exports.itemSize = itemSize
exports.capacityUnits = capacityUnits
exports.matchesFilter = matchesFilter

function create(options) {
  options = options || {}
  if (options.createTableMs == null) options.createTableMs = 500
  if (options.deleteTableMs == null) options.deleteTableMs = 500
  if (options.updateTableMs == null) options.updateTableMs = 500

  var db = levelup(options.path || '/does/not/matter', options.path ? {} : {db: memdown}),
      sublevelDb = sublevel(db),
      tableDb = sublevelDb.sublevel('table', {valueEncoding: 'json'}),
      itemDbs = []

  tableDb.lock = new Lock()

  // XXX: Is there a better way to get this?
  tableDb.awsAccountId = (process.env.AWS_ACCOUNT_ID || '0000-0000-0000').replace(/[^\d]/g, '')
  tableDb.awsRegion = process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION || 'us-east-1'

  function getItemDb(name) {
    if (!itemDbs[name]) {
      itemDbs[name] = sublevelDb.sublevel('item-' + name, {valueEncoding: 'json'})
      itemDbs[name].lock = new Lock()
    }
    return itemDbs[name]
  }

  function deleteItemDb(name, cb) {
    var itemDb = getItemDb(name)
    delete itemDbs[name]
    lazyStream(itemDb.createKeyStream(), cb).join(function(keys) {
      itemDb.batch(keys.map(function(key) { return {type: 'del', key: key} }), cb)
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
    createTableMs: options.createTableMs,
    deleteTableMs: options.deleteTableMs,
    updateTableMs: options.updateTableMs,
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

function validateKey(dataKey, table) {
  if (table.KeySchema.length != Object.keys(dataKey).length) return validationError()

  var keyStr, i, j, attr, type, sizeError
  for (i = 0; i < table.KeySchema.length; i++) {
    attr = table.KeySchema[i].AttributeName
    if (dataKey[attr] == null) return validationError()
    for (j = 0; j < table.AttributeDefinitions.length; j++) {
      if (table.AttributeDefinitions[j].AttributeName != attr) continue
      type = table.AttributeDefinitions[j].AttributeType
      if (dataKey[attr][type] == null) return validationError()
      sizeError = checkKeySize(dataKey[attr][type], type, !i)
      if (sizeError) return sizeError
      if (!keyStr) keyStr = hashPrefix(dataKey[attr][type], type)
      keyStr += '~' + toLexiStr(dataKey[attr][type], type)
      break
    }
  }
  return keyStr
}

function validateItem(dataItem, table) {
  var keyStr, i, j, k, attr, type, sizeError
  for (i = 0; i < table.KeySchema.length; i++) {
    attr = table.KeySchema[i].AttributeName
    if (dataItem[attr] == null)
      return validationError('One or more parameter values were invalid: ' +
        'Missing the key ' + attr + ' in the item')
    for (j = 0; j < table.AttributeDefinitions.length; j++) {
      if (table.AttributeDefinitions[j].AttributeName != attr) continue
      type = table.AttributeDefinitions[j].AttributeType
      if (dataItem[attr][type] == null)
        return validationError('One or more parameter values were invalid: ' +
          'Type mismatch for key ' + attr + ' expected: ' + table.AttributeDefinitions[j].AttributeType +
          ' actual: ' + Object.keys(dataItem[attr])[0])
      sizeError = checkKeySize(dataItem[attr][type], type, !i)
      if (sizeError) return sizeError
      if (!keyStr) keyStr = hashPrefix(dataItem[attr][type], type)
      keyStr += '~' + toLexiStr(dataItem[attr][type], type)
      break
    }
  }
  if (table.GlobalSecondaryIndexes) {
    for (i = table.GlobalSecondaryIndexes.length - 1; i >= 0; i--) {
      for (k = 0; k < table.GlobalSecondaryIndexes[i].KeySchema.length; k++) {
        attr = table.GlobalSecondaryIndexes[i].KeySchema[k].AttributeName
        for (j = 0; j < table.AttributeDefinitions.length; j++) {
          if (table.AttributeDefinitions[j].AttributeName != attr) continue
          type = table.AttributeDefinitions[j].AttributeType
          if (dataItem[attr] && !dataItem[attr][type])
            return validationError('One or more parameter values were invalid: ' +
              'Type mismatch for Index Key ' + attr + ' Expected: ' + type + ' Actual: ' +
              Object.keys(dataItem[attr])[0] + ' IndexName: ' + table.GlobalSecondaryIndexes[i].IndexName)
        }
      }
    }
  }
  if (table.LocalSecondaryIndexes) {
    for (i = table.LocalSecondaryIndexes.length - 1; i >= 0; i--) {
      attr = table.LocalSecondaryIndexes[i].KeySchema[1].AttributeName
      for (j = 0; j < table.AttributeDefinitions.length; j++) {
        if (table.AttributeDefinitions[j].AttributeName != attr) continue
        type = table.AttributeDefinitions[j].AttributeType
        if (dataItem[attr] && !dataItem[attr][type])
          return validationError('One or more parameter values were invalid: ' +
            'Type mismatch for Index Key ' + attr + ' Expected: ' + type + ' Actual: ' +
            Object.keys(dataItem[attr])[0] + ' IndexName: ' + table.LocalSecondaryIndexes[i].IndexName)
      }
    }
  }
  return keyStr
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
  var expected = data.Expected
  if (!expected) return null

  existingItem = existingItem || {}

  if (!matchesFilter(existingItem, expected, data.ConditionalOperator)) {
    return conditionalError()
  }
}

function validationError(msg) {
  if (msg == null) msg = 'The provided key element does not match the schema'
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
  var size = 0, attr, type, val
  for (attr in item) {
    type = Object.keys(item[attr])[0]
    val = item[attr][type]
    size += skipAttr ? 2 : attr.length
    switch (type) {
      case 'S':
        size += val.length
        break
      case 'B':
        size += new Buffer(val, 'base64').length
        break
      case 'N':
        val = new Big(val)
        size += Math.ceil(val.c.length / 2) + (val.e % 2 ? 1 : 2)
        break
      case 'SS':
        size += val.reduce(function(sum, x) { return sum + x.length }, skipAttr ? val.length : 0) // eslint-disable-line no-loop-func
        break
      case 'BS':
        size += val.reduce(function(sum, x) { return sum + new Buffer(x, 'base64').length }, skipAttr ? val.length : 0) // eslint-disable-line no-loop-func
        break
      case 'NS':
        size += val.reduce(function(sum, x) { // eslint-disable-line no-loop-func
          x = new Big(x)
          return sum + Math.ceil(x.c.length / 2) + (x.e % 2 ? 1 : 2)
        }, skipAttr ? val.length : 0)
        break
    }
  }
  return size
}

function capacityUnits(item, isRead, isConsistent) {
  var size = item ? Math.ceil(itemSize(item) / 1024 / (isRead ? 4 : 1)) : 1
  return size / (!isRead || isConsistent ? 1 : 2)
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
  var results = Object.keys(filter).map(function(attr) {
    var comp = filter[attr].Exists != null ? (filter[attr].Exists ? 'NOT_NULL' : 'NULL') :
          filter[attr].ComparisonOperator || 'EQ',
        compVals = filter[attr].AttributeValueList || (filter[attr].Value ? [filter[attr].Value] : null),
        compType = compVals ? Object.keys(compVals[0])[0] : null,
        compVal = compVals ? compVals[0][compType] : null,
        attrType = val[attr] ? Object.keys(val[attr])[0] : null,
        attrVal = val[attr] ? val[attr][attrType] : null

    switch (comp) {
      case 'EQ':
        if (compType != attrType || !valsEqual(attrVal, compVal)) return false
        break
      case 'NE':
        if (compType == attrType && valsEqual(attrVal, compVal)) return false
        break
      case 'LE':
        if (compType != attrType ||
          (attrType == 'N' && !new Big(attrVal).lte(compVal)) ||
          (attrType != 'N' && toLexiStr(attrVal, attrType) > toLexiStr(compVal, attrType))) return false
        break
      case 'LT':
        if (compType != attrType ||
          (attrType == 'N' && !new Big(attrVal).lt(compVal)) ||
          (attrType != 'N' && toLexiStr(attrVal, attrType) >= toLexiStr(compVal, attrType))) return false
        break
      case 'GE':
        if (compType != attrType ||
          (attrType == 'N' && !new Big(attrVal).gte(compVal)) ||
          (attrType != 'N' && toLexiStr(attrVal, attrType) < toLexiStr(compVal, attrType))) return false
        break
      case 'GT':
        if (compType != attrType ||
          (attrType == 'N' && !new Big(attrVal).gt(compVal)) ||
          (attrType != 'N' && toLexiStr(attrVal, attrType) <= toLexiStr(compVal, attrType))) return false
        break
      case 'NOT_NULL':
        if (attrVal == null) return false
        break
      case 'NULL':
        if (attrVal != null) return false
        break
      case 'CONTAINS':
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
        if (compType != attrType) return false
        if (compType == 'B') {
          attrVal = new Buffer(attrVal, 'base64').toString()
          compVal = new Buffer(compVal, 'base64').toString()
        }
        if (attrVal.indexOf(compVal) !== 0) return false
        break
      case 'IN':
        if (!attrVal) return false
        if (!compVals.some(function(compVal) {
          compType = Object.keys(compVal)[0]
          compVal = compVal[compType]
          return compType == attrType && attrVal == compVal
        })) return false
        break
      case 'BETWEEN':
        if (!attrVal || compType != attrType ||
          (attrType == 'N' && (!new Big(attrVal).gte(compVal) || !new Big(attrVal).lte(compVals[1].N))) ||
          (attrType != 'N' && (toLexiStr(attrVal, attrType) < toLexiStr(compVal, attrType) ||
            toLexiStr(attrVal, attrType) > toLexiStr(compVals[1][compType], attrType)))) return false
    }
    return true
  })

  var passed = results.reduce(function(memo, result) {
    if (result) memo++
    return memo
  }, 0)

  if (conditionalOperator && conditionalOperator === 'OR') {
    if (passed === 0) return false
  } else if (passed < Object.keys(filter).length) return false
  return true
}
