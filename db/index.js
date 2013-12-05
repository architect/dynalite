var crypto = require('crypto'),
    lazy = require('lazy'),
    levelup = require('levelup'),
    memdown = require('memdown'),
    sublevel = require('level-sublevel'),
    Lock = require('lock'),
    Big = require('big.js')

exports.create = create
exports.lazy = lazyStream
exports.validateKey = validateKey
exports.validateItem = validateItem
exports.toLexiStr = toLexiStr
exports.validationError = validationError
exports.checkConditional = checkConditional
exports.itemSize = itemSize
exports.capacityUnits = capacityUnits
exports.matchesFilter = matchesFilter

function create(options) {
  options = options || {}
  options.path = options.path || memdown
  if (options.createTableMs == null) options.createTableMs = 500
  if (options.deleteTableMs == null) options.deleteTableMs = 500
  if (options.updateTableMs == null) options.updateTableMs = 500

  var db = sublevel(levelup(options.path)),
      tableDb = db.sublevel('table', {valueEncoding: 'json'}),
      itemDbs = []

  tableDb.lock = new Lock()

  function getItemDb(name) {
    if (!itemDbs[name]) {
      itemDbs[name] = db.sublevel('item-' + name, {valueEncoding: 'json'})
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

  return {
    createTableMs: options.createTableMs,
    deleteTableMs: options.deleteTableMs,
    updateTableMs: options.updateTableMs,
    tableDb: tableDb,
    getItemDb: getItemDb,
    deleteItemDb: deleteItemDb,
    getTable: getTable,
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
  var keyStr, i, j, attr, type, sizeError
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
  if (table.LocalSecondaryIndexes) {
    for (i = 0; i < table.LocalSecondaryIndexes.length; i++) {
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
  if (type == 'N') return
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
  if (type != 'N') return keyPiece
  var bigNum = Big(keyPiece), digits,
      exp = !bigNum.c[0] ? 0 : bigNum.s == -1 ? 125 - bigNum.e : 130 + bigNum.e
  if (bigNum.s == -1) {
    bigNum.e = 0
    digits = Big(10).plus(bigNum).toFixed().replace(/\./, '')
  } else {
    digits = bigNum.c.join('')
  }
  return (bigNum.s == -1 ? '0' : '1') + ('0' + exp.toString(16)).slice(-2) + digits
}

function hashPrefix(hashKey, type) {
  if (type == 'S') {
    hashKey = new Buffer(hashKey, 'utf8')
  } else if (type == 'N') {
    hashKey = numToBuffer(hashKey)
  } else if (type == 'B') {
    hashKey = new Buffer(hashKey, 'base64')
  }
  // TODO: Can use the whole hash if we deem it important - for now just first six chars
  return crypto.createHash('md5').update('Outliers').update(hashKey).digest('hex').slice(0, 6)
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
      byteArray[byteArrayIndex] = byteArray[byteArrayIndex] + mantissa[mantissaIndex]
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

function checkConditional(expected, existingItem) {
  if (!expected) return

  existingItem = existingItem || {}

  for (var attr in expected) {
    if (expected[attr].Exists === false && existingItem[attr] != null)
      return conditionalError()
    if (expected[attr].Value && !valueEquals(expected[attr].Value, existingItem[attr]))
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
        val = Big(val)
        size += Math.ceil(val.c.length / 2) + (val.e % 2 ? 1 : 2)
        break
      case 'SS':
        size += val.reduce(function(sum, x) { return sum + x.length }, skipAttr ? val.length : 0)
        break
      case 'BS':
        size += val.reduce(function(sum, x) { return sum + new Buffer(x, 'base64').length }, skipAttr ? val.length : 0)
        break
      case 'NS':
        size += val.reduce(function(sum, x) {
          x = Big(x)
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

// TODO: Ensure that sets match
function valueEquals(val1, val2) {
  if (!val1 || !val2) return false
  var key1 = Object.keys(val1)[0], key2 = Object.keys(val2)[0]
  if (key1 != key2) return false
  return val1[key1] == val2[key2]
}

function matchesFilter(val, filter) {
  for (var attr in filter) {
    var comp = filter[attr].ComparisonOperator,
        compVals = filter[attr].AttributeValueList,
        compType = compVals ? Object.keys(compVals[0])[0] : null,
        compVal = compVals ? compVals[0][compType] : null,
        attrType = val[attr] ? Object.keys(val[attr])[0] : null,
        attrVal = val[attr] ? val[attr][attrType] : null
    switch (comp) {
      case 'EQ':
        if (compType != attrType || attrVal != compVal) return false
        break
      case 'NE':
        if (compType == attrType && attrVal == compVal) return false
        break
      case 'LE':
        if (compType != attrType ||
          (attrType == 'N' && !Big(attrVal).lte(compVal)) ||
          (attrType != 'N' && attrVal > compVal)) return false
        break
      case 'LT':
        if (compType != attrType ||
          (attrType == 'N' && !Big(attrVal).lt(compVal)) ||
          (attrType != 'N' && attrVal >= compVal)) return false
        break
      case 'GE':
        if (compType != attrType ||
          (attrType == 'N' && !Big(attrVal).gte(compVal)) ||
          (attrType != 'N' && attrVal < compVal)) return false
        break
      case 'GT':
        if (compType != attrType ||
          (attrType == 'N' && !Big(attrVal).gt(compVal)) ||
          (attrType != 'N' && attrVal <= compVal)) return false
        break
      case 'NOT_NULL':
        if (!attrVal) return false
        break
      case 'NULL':
        if (attrVal) return false
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
          (attrType == 'N' && (!Big(attrVal).gte(compVal) || !Big(attrVal).lte(compVals[1].N))) ||
          (attrType != 'N' && (attrVal < compVal || attrVal > compVals[1][compType]))) return false
    }
  }
  return true
}
