var Readable = require('stream').Readable,
    lazy = require('lazy'),
    levelup = require('levelup'),
    MemDown = require('memdown'),
    sublevel = require('level-sublevel'),
    deleteStream = require('level-delete-stream'),
    Lock = require('lock'),
    Big = require('big.js'),
    murmur = require('murmurhash-js')

var db = sublevel(levelup('./mydb', {db: function(location) { return new MemDown(location) }})),
    tableDb = db.sublevel('table', {valueEncoding: 'json'}),
    itemDbs = []

exports.createTableMs = 500
exports.deleteTableMs = 500
exports.lazy = lazyStream
exports.tableDb = tableDb
exports.getItemDb = getItemDb
exports.deleteItemDb = deleteItemDb
exports.getTable = getTable
exports.validateKey = validateKey
exports.validateItem = validateItem
exports.toLexiStr = toLexiStr
exports.validationError = validationError
exports.checkConditional = checkConditional
exports.itemSize = itemSize
exports.capacityUnits = capacityUnits

tableDb.lock = new Lock()

function getItemDb(name) {
  if (!itemDbs[name]) {
    itemDbs[name] = db.sublevel('item-' + name, {valueEncoding: 'json'})
    itemDbs[name].lock = new Lock()
  }
  return itemDbs[name]
}

function deleteItemDb(name, cb) {
  var itemDb = itemDbs[name] || db.sublevel('item-' + name, {valueEncoding: 'json'})
  delete itemDbs[name]
  itemDb.createKeyStream().pipe(deleteStream(db, cb))
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

function lazyStream(stream, errHandler) {
  if (errHandler) stream.on('error', errHandler)
  return lazy(stream)
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
      if (!keyStr) keyStr = hashPrefix(dataKey[attr][type])
      keyStr += '\xff' + toLexiStr(dataKey[attr][type], type)
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
      if (!keyStr) keyStr = hashPrefix(dataItem[attr][type])
      keyStr += '\xff' + toLexiStr(dataItem[attr][type], type)
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

// TODO: Not sure what sort of hashing algorithm is used
function hashPrefix(hashKey) {
  return ('00' + (murmur(hashKey) % 4096).toString(16)).slice(-3)
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

function capacityUnits(item, isDouble) {
  var size = item ? Math.ceil(itemSize(item) / 1024) : 1
  return size * (isDouble ? 1 : 0.5)
}

// TODO: Ensure that sets match
function valueEquals(val1, val2) {
  if (!val1 || !val2) return false
  var key1 = Object.keys(val1)[0], key2 = Object.keys(val2)[0]
  if (key1 != key2) return false
  return val1[key1] == val2[key2]
}
