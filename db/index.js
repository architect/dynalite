var Readable = require('stream').Readable,
    lazy = require('lazy'),
    levelup = require('levelup'),
    MemDown = require('memdown'),
    sublevel = require('level-sublevel'),
    Lock = require('lock'),
    Big = require('big.js')

var db = sublevel(levelup('./mydb', {db: function(location) { return new MemDown(location) }})),
    tableDb = db.sublevel('table', {valueEncoding: 'json'}),
    itemDb = db.sublevel('item', {valueEncoding: 'json'})

exports.createTableMs = 500
exports.deleteTableMs = 500
exports.lazy = lazyStream
exports.tableDb = tableDb
exports.itemDb = itemDb
exports.getTable = getTable
exports.validateKey = validateKey
exports.validateItem = validateItem
exports.validationError = validationError
exports.checkConditional = checkConditional

tableDb.lock = new Lock()
itemDb.lock = new Lock()

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

  var keyStr = table.TableName, i, j, attr, type
  for (i = 0; i < table.KeySchema.length; i++) {
    attr = table.KeySchema[i].AttributeName
    if (dataKey[attr] == null) return validationError()
    for (j = 0; j < table.AttributeDefinitions.length; j++) {
      if (table.AttributeDefinitions[j].AttributeName != attr) continue
      type = table.AttributeDefinitions[j].AttributeType
      if (dataKey[attr][type] == null) return validationError()
      keyStr += '\xff' + toLexiStr(dataKey[attr][type], type)
      break
    }
  }
  return keyStr
}

function validateItem(dataItem, table) {
  var keyStr = table.TableName, i, j, attr, type
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
      keyStr += '\xff' + toLexiStr(dataItem[attr][type], type)
      break
    }
  }
  return keyStr
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

// TODO: Ensure that sets match
function valueEquals(val1, val2) {
  if (!val1 || !val2) return false
  var key1 = Object.keys(val1)[0], key2 = Object.keys(val2)[0]
  if (key1 != key2) return false
  return val1[key1] == val2[key2]
}
