var db = require('../db'),
    itemDb = db.itemDb

module.exports = function putItem(data, cb) {

  db.getTable(data.TableName, function(err, table) {
    // TODO: Is there a cleaner way to handle this?
    if (err && err.name == 'NotFoundError') err.body.message = 'Requested resource not found'
    if (err) return cb(err)

    // TODO: Factor this out too
    if (table.TableStatus == 'CREATING' || table.TableStatus == 'DELETING') {
      err = new Error('Requested resource not found')
      err.statusCode = 400
      err.body = {
        __type: 'com.amazonaws.dynamodb.v20120810#ResourceNotFoundException',
        message: 'Requested resource not found',
      }
      return cb(err)
    }

    // TODO: refactor this out
    function validationError(msg) {
      var err = new Error(msg)
      err.statusCode = 400
      err.body = {
        __type: 'com.amazon.coral.validate#ValidationException',
        message: msg,
      }
      return err
    }

    // TODO: refactor this out
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

    var key = ''
    for (var i = 0; i < table.KeySchema.length; i++) {
      var keyName = table.KeySchema[i].AttributeName
      if (data.Item[keyName] == null)
        return cb(validationError('One or more parameter values were invalid: ' +
          'Missing the key ' + keyName + ' in the item'))
      for (var j = 0; j < table.AttributeDefinitions.length; j++) {
        if (table.AttributeDefinitions[j].AttributeName != keyName) continue
        if (data.Item[keyName][table.AttributeDefinitions[j].AttributeType] == null)
          return cb(validationError('One or more parameter values were invalid: ' +
            'Type mismatch for key ' + keyName + ' expected: ' + table.AttributeDefinitions[j].AttributeType +
            ' actual: ' + Object.keys(data.Item[keyName])[0]))
        key += data.Item[keyName][table.AttributeDefinitions[j].AttributeType]
        break
      }
    }

    var fetchExisting = function(cb) { cb() }
    if (data.ReturnValues == 'ALL_OLD' || data.Expected)
      fetchExisting = itemDb.get.bind(itemDb, key)

    fetchExisting(function(err, existingItem) {
      if (err && err.name != 'NotFoundError') return cb(err)

      existingItem = existingItem || {}

      if (data.Expected) {

        // First go through the key and check existence
        for (var attr in data.Expected) {
          if (data.Expected[attr].Exists === false && existingItem[attr] != null)
            return cb(conditionalError())
          if (data.Expected[attr].Value && !valueEquals(data.Expected[attr].Value, existingItem[attr]))
            return cb(conditionalError())
        }

      }

      var returnObj = {}
      if (data.ReturnValues == 'ALL_OLD') returnObj.Attributes = existingItem
      itemDb.put(key, data.Item, function(err) {
        if (err) return cb(err)
        cb(null, returnObj)
      })
    })
  })
}

// TODO: Refactor and ensure that sets match too
function valueEquals(val1, val2) {
  if (!val1 || !val2) return false
  var key1 = Object.keys(val1)[0], key2 = Object.keys(val2)[0]
  if (key1 != key2) return false
  return val1[key1] == val2[key2]
}
