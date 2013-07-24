var db = require('../db'),
    itemDb = db.itemDb

module.exports = function getItem(data, cb) {

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
    var validationError = new Error('The provided key element does not match the schema')
    validationError.statusCode = 400
    validationError.body = {
      __type: 'com.amazon.coral.validate#ValidationException',
      message: 'The provided key element does not match the schema',
    }

    if (table.KeySchema.length != Object.keys(data.Key).length) return cb(validationError)

    var key = ''
    for (var i = 0; i < table.KeySchema.length; i++) {
      var keyName = table.KeySchema[i].AttributeName
      if (data.Key[keyName] == null) return cb(validationError)
      for (var j = 0; j < table.AttributeDefinitions.length; j++) {
        if (table.AttributeDefinitions[j].AttributeName != keyName) continue
        if (data.Key[keyName][table.AttributeDefinitions[j].AttributeType] == null) return cb(validationError)
        key += data.Key[keyName][table.AttributeDefinitions[j].AttributeType]
        break
      }
    }

    itemDb.get(key, function(err, item) {
      if (err && err.name != 'NotFoundError') return cb(err)

      var returnObj = {}

      if (item) {
        if (data.AttributesToGet) {
          returnObj.Item = {}
          data.AttributesToGet.forEach(function(attr) {
            returnObj.Item[attr] = item[attr]
          })
        } else {
          returnObj.Item = item
        }
      }

      if (data.ReturnConsumedCapacity == 'TOTAL') {
        var units = data.ConsistentRead ? 1 : 0.5
        returnObj.ConsumedCapacity = {CapacityUnits: units, TableName: data.TableName}
      }

      cb(null, returnObj)
    })
  })
}

