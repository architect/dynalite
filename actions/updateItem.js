var db = require('../db'),
    itemDb = db.itemDb

module.exports = function putItem(data, cb) {

  db.getTable(data.TableName, function(err, table) {
    if (err) return cb(err)

    var key = db.validateKey(data.Key, table)
    if (key instanceof Error) return cb(key)

    var putItem = {}
    for (var attr in data.Key) {
      putItem[attr] = data.Key[attr]
    }

    if (data.AttributeUpdates) {
      for (var i = 0; i < table.KeySchema.length; i++) {
        var keyName = table.KeySchema[i].AttributeName
        if (data.AttributeUpdates[keyName])
          return cb(db.validationError('One or more parameter values were invalid: ' +
            'Cannot update attribute ' + keyName + '. This attribute is part of the key'))
      }
      for (var attr in data.AttributeUpdates) {
        if (data.AttributeUpdates[attr].Value &&
            (data.AttributeUpdates[attr].Action == 'PUT' || data.AttributeUpdates[attr].Action == null)) {
          putItem[attr] = data.AttributeUpdates[attr].Value
        }
      }
    }

    var fetchExisting = (data.ReturnValues == 'ALL_OLD' || data.ReturnValues == 'UPDATED_OLD' || data.Expected) ?
      itemDb.get.bind(itemDb, key) : function(cb) { cb() }

    fetchExisting(function(err, existingItem) {
      if (err && err.name != 'NotFoundError') return cb(err)

      if ((err = db.checkConditional(data.Expected, existingItem)) != null) return cb(err)

      var returnObj = {}

      if (existingItem) {
        if (data.ReturnValues == 'ALL_OLD') {
          returnObj.Attributes = existingItem
        } else if (data.ReturnValues == 'UPDATED_OLD') {
          returnObj.Attributes = {}
          for (var attr in data.AttributeUpdates) {
            if (data.AttributeUpdates[attr].Value &&
                (data.AttributeUpdates[attr].Action == 'PUT' || data.AttributeUpdates[attr].Action == null)) {
              returnObj.Attributes[attr] = existingItem[attr]
            }
          }
        }
      }

      if (data.ReturnValues == 'ALL_NEW') {
        returnObj.Attributes = putItem
      } else if (data.ReturnValues == 'UPDATED_NEW') {
        returnObj.Attributes = {}
        for (var attr in data.AttributeUpdates) {
          if (data.AttributeUpdates[attr].Value &&
              (data.AttributeUpdates[attr].Action == 'PUT' || data.AttributeUpdates[attr].Action == null)) {
            returnObj.Attributes[attr] = putItem[attr]
          }
        }
      }

      itemDb.put(key, putItem, function(err) {
        if (err) return cb(err)
        cb(null, returnObj)
      })
    })
  })
}

