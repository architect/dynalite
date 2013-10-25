var db = require('../db'),
    itemDb = db.itemDb

module.exports = function updateItem(data, cb) {

  db.getTable(data.TableName, function(err, table) {
    if (err) return cb(err)

    var key = db.validateKey(data.Key, table)
    if (key instanceof Error) return cb(key)

    if (data.AttributeUpdates) {
      for (var i = 0; i < table.KeySchema.length; i++) {
        var keyName = table.KeySchema[i].AttributeName
        if (data.AttributeUpdates[keyName])
          return cb(db.validationError('One or more parameter values were invalid: ' +
            'Cannot update attribute ' + keyName + '. This attribute is part of the key'))
      }
    }

    itemDb.lock(key, function(release) {
      cb = release(cb)

      itemDb.get(key, function(err, oldItem) {
        if (err && err.name != 'NotFoundError') return cb(err)

        if ((err = db.checkConditional(data.Expected, oldItem)) != null) return cb(err)

        var returnObj = {}, item = data.Key

        if (oldItem) {
          for (var attr in oldItem) {
            item[attr] = oldItem[attr]
          }
          if (data.ReturnValues == 'ALL_OLD') {
            returnObj.Attributes = oldItem
          } else if (data.ReturnValues == 'UPDATED_OLD') {
            returnObj.Attributes = {}
            for (var attr in data.AttributeUpdates) {
              if (oldItem[attr] != null) {
                returnObj.Attributes[attr] = oldItem[attr]
              }
            }
          }
        }

        for (var attr in data.AttributeUpdates) {
          if (data.AttributeUpdates[attr].Action == 'PUT' || data.AttributeUpdates[attr].Action == null) {
            item[attr] = data.AttributeUpdates[attr].Value
          } else if (data.AttributeUpdates[attr].Action == 'ADD') {
            if (data.AttributeUpdates[attr].Value.N) {
              if (item[attr] && !item[attr].N)
                return cb(db.validationError('Type mismatch for attribute to update'))
              if (!item[attr]) item[attr] = {N: '0'}
              item[attr].N = '' + (+item[attr].N + +data.AttributeUpdates[attr].Value.N)
            } else {
              var type = Object.keys(data.AttributeUpdates[attr].Value)[0]
              if (item[attr] && !item[attr][type])
                return cb(db.validationError('Type mismatch for attribute to update'))
              if (!item[attr]) item[attr] = {}
              item[attr][type] = (item[attr][type] || []).concat(data.AttributeUpdates[attr].Value[type])
            }
          } else if (data.AttributeUpdates[attr].Action == 'DELETE') {
            if (data.AttributeUpdates[attr].Value) {
              var type = Object.keys(data.AttributeUpdates[attr].Value)[0]
              if (item[attr] && !item[attr][type])
                return cb(db.validationError('Type mismatch for attribute to update'))
              if (item[attr] && item[attr][type]) {
                item[attr][type] = item[attr][type].filter(function(val) {
                  return !~data.AttributeUpdates[attr].Value[type].indexOf(val)
                })
              }
            } else {
              delete item[attr]
            }
          }
        }

        if (data.ReturnValues == 'ALL_NEW') {
          returnObj.Attributes = item
        } else if (data.ReturnValues == 'UPDATED_NEW') {
          returnObj.Attributes = {}
          for (var attr in data.AttributeUpdates) {
            if (item[attr]) returnObj.Attributes[attr] = item[attr]
          }
        }

        itemDb.put(key, item, function(err) {
          if (err) return cb(err)
          cb(null, returnObj)
        })
      })
    })
  })
}

