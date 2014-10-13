var Big = require('big.js'),
    db = require('../db')

module.exports = function updateItem(store, data, cb) {

  store.getTable(data.TableName, function(err, table) {
    if (err) return cb(err)

    var key = db.validateKey(data.Key, table), itemDb = store.getItemDb(data.TableName)
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

        if ((err = db.checkConditional(data, oldItem)) != null) return cb(err)

        var attr, type, returnObj = {}, item = data.Key

        if (oldItem) {
          for (attr in oldItem) {
            item[attr] = oldItem[attr]
          }
          if (data.ReturnValues == 'ALL_OLD') {
            returnObj.Attributes = oldItem
          } else if (data.ReturnValues == 'UPDATED_OLD') {
            returnObj.Attributes = {}
            for (attr in data.AttributeUpdates) {
              if (oldItem[attr] != null) {
                returnObj.Attributes[attr] = oldItem[attr]
              }
            }
          }
        }

        for (attr in data.AttributeUpdates) {
          if (data.AttributeUpdates[attr].Action == 'PUT' || data.AttributeUpdates[attr].Action == null) {
            item[attr] = data.AttributeUpdates[attr].Value
          } else if (data.AttributeUpdates[attr].Action == 'ADD') {
            if (data.AttributeUpdates[attr].Value.N) {
              if (item[attr] && !item[attr].N)
                return cb(db.validationError('Type mismatch for attribute to update'))
              if (!item[attr]) item[attr] = {N: '0'}
              item[attr].N = Big(item[attr].N).plus(data.AttributeUpdates[attr].Value.N).toFixed()
            } else {
              type = Object.keys(data.AttributeUpdates[attr].Value)[0]
              if (item[attr] && !item[attr][type])
                return cb(db.validationError('Type mismatch for attribute to update'))
              if (!item[attr]) item[attr] = {}
              if (!item[attr][type]) item[attr][type] = []
              /* jshint -W083 */
              item[attr][type] = item[attr][type].concat(data.AttributeUpdates[attr].Value[type].filter(function(a) {
                return !~item[attr][type].indexOf(a)
              }))
            }
          } else if (data.AttributeUpdates[attr].Action == 'DELETE') {
            if (data.AttributeUpdates[attr].Value) {
              type = Object.keys(data.AttributeUpdates[attr].Value)[0]
              if (item[attr] && !item[attr][type])
                return cb(db.validationError('Type mismatch for attribute to update'))
              if (item[attr] && item[attr][type]) {
                item[attr][type] = item[attr][type].filter(function(val) {
                  return !~data.AttributeUpdates[attr].Value[type].indexOf(val)
                })
                if (!item[attr][type].length) delete item[attr]
              }
            } else {
              delete item[attr]
            }
          }
        }

        if (db.itemSize(item) > db.MAX_SIZE)
          return cb(db.validationError('Item size to update has exceeded the maximum allowed size'))

        if (data.ReturnValues == 'ALL_NEW') {
          returnObj.Attributes = item
        } else if (data.ReturnValues == 'UPDATED_NEW') {
          returnObj.Attributes = {}
          /* jshint -W088 */
          for (attr in data.AttributeUpdates) {
            if (item[attr]) returnObj.Attributes[attr] = item[attr]
          }
        }

        if (data.ReturnConsumedCapacity == 'TOTAL')
          returnObj.ConsumedCapacity = {
            CapacityUnits: Math.max(db.capacityUnits(oldItem), db.capacityUnits(item)),
            TableName: data.TableName,
          }

        itemDb.put(key, item, function(err) {
          if (err) return cb(err)
          cb(null, returnObj)
        })
      })
    })
  })
}

