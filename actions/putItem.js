var db = require('../db')

module.exports = function putItem(data, cb) {

  db.getTable(data.TableName, function(err, table) {
    if (err) return cb(err)

    var key = db.validateItem(data.Item, table), itemDb = db.getItemDb(data.TableName)
    if (key instanceof Error) return cb(key)

    var fetchExisting = (data.ReturnValues == 'ALL_OLD' || data.Expected) ?
      itemDb.get.bind(itemDb, key) : function(cb) { cb() }

    itemDb.lock(key, function(release) {
      cb = release(cb)

      fetchExisting(function(err, existingItem) {
        if (err && err.name != 'NotFoundError') return cb(err)

        if ((err = db.checkConditional(data.Expected, existingItem)) != null) return cb(err)

        var returnObj = {}

        if (existingItem && data.ReturnValues == 'ALL_OLD')
          returnObj.Attributes = existingItem

        if (data.ReturnConsumedCapacity == 'TOTAL')
          returnObj.ConsumedCapacity = {CapacityUnits: db.capacityUnits(data.Item, true), TableName: data.TableName}

        itemDb.put(key, data.Item, function(err) {
          if (err) return cb(err)
          cb(null, returnObj)
        })
      })
    })
  })
}

