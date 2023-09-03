var db = require('../db')

module.exports = function deleteItem (store, data, cb) {

  store.getTable(data.TableName, function (err, table) {
    if (err) return cb(err)

    let invalid = db.validateKey(data.Key, table)
    if (invalid != null) return cb(invalid)

    var itemDb = store.getItemDb(data.TableName), key = db.createKey(data.Key, table)

    itemDb.lock(key, function (release) {
      cb = release(cb)

      itemDb.get(key, function (err, existingItem) {
        if (err && err.name != 'NotFoundError') return cb(err)

        let invalid =  db.checkConditional(data, existingItem)
        if (invalid != null) return cb(invalid)

        var returnObj = {}

        if (existingItem && data.ReturnValues == 'ALL_OLD')
          returnObj.Attributes = existingItem

        returnObj.ConsumedCapacity = db.addConsumedCapacity(data, false, existingItem)

        db.updateIndexes(store, table, existingItem, null, function (err) {
          if (err) return cb(err)

          itemDb.del(key, function (err) {
            if (err) return cb(err)
            cb(null, returnObj)
          })
        })
      })
    })
  })
}
