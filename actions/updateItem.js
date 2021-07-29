var db = require('../db')

module.exports = function updateItem(store, data, cb) {

  store.getTable(data.TableName, function(err, table) {
    if (err) return cb(err)

    if ((err = db.validateKey(data.Key, table)) != null) return cb(err)

    if ((err = db.validateUpdates(data.AttributeUpdates, data._updates, table)) != null) return cb(err)

    var itemDb = store.getItemDb(data.TableName), key = db.createKey(data.Key, table)

    itemDb.lock(key, function(release) {
      cb = release(cb)

      itemDb.get(key, function(err, oldItem) {
        if (err && err.name != 'NotFoundError') return cb(err)

        if ((err = db.checkConditional(data, oldItem)) != null) return cb(err)

        var returnObj = {}, item = data.Key,
          paths = data._updates ? data._updates.paths : Object.keys(data.AttributeUpdates || {})

        if (oldItem) {
          item = db.deepClone(oldItem)
          if (data.ReturnValues == 'ALL_OLD') {
            returnObj.Attributes = oldItem
          } else if (data.ReturnValues == 'UPDATED_OLD') {
            returnObj.Attributes = db.mapPaths(paths, oldItem)
          }
        }

        err = data._updates ? db.applyUpdateExpression(data._updates.sections, table, item) :
            db.applyAttributeUpdates(data.AttributeUpdates, table, item)
        if (err) return cb(err)

        if (db.itemSize(item) > store.options.maxItemSize)
          return cb(db.validationError('Item size to update has exceeded the maximum allowed size'))

        if (data.ReturnValues == 'ALL_NEW') {
          returnObj.Attributes = item
        } else if (data.ReturnValues == 'UPDATED_NEW') {
          returnObj.Attributes = db.mapPaths(paths, item)
        }

        returnObj.ConsumedCapacity = db.addConsumedCapacity(data, false, oldItem, item)

        db.updateIndexes(store, table, oldItem, item, function(err) {
          if (err) return cb(err)

          itemDb.put(key, item, function(err) {
            if (err) return cb(err)
            cb(null, returnObj)
          })
        })
      })
    })
  })
}
