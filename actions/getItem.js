var db = require('../db')

module.exports = function getItem(store, data, cb) {

  store.getTable(data.TableName, function(err, table) {
    if (err) return cb(err)

    if ((err = db.validateKey(data.Key, table)) != null) return cb(err)

    if ((err = db.validateKeyPaths((data._projection || {}).nestedPaths, table)) != null) return cb(err)

    var itemDb = store.getItemDb(data.TableName), key = db.createKey(data.Key, table)

    itemDb.get(key, function(err, item) {
      if (err && err.name != 'NotFoundError') return cb(err)

      var returnObj = {}, paths = data._projection ? data._projection.paths : data.AttributesToGet

      if (item) {
        returnObj.Item = paths ? db.mapPaths(paths, item) : item
      }

      returnObj.ConsumedCapacity = db.addConsumedCapacity(data, true, item)

      cb(null, returnObj)
    })
  })
}
