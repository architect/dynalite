var db = require('../db')

module.exports = function getItem (store, data, cb) {

  store.getTable(data.TableName, function (err, table) {
    if (err) return cb(err)

    let invalidKey = db.validateKey(data.Key, table)
    if (invalidKey != null) return cb(invalidKey)

    let invalidKeyPath = db.validateKeyPaths((data._projection || {}).nestedPaths, table)
    if (invalidKeyPath != null) return cb(invalidKeyPath)

    var itemDb = store.getItemDb(data.TableName), key = db.createKey(data.Key, table)

    itemDb.get(key, function (err, item) {
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
