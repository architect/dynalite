var db = require('../db')

module.exports = function getItem(store, data, cb) {

  store.getTable(data.TableName, function(err, table) {
    if (err) return cb(err)

    var key = db.validateKey(data.Key, table), itemDb = store.getItemDb(data.TableName)
    if (key instanceof Error) return cb(key)

    if (data._projection) {
      err = db.validateKeyPaths(data._projection.nestedPaths, table)
      if (err) return cb(err)
    }

    itemDb.get(key, function(err, item) {
      if (err && err.name != 'NotFoundError') return cb(err)

      var returnObj = {}

      if (item) {
        if (data._projection) {
          returnObj.Item = db.mapPaths(data._projection.paths, item)
        } else if (data.AttributesToGet) {
          returnObj.Item = data.AttributesToGet.reduce(function(returnItem, attr) {
            if (item[attr] != null) returnItem[attr] = item[attr]
            return returnItem
          }, {})
        } else {
          returnObj.Item = item
        }
      }

      returnObj.ConsumedCapacity = db.addConsumedCapacity(data, true, item)

      cb(null, returnObj)
    })
  })
}
