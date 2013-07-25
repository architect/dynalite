var db = require('../db'),
    itemDb = db.itemDb

module.exports = function getItem(data, cb) {

  db.getTable(data.TableName, function(err, table) {
    if (err) return cb(err)

    var key = db.validateKey(data.Key, table)
    if (key instanceof Error) return cb(key)

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

