var db = require('../db')

module.exports = function getItem(data, cb) {

  db.getTable(data.TableName, function(err, table) {
    if (err) return cb(err)

    var key = db.validateKey(data.Key, table), itemDb = db.getItemDb(data.TableName)
    if (key instanceof Error) return cb(key)

    itemDb.get(key, function(err, item) {
      if (err && err.name != 'NotFoundError') return cb(err)

      var returnObj = {}

      if (item) {
        if (data.AttributesToGet) {
          returnObj.Item = data.AttributesToGet.reduce(function(returnItem, attr) {
            if (item[attr] != null) returnItem[attr] = item[attr]
            return returnItem
          }, {})
        } else {
          returnObj.Item = item
        }
      }

      if (data.ReturnConsumedCapacity == 'TOTAL')
        returnObj.ConsumedCapacity = {CapacityUnits: db.capacityUnits(item, true, data.ConsistentRead), TableName: data.TableName}

      cb(null, returnObj)
    })
  })
}

