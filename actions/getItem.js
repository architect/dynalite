var db = require('../db')

module.exports = function getItem(store, data, cb) {

  store.getTable(data.TableName, function(err, table) {
    if (err) return cb(err)

    var key = db.validateKey(data.Key, table), itemDb = store.getItemDb(data.TableName)
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

      if (~['TOTAL', 'INDEXES'].indexOf(data.ReturnConsumedCapacity))
        returnObj.ConsumedCapacity =  {
          CapacityUnits: db.capacityUnits(item, true, data.ConsistentRead),
          TableName: data.TableName,
          Table: data.ReturnConsumedCapacity == 'INDEXES' ?
            {CapacityUnits: db.capacityUnits(item, true, data.ConsistentRead)} : undefined
        }

      cb(null, returnObj)
    })
  })
}

