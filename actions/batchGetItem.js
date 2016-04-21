var async = require('async'),
    getItem = require('./getItem'),
    db = require('../db')

module.exports = function batchGetItem(store, data, cb) {
  var requests = {}

  async.series([
    async.each.bind(async, Object.keys(data.RequestItems), addTableRequests),
    async.parallel.bind(async, requests),
  ], function(err, responses) {
    if (err) return cb(err)
    var res = {Responses: {}, UnprocessedKeys: {}}, table, tableResponses = responses[1], totalSize = 0, capacities = {}

    for (table in tableResponses) {
      // Order is pretty random
      // Assign keys before we shuffle
      tableResponses[table].forEach(function(tableRes, ix) { tableRes._key = data.RequestItems[table].Keys[ix] }) // eslint-disable-line no-loop-func
      shuffle(tableResponses[table])
      res.Responses[table] = tableResponses[table].map(function(tableRes) { // eslint-disable-line no-loop-func
        if (tableRes.Item) {
          // TODO: This is totally inefficient - should fix this
          var newSize = totalSize + db.itemSize(tableRes.Item)
          if (newSize > (1024 * 1024 + store.options.maxItemSize - 3)) {
            if (!res.UnprocessedKeys[table]) {
              res.UnprocessedKeys[table] = {Keys: []}
              if (data.RequestItems[table].AttributesToGet)
                res.UnprocessedKeys[table].AttributesToGet = data.RequestItems[table].AttributesToGet
              if (data.RequestItems[table].ConsistentRead)
                res.UnprocessedKeys[table].ConsistentRead = data.RequestItems[table].ConsistentRead
            }
            if (!capacities[table]) capacities[table] = 0
            capacities[table] += 1
            res.UnprocessedKeys[table].Keys.push(tableRes._key)
            return null
          }
          totalSize = newSize
        }
        if (tableRes.ConsumedCapacity) {
          if (!capacities[table]) capacities[table] = 0
          capacities[table] += tableRes.ConsumedCapacity.CapacityUnits
        }
        return tableRes.Item
      }).filter(Boolean)
    }

    if (~['TOTAL', 'INDEXES'].indexOf(data.ReturnConsumedCapacity)) {
      res.ConsumedCapacity = Object.keys(tableResponses).map(function(table) {
        return {
          CapacityUnits: capacities[table],
          TableName: table,
          Table: data.ReturnConsumedCapacity == 'INDEXES' ? {CapacityUnits: capacities[table]} : undefined,
        }
      })
    }

    cb(null, res)
  })

  function addTableRequests(tableName, cb) {
    store.getTable(tableName, function(err, table) {
      if (err) return cb(err)

      var req = data.RequestItems[tableName], i, key, options, gets = []

      for (i = 0; i < req.Keys.length; i++) {
        key = req.Keys[i]

        if ((err = db.validateKey(key, table)) != null) return cb(err)

        options = {TableName: tableName, Key: key}
        if (req._projection) options._projection = req._projection
        if (req.AttributesToGet) options.AttributesToGet = req.AttributesToGet
        if (req.ConsistentRead) options.ConsistentRead = req.ConsistentRead
        if (data.ReturnConsumedCapacity) options.ReturnConsumedCapacity = data.ReturnConsumedCapacity
        gets.push(options)
      }

      requests[tableName] = async.map.bind(async, gets, function(data, cb) { return getItem(store, data, cb) })

      cb()
    })
  }
}

function shuffle(arr) {
  var i, j, temp
  for (i = arr.length - 1; i >= 1; i--) {
    j = Math.floor(Math.random() * (i + 1))
    temp = arr[i]
    arr[i] = arr[j]
    arr[j] = temp
  }
}
