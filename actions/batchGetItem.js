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
      tableResponses[table].forEach(function(tableRes, ix) { tableRes._key = data.RequestItems[table].Keys[ix] })
      shuffle(tableResponses[table])
      res.Responses[table] = tableResponses[table].map(function(tableRes) {
        if (tableRes.Item) {
          // TODO: This is totally inefficient - should fix this
          var newSize = totalSize + db.itemSize(tableRes.Item)
          if (newSize > 1048574) {
            if (!res.UnprocessedKeys[table]) {
              res.UnprocessedKeys[table] = {Keys: []}
              if (data.RequestItems[table].AttributesToGet)
                res.UnprocessedKeys[table].AttributesToGet = data.RequestItems[table].AttributesToGet
              if (data.RequestItems[table].ConsistentRead)
                res.UnprocessedKeys[table].ConsistentRead = data.RequestItems[table].ConsistentRead
            }
            res.UnprocessedKeys[table].Keys.push(tableRes._key)
            return
          }
          totalSize = newSize
        }
        if (tableRes.ConsumedCapacity) {
          if (!capacities[table]) capacities[table] = 0
          capacities[table] += tableRes.ConsumedCapacity.CapacityUnits
        }
        return tableRes.Item
      }).filter(function(x) { return x })
    }

    if (data.ReturnConsumedCapacity == 'TOTAL') {
      res.ConsumedCapacity = Object.keys(tableResponses).map(function(table) {
        return {CapacityUnits: capacities[table], TableName: table}
      })
    }

    cb(null, res)
  })

  function addTableRequests(tableName, cb) {
    store.getTable(tableName, function(err, table) {
      if (err) return cb(err)

      var req = data.RequestItems[tableName], i, key, options, gets = [], seenKeys = {}

      for (i = 0; i < req.Keys.length; i++) {
        key = req.Keys[i]

        options = {TableName: tableName, Key: key}
        if (req.AttributesToGet) options.AttributesToGet = req.AttributesToGet
        if (req.ConsistentRead) options.ConsistentRead = req.ConsistentRead
        if (data.ReturnConsumedCapacity) options.ReturnConsumedCapacity = data.ReturnConsumedCapacity
        gets.push(options)

        key = db.validateKey(key, table)
        if (key instanceof Error) return cb(key)
        if (seenKeys[key])
          return cb(db.validationError('Provided list of item keys contains duplicates'))
        seenKeys[key] = true
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
