var async = require('async'),
    getItem = require('./getItem'),
    db = require('../db')

module.exports = function batchGetItem(data, cb) {
  var requests = {}

  async.series([
    async.each.bind(async, Object.keys(data.RequestItems), addTableRequests),
    async.parallel.bind(async, requests),
  ], function(err, responses) {
    if (err) return cb(err)
    var res = {Responses: {}, UnprocessedKeys: {}}, table, tables = responses[1]

    for (table in tables)
      res.Responses[table] = tables[table].map(function(res) { return res.Item }).filter(function(x) { return x })

    if (data.ReturnConsumedCapacity == 'TOTAL')
      res.ConsumedCapacity = Object.keys(tables).map(function(table) {
        return {CapacityUnits: tables[table].reduce(function(total, res) {
          return total + res.ConsumedCapacity.CapacityUnits
        }, 0), TableName: table}
      })

    cb(null, res)
  })

  function addTableRequests(tableName, cb) {
    db.getTable(tableName, function(err, table) {
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

      requests[tableName] = async.map.bind(async, gets, getItem)

      cb()
    })
  }
}

