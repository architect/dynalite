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
    cb(null, {Responses: responses[1], UnprocessedKeys: {}})
  })

  function addTableRequests(tableName, cb) {
    db.getTable(tableName, function(err, table) {
      if (err) return cb(err)

      var req = data.RequestItems[tableName], i, key, options, gets = [], seenKeys = {}

      for (i = 0; i < req.Keys.length; i++) {
        key = req.Keys[i]

        options = {TableName: tableName, Key: key}
        if (req.AttributesToGet) options.AttributesToGet = req.AttributesToGet
        gets.push(options)

        key = db.validateKey(key, table)
        if (key instanceof Error) return cb(key)
        if (seenKeys[key])
          return cb(db.validationError('Provided list of item keys contains duplicates'))
        seenKeys[key] = true
      }

      requests[tableName] = function(cb) {
        async.map(gets, getItem, function(err, resps) {
          if (err) return cb(err)
          cb(null, resps.map(function(res) { return res.Item }).filter(function(x) { return x }))
        })
      }

      cb()
    })
  }
}

