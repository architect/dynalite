var async = require('async'),
    putItem = require('./putItem'),
    deleteItem = require('./deleteItem'),
    db = require('../db')

module.exports = function batchWriteItem(data, cb) {
  var actions = []

  async.series([
    async.each.bind(async, Object.keys(data.RequestItems), addTableActions),
    async.parallel.bind(async, actions),
  ], function(err) {
    if (err) return cb(err)
    cb(null, {UnprocessedItems: {}})
  })

  function addTableActions(tableName, cb) {
    db.getTable(tableName, function(err, table) {
      if (err) return cb(err)

      var reqs = data.RequestItems[tableName], i, req, key, seenKeys = {}

      for (i = 0; i < reqs.length; i++) {
        req = reqs[i]

        if (req.PutRequest) {

          actions.push(putItem.bind(null, {TableName: tableName, Item: req.PutRequest.Item}))

          key = db.validateItem(req.PutRequest.Item, table)

        } else if (req.DeleteRequest) {

          actions.push(deleteItem.bind(null, {TableName: tableName, Key: req.DeleteRequest.Key}))

          key = db.validateKey(req.DeleteRequest.Key, table)
        }
        if (key instanceof Error) return cb(key)
        if (seenKeys[key])
          return cb(db.validationError('Provided list of item keys contains duplicates'))
        seenKeys[key] = true
      }

      cb()
    })
  }
}

