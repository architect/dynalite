var async = require('async'),
    putItem = require('./putItem'),
    deleteItem = require('./deleteItem'),
    db = require('../db')

module.exports = function batchWriteItem(data, cb) {
  var puts = [], deletes = []

  function validateTableKeys(tableName, cb) {
    db.getTable(tableName, function(err, table) {
      if (err) return cb(err)

      var reqs = data.RequestItems[tableName], i, req, key, seenKeys = {}
      for (i = 0; i < reqs.length; i++) {
        req = reqs[i]
        if (req.PutRequest) {
          key = db.validateItem(req.PutRequest.Item, table)

          if (key instanceof Error) return cb(key)
          if (seenKeys[key])
            return cb(db.validationError('Provided list of item keys contains duplicates'))
          seenKeys[key] = true

          puts.push({TableName: tableName, Item: req.PutRequest.Item})
        } else if (req.DeleteRequest) {
          key = db.validateKey(req.DeleteRequest.Key, table)

          if (key instanceof Error) return cb(key)
          if (seenKeys[key])
            return cb(db.validationError('Provided list of item keys contains duplicates'))
          seenKeys[key] = true

          deletes.push({TableName: tableName, Key: req.DeleteRequest.Key})
        }
      }

      cb()
    })
  }

  async.series([
    async.apply(async.forEach, Object.keys(data.RequestItems), validateTableKeys),
    async.apply(async.parallel, [
      async.apply(async.forEach, puts, putItem),
      async.apply(async.forEach, deletes, deleteItem),
    ])
  ], function(err) {
    if (err) return cb(err)
    cb(null, {UnprocessedItems: {}})
  })
}

