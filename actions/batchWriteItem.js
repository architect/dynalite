var async = require('async'),
    putItem = require('./putItem'),
    deleteItem = require('./deleteItem'),
    db = require('../db')

module.exports = function batchWriteItem(store, data, cb) {
  var actions = []

  async.series([
    async.each.bind(async, Object.keys(data.RequestItems), addTableActions),
    async.parallel.bind(async, actions),
  ], function(err, responses) {
    if (err) {
      if (err.body && (/Missing the key/.test(err.body.message) || /Type mismatch for key/.test(err.body.message)))
        err.body.message = 'The provided key element does not match the schema'
      return cb(err)
    }
    var res = {UnprocessedItems: {}}, tableUnits = {}

    if (~['TOTAL', 'INDEXES'].indexOf(data.ReturnConsumedCapacity)) {
      responses[1].forEach(function(action) {
        var table = action.ConsumedCapacity.TableName
        if (!tableUnits[table]) tableUnits[table] = 0
        tableUnits[table] += action.ConsumedCapacity.CapacityUnits
      })
      res.ConsumedCapacity = Object.keys(tableUnits).map(function(table) {
        return {
          CapacityUnits: tableUnits[table],
          TableName: table,
          Table: data.ReturnConsumedCapacity == 'INDEXES' ? {CapacityUnits: tableUnits[table]} : undefined,
        }
      })
    }

    cb(null, res)
  })

  function addTableActions(tableName, cb) {
    store.getTable(tableName, function(err, table) {
      if (err) return cb(err)

      var reqs = data.RequestItems[tableName], i, req, key, seenKeys = {}, options

      for (i = 0; i < reqs.length; i++) {
        req = reqs[i]

        options = {TableName: tableName}
        if (data.ReturnConsumedCapacity) options.ReturnConsumedCapacity = data.ReturnConsumedCapacity

        if (req.PutRequest) {

          if ((err = db.validateItem(req.PutRequest.Item, table)) != null) return cb(err)

          options.Item = req.PutRequest.Item
          actions.push(putItem.bind(null, store, options))

          key = db.createKey(options.Item, table)

        } else if (req.DeleteRequest) {

          if ((err = db.validateKey(req.DeleteRequest.Key, table) != null)) return cb(err)

          options.Key = req.DeleteRequest.Key
          actions.push(deleteItem.bind(null, store, options))

          key = db.createKey(options.Key, table)
        }
        if (seenKeys[key])
          return cb(db.validationError('Provided list of item keys contains duplicates'))
        seenKeys[key] = true
      }

      cb()
    })
  }
}
