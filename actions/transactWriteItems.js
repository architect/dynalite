var async = require('async'),
    putItem = require('./putItem'),
    deleteItem = require('./deleteItem'),
    updateItem = require('./updateItem'),
    db = require('../db')

module.exports = function transactWriteItem(store, data, cb) {
  var actions = []

  async.series([
    async.eachSeries.bind(async, data.TransactItems, addActions),
    async.series.bind(async, actions),
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

  function addActions(transactItem, cb) {
      var options = {}
      var tableName

          if (data.ReturnConsumedCapacity) options.ReturnConsumedCapacity = data.ReturnConsumedCapacity

          if (transactItem.Put) {
            tableName = transactItem.Put.TableName;
            options = {TableName: tableName}

            store.getTable(tableName, function(err, table) {
              if (err) return cb(err)
              if ((err = db.validateItem(transactItem.Put.Item, table)) != null) return cb(err)

              options.Item = transactItem.Put.Item
              actions.push(putItem.bind(null, store, options))
              db.createKey(options.Item, table)

              cb()
            })
          } else if (transactItem.Delete) {
            tableName = transactItem.Delete.TableName;
            options = {TableName: tableName}

            store.getTable(tableName, function(err, table) {
              if (err) return cb(err)
              if ((err = db.validateKey(transactItem.Delete.Key, table) != null)) return cb(err)

              options.Key = transactItem.Delete.Key
              actions.push(deleteItem.bind(null, store, options))

              db.createKey(options.Key, table)

              cb()
            })
          } else if (transactItem.Update) {

            store.getTable(tableName, function(err, table) {
              if (err) return cb(err)
              if ((err = db.validateKey(transactItem.Update.Key, table) != null)) return cb(err)

              options.Key = transactItem.Update.Key
              actions.push(updateItem.bind(null, store, options))

              db.createKey(options.Key, table)

              cb()
            })
          }
  }
}
