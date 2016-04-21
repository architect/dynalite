var async = require('async')

module.exports = function deleteTable(store, data, cb) {

  var key = data.TableName, tableDb = store.tableDb

  store.getTable(key, false, function(err, table) {
    if (err) return cb(err)

    // Check if table is ACTIVE or not?
    if (table.TableStatus == 'CREATING') {
      err = new Error
      err.statusCode = 400
      err.body = {
        __type: 'com.amazonaws.dynamodb.v20120810#ResourceInUseException',
        message: 'Attempt to change a resource which is still in use: Table is being created: ' + key,
      }
      return cb(err)
    }

    table.TableStatus = 'DELETING'

    var deletes = [store.deleteItemDb.bind(store, key)]
    ;['Local', 'Global'].forEach(function(indexType) {
      var indexes = table[indexType + 'SecondaryIndexes'] || []
      deletes = deletes.concat(indexes.map(function(index) {
        return store.deleteIndexDb.bind(store, indexType, table.TableName, index.IndexName)
      }))
    })

    delete table.GlobalSecondaryIndexes

    tableDb.put(key, table, function(err) {
      if (err) return cb(err)

      async.parallel(deletes, function(err) {
        if (err) return cb(err)

        setTimeout(function() {
          tableDb.del(key, function(err) {
            // eslint-disable-next-line no-console
            if (err && !/Database is not open/.test(err)) console.error(err.stack || err)
          })
        }, store.options.deleteTableMs)

        cb(null, {TableDescription: table})
      })
    })
  })

}
