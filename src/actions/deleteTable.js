var async = require('async')

module.exports = function deleteTable (store, data, cb) {

  var key = data.TableName, tableDb = store.tableDb

  store.getTable(key, false, function (err, table) {
    if (err) return cb(err)

    // Handle corrupted table entries
    if (!table || typeof table !== 'object') {
      // Table entry is corrupted, treat as if table doesn't exist
      err = new Error
      err.statusCode = 400
      err.body = {
        __type: 'com.amazonaws.dynamodb.v20120810#ResourceNotFoundException',
        message: 'Requested resource not found: Table: ' + key + ' not found',
      }
      return cb(err)
    }

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

    var deletes = [ store.deleteItemDb.bind(store, key), store.deleteTagDb.bind(store, key) ]
    ;[ 'Local', 'Global' ].forEach(function (indexType) {
      var indexes = table[indexType + 'SecondaryIndexes'] || []
      deletes = deletes.concat(indexes.map(function (index) {
        return store.deleteIndexDb.bind(store, indexType, table.TableName, index.IndexName)
      }))
    })

    delete table.GlobalSecondaryIndexes

    tableDb.put(key, table, function (err) {
      if (err) return cb(err)

      async.parallel(deletes, function (err) {
        if (err) return cb(err)

        setTimeout(function () {
          tableDb.del(key, function (err) {

            if (err && !/Database is (not open|closed)/.test(err)) console.error(err.stack || err)
          })
        }, store.options.deleteTableMs)

        cb(null, { TableDescription: table })
      })
    })
  })

}
