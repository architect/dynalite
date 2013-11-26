
module.exports = function createTable(store, data, cb) {

  var key = data.TableName, tableDb = store.tableDb

  tableDb.lock(key, function(release) {
    cb = release(cb)

    tableDb.get(key, function(err) {
      if (err && err.name != 'NotFoundError') return cb(err)
      if (!err) {
        err = new Error
        err.statusCode = 400
        err.body = {
          __type: 'com.amazonaws.dynamodb.v20120810#ResourceInUseException',
          message: '',
        }
        return cb(err)
      }

      data.CreationDateTime = Date.now() / 1000
      data.ItemCount = 0
      data.ProvisionedThroughput.NumberOfDecreasesToday = 0
      data.TableSizeBytes = 0
      data.TableStatus = 'CREATING'
      if (data.LocalSecondaryIndexes) {
        data.LocalSecondaryIndexes.forEach(function(index) {
          index.IndexSizeBytes = 0
          index.ItemCount = 0
        })
      }

      tableDb.put(key, data, function(err) {
        if (err) return cb(err)

        setTimeout(function() {

          // Shouldn't need to lock/fetch as nothing should have changed
          data.TableStatus = 'ACTIVE'

          tableDb.put(key, data, function(err) {
            // TODO: Need to check this
            if (err) console.error(err)
          })

        }, store.createTableMs)

        cb(null, {TableDescription: data})
      })
    })
  })

}


