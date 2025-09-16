var crypto = require('crypto')

module.exports = function createTable (store, data, cb) {

  var key = data.TableName, tableDb = store.tableDb

  tableDb.lock(key, function (release) {
    cb = release(cb)

    tableDb.get(key, function (err, existingTable) {
      if (err && err.name != 'NotFoundError') return cb(err)

      // Check if table exists and is valid
      if (!err && existingTable && typeof existingTable === 'object' && existingTable.TableStatus) {
        err = new Error
        err.statusCode = 400
        err.body = {
          __type: 'com.amazonaws.dynamodb.v20120810#ResourceInUseException',
          message: '',
        }
        return cb(err)
      }

      // If table exists but is corrupted, delete it first
      if (!err && existingTable && (!existingTable.TableStatus || typeof existingTable !== 'object')) {
        tableDb.del(key, function () {
          // Ignore deletion errors and proceed with creation
          createNewTable()
        })
        return
      }

      // Table doesn't exist, create it
      createNewTable()

      function createNewTable () {
        data.TableArn = 'arn:aws:dynamodb:' + tableDb.awsRegion + ':' + tableDb.awsAccountId + ':table/' + data.TableName
        data.TableId = uuidV4()
        data.CreationDateTime = Date.now() / 1000
        data.ItemCount = 0
        if (!data.ProvisionedThroughput) {
          data.ProvisionedThroughput = { ReadCapacityUnits: 0, WriteCapacityUnits: 0 }
        }
        data.ProvisionedThroughput.NumberOfDecreasesToday = 0
        data.TableSizeBytes = 0
        data.TableStatus = 'CREATING'
        if (data.BillingMode == 'PAY_PER_REQUEST') {
          data.BillingModeSummary = { BillingMode: 'PAY_PER_REQUEST' }
          data.TableThroughputModeSummary = { TableThroughputMode: 'PAY_PER_REQUEST' }
          delete data.BillingMode
        }
        if (data.LocalSecondaryIndexes) {
          data.LocalSecondaryIndexes.forEach(function (index) {
            index.IndexArn = 'arn:aws:dynamodb:' + tableDb.awsRegion + ':' + tableDb.awsAccountId + ':table/' +
            data.TableName + '/index/' + index.IndexName
            index.IndexSizeBytes = 0
            index.ItemCount = 0
          })
        }
        if (data.GlobalSecondaryIndexes) {
          data.GlobalSecondaryIndexes.forEach(function (index) {
            index.IndexArn = 'arn:aws:dynamodb:' + tableDb.awsRegion + ':' + tableDb.awsAccountId + ':table/' +
            data.TableName + '/index/' + index.IndexName
            index.IndexSizeBytes = 0
            index.ItemCount = 0
            index.IndexStatus = 'CREATING'
            if (!index.ProvisionedThroughput) {
              index.ProvisionedThroughput = { ReadCapacityUnits: 0, WriteCapacityUnits: 0 }
            }
            index.ProvisionedThroughput.NumberOfDecreasesToday = 0
          })
        }

        tableDb.put(key, data, function (err) {
          if (err) return cb(err)

          setTimeout(function () {

            // Shouldn't need to lock/fetch as nothing should have changed
            data.TableStatus = 'ACTIVE'
            if (data.GlobalSecondaryIndexes) {
              data.GlobalSecondaryIndexes.forEach(function (index) {
                index.IndexStatus = 'ACTIVE'
              })
            }

            if (data.BillingModeSummary) {
              data.BillingModeSummary.LastUpdateToPayPerRequestDateTime = data.CreationDateTime
            }

            tableDb.put(key, data, function (err) {

              if (err && !/Database is (not open|closed)/.test(err)) console.error(err.stack || err)
            })

          }, store.options.createTableMs)

          cb(null, { TableDescription: data })
        })
      }
    })
  })

}

function uuidV4 () {
  var bytes = crypto.randomBytes(14).toString('hex')
  return bytes.slice(0, 8) + '-' + bytes.slice(8, 12) + '-4' + bytes.slice(13, 16) + '-' +
    bytes.slice(16, 20) + '-' + bytes.slice(20, 28)
}
