var async = require('async'),
    crypto = require('crypto'),
    kinesaliteCreateStream = require('kinesalite/actions/createStream')

module.exports = function createTable(store, data, cb) {

  var key = data.TableName, tableDb = store.tableDb

  async.auto({
    lock: function(callback) {
      tableDb.lock(key, function(release) {
        callback(null, release)
      })
    },
    checkTable: ['lock', function(results, callback) {
      tableDb.get(key, function(err) {
        if (err && err.name != 'NotFoundError') return callback(err)
        if (!err) {
          err = new Error
          err.statusCode = 400
          err.body = {
            __type: 'com.amazonaws.dynamodb.v20120810#ResourceInUseException',
            message: '',
          }
          return callback(err)
        }

        callback()
      })
    }],
    streamUpdates: ['checkTable', function(results, callback) {
      if (!data.StreamSpecification) {
        return callback()
      }

      kinesaliteCreateStream(store.kinesalite, {StreamName: data.TableName, ShardCount: 1}, function(err) {
        if (err) return callback(error)

        callback(null, {
          StreamSpecification: data.StreamSpecification,
          LatestStreamLabel: (new Date()).toISOString().replace('Z', ''),
          LatestStreamArn: 'arn:aws:dynamodb:' + tableDb.awsRegion + ':' + tableDb.awsAccountId + ':table/' + data.TableName + '/stream/' + data.LatestStreamLabel,
        })
      })
    }],
    createTable: ['streamUpdates', function(results, callback) {
      data.TableArn = 'arn:aws:dynamodb:' + tableDb.awsRegion + ':' + tableDb.awsAccountId + ':table/' + data.TableName
      data.TableId = uuidV4()
      data.CreationDateTime = Date.now() / 1000
      data.ItemCount = 0
      data.ProvisionedThroughput.NumberOfDecreasesToday = 0
      data.TableSizeBytes = 0
      data.TableStatus = 'CREATING'
      if (data.LocalSecondaryIndexes) {
        data.LocalSecondaryIndexes.forEach(function(index) {
          index.IndexArn = 'arn:aws:dynamodb:' + tableDb.awsRegion + ':' + tableDb.awsAccountId + ':table/' +
            data.TableName + '/index/' + index.IndexName
          index.IndexSizeBytes = 0
          index.ItemCount = 0
        })
      }
      if (data.GlobalSecondaryIndexes) {
        data.GlobalSecondaryIndexes.forEach(function(index) {
          index.IndexArn = 'arn:aws:dynamodb:' + tableDb.awsRegion + ':' + tableDb.awsAccountId + ':table/' +
            data.TableName + '/index/' + index.IndexName
          index.IndexSizeBytes = 0
          index.ItemCount = 0
          index.IndexStatus = 'CREATING'
          index.ProvisionedThroughput.NumberOfDecreasesToday = 0
        })
      }

      if (results.streamUpdates) {
        data.LatestStreamLabel = (new Date()).toISOString().replace('Z', '')
        data.LatestStreamArn = 'arn:aws:dynamodb:' + tableDb.awsRegion + ':' + tableDb.awsAccountId + ':table/' + data.TableName + '/stream/' + data.LatestStreamLabel
      }

      tableDb.put(key, data, callback)
    }],
    setActive: ['createTable', function(results, callback) {
      setTimeout(function() {

        // Shouldn't need to lock/fetch as nothing should have changed
        data.TableStatus = 'ACTIVE'
        if (data.GlobalSecondaryIndexes) {
          data.GlobalSecondaryIndexes.forEach(function(index) {
            index.IndexStatus = 'ACTIVE'
          })
        }

        tableDb.put(key, data, function(err) {
          // eslint-disable-next-line no-console
          if (err && !/Database is not open/.test(err)) console.error(err.stack || err)
        })

      }, store.options.createTableMs)

      callback()
    }],
  }, function(err, results) {
    var release = results.lock
    cb = release(cb)

    if (err) {
      return cb(err)
    }

    cb(null, {TableDescription: data})
  })
}

function uuidV4() {
  var bytes = crypto.randomBytes(14).toString('hex')
  return bytes.slice(0, 8) + '-' + bytes.slice(8, 12) + '-4' + bytes.slice(13, 16) + '-' +
    bytes.slice(16, 20) + '-' + bytes.slice(20, 28)
}
