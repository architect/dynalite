var db = require('.'),
    once = require('once'),
    Big = require('big.js')

exports.createStreamRecord = createStreamRecord
exports.createShardIteratorToken = createShardIteratorToken
exports.getTableNameFromStreamArn = getTableNameFromStreamArn
exports.encodeIterator = encodeIterator
exports.decodeIterator = decodeIterator

function createStreamRecord(store, table, oldItem, newItem, cb) {
  if (!oldItem && !newItem) {
    throw new Error('Both old and new item are undefined')
  }

  var streamDb = store.getStreamDb(table.LatestStreamArn)

  var insertStreamRecord = once(function(key) {
    if (key) {
      key = Big(key).plus(1).toString()
    } else {
      key = '100000000000000000001'
    }

    var record = {
      awsRegion: store.tableDb.awsRegion,
      dynamodb: {
        ApproximateCreationDateTime: Date.now(),
        Keys: {},
        SequenceNumber: key,
        SizeBytes: 0,
        StreamViewType: table.StreamSpecification.StreamViewType,
      },
      eventID: key,
      eventSource: 'aws:dynamodb',
      eventVersion: '1.1',
    }

    if (oldItem) {
      record.dynamodb.OldImage = oldItem
      record.dynamodb.SizeBytes += db.itemSize(oldItem, false, true)
      record.eventName = 'REMOVE'
    }
    if (newItem) {
      record.dynamodb.NewImage = newItem
      record.dynamodb.SizeBytes += db.itemSize(newItem, false, true)

      if (record.eventName) {
        record.eventName = 'MODIFY'
      } else {
        record.eventName = 'INSERT'
      }
    }

    db.traverseKey(table, function(attr) {
      if (newItem) {
        return record.dynamodb.Keys[attr] = newItem[attr]
      } else {
        return record.dynamodb.Keys[attr] = oldItem[attr]
      }
    })

    streamDb.put(key, record, cb)
  })

  db.lazy(streamDb.createKeyStream({reverse: true, limit: 1}), cb)
    .on('end', insertStreamRecord)
    .head(insertStreamRecord)
}

function createShardIteratorToken(store, iterator, cb) {
  if (iterator.ShardIteratorType == 'LATEST') {
    var processKey = once(function(key) {
      if (key) {
        iterator.ShardIteratorType = 'AFTER_SQUENCE_NUMBER'
        iterator.SequenceNumber = key
      } else {
        iterator.ShardIteratorType = 'TRIM_HORIZON'
      }

      cb(null, encodeIterator(iterator))
    })

    var tableName = getTableNameFromStreamArn(iterator.StreamArn)
    store.getTable(tableName, false, function(err, table) {
      if (err) return cb(err)

      var streamDb = store.getStreamDb(table.LatestStreamArn)
      db.lazy(streamDb.createKeyStream({reverse: true, limit: 1}), cb)
        .on('end', processKey)
        .head(processKey)
    })
  } else {
    cb(null, encodeIterator(iterator))
  }
}

function getTableNameFromStreamArn(streamArn) {
  var streamArnParts = streamArn.split('/stream/'),
      tableArn = streamArnParts[0],
      tableArnParts = tableArn.split(':table/'),
      tableName = tableArnParts[1]

  return tableName
}

function encodeIterator(iterator) {
  var encodedString = Buffer.from(JSON.stringify(iterator)).toString('base64'),
      result = iterator.StreamArn + '|1|' + encodedString

  return result
}

function decodeIterator(token) {
  var iteratorParts = token.split('|1|'),
      encodedString = iteratorParts[1],
      decodedString = Buffer.from(encodedString, 'base64').toString('utf8'),
      iterator = JSON.parse(decodedString)

  return iterator
}
