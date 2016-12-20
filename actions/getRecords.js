var once = require('once'),
    db = require('../db'),
    streams = require('../db/streams')

module.exports = function getRecords(store, data, cb) {
  cb = once(cb)

  var opts, limit = data.Limit || 100,
      iterator = streams.decodeIterator(data.ShardIterator),
      tableName = streams.getTableNameFromStreamArn(iterator.StreamArn)

  if (iterator.SequenceNumber) {
    switch (iterator.ShardIteratorType) {
      case 'AT_SEQUENCE_NUMBER':
        opts = {gte: iterator.SequenceNumber}
        break
      case 'AFTER_SQUENCE_NUMBER':
        opts = {gt: iterator.SequenceNumber}
        break
    }
  }

  store.getTable(tableName, false, function(err, table) {
    if (err) return cb(err)

    var streamDb = store.getStreamDb(table.LatestStreamArn)
    db.lazy(streamDb.createValueStream(opts), cb)
      .take(limit + 1)
      .join(function(records) {
        var result = {}
        if (records.length > limit) {
          iterator.ShardIteratorType = 'AT_SEQUENCE_NUMBER'
          iterator.SequenceNumber = records[limit].eventID

          records.splice(limit)

          result.NextShardIterator = streams.encodeIterator(iterator)
        } else if (records.length > 0) {
          iterator.ShardIteratorType = 'AFTER_SQUENCE_NUMBER'
          iterator.SequenceNumber = records[records.length - 1].eventID

          result.NextShardIterator = streams.encodeIterator(iterator)
        } else {
          result.NextShardIterator = data.ShardIterator
        }
        result.Records = records

        cb(null, result)
      })
  })
}
