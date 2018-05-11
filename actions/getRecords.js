var kinesaliteGetRecords = require('kinesalite/actions/getRecords')

module.exports = function getRecords(store, data, cb) {
  kinesaliteGetRecords(store.kinesalite, data, function(err, results) {
    if (err) return cb(err)

    cb(null, {
      NextShardIterator: results.NextShardIterator,
      Records: results.Records.map(function(record) {
        var recordData = JSON.parse(record.Data)
        recordData.dynamodb.SequenceNumber = record.SequenceNumber
        return recordData
      }),
    })
  })
}
