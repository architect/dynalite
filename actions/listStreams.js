var once = require('once'),
    db = require('../db'),
    getTableNameFromStreamArn = require('../utils').getTableNameFromStreamArn

module.exports = function listStreams(store, data, cb) {
  cb = once(cb)
  var opts, limit = data.Limit || 100

  if (data.ExclusiveStartStreamArn) {
    opts = {gt: getTableNameFromStreamArn(data.ExclusiveStartStreamArn)}
  }

  db.lazy(store.tableDb.createValueStream(opts), cb)
    .filter(function(table) {
      return table.hasOwnProperty('StreamSpecification')
    })
    .take(limit + 1)
    .map(function(table) {
      return {
        StreamArn: table.LatestStreamArn,
        StreamLabel: table.LatestStreamLabel,
        TableName: table.TableName,
      }
    })
    .join(function(streams) {
      var result = {}
      if (streams.length > limit) {
        streams.splice(limit)
        result.LastEvaluatedStreamArn = streams[streams.length - 1].StreamArn
      }
      result.Streams = streams
      cb(null, result)
    })
}
