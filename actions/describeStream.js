var kinesaliteDescribeStream = require('kinesalite/actions/describeStream'),
    utils = require('../utils')

module.exports = function describeStream(store, data, cb) {
  var tableName = utils.getTableNameFromStreamArn(data.StreamArn)

  store.getTable(tableName, false, function(err, table) {
    if (err) return cb(err)

    kinesaliteDescribeStream(store.kinesalite, {StreamName: tableName}, function(err, stream) {
      if (err) return cb(err)

      cb(null, {
        StreamDescription: {
          CreationRequestDateTime: stream.StreamDescription.StreamCreationTimestamp,
          KeySchema: table.KeySchema,
          Shards: stream.StreamDescription.Shards.map(function(shard) {
            shard.ShardId = utils.makeShardIdLong(shard.ShardId)
            shard.ParentShardId = utils.makeShardIdLong(shard.ParentShardId)
            return shard
          }),
          StreamArn: table.LatestStreamArn,
          StreamLabel: table.LatestStreamLabel,
          StreamStatus: stream.StreamDescription.StreamStatus,
          StreamViewType: table.StreamSpecification.StreamViewType,
          TableName: tableName,
        },
      })
    })
  })
}
