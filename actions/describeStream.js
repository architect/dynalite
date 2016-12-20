module.exports = function describeStream(store, data, cb) {
  var streamArnParts = data.StreamArn.split('/stream/'),
      tableArn = streamArnParts[0],
      tableArnParts = tableArn.split(':table/'),
      tableName = tableArnParts[1]

  store.getTable(tableName, false, function(err, table) {
    if (err) return cb(err)

    cb(null, {
      StreamDescription: {
        CreationRequestDateTime: table.CreationDateTime,
        KeySchema: table.KeySchema,
        Shards: [
          {
            ShardId: 'shardId-00000000000000000000-00000001',
            SequenceNumberRange: {
              StartingSequenceNumber: '100000000000000000001',
            },
          },
        ],
        StreamArn: table.LatestStreamArn,
        StreamLabel: table.LatestStreamLabel,
        StreamStatus: 'ENABLED',
        StreamViewType: table.StreamSpecification.StreamViewType,
        TableName: table.TableName,
      },
    })
  })
}
