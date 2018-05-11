var kinesaliteGetShardIterator = require('kinesalite/actions/getShardIterator'),
    utils = require('../utils')

module.exports = function getShardIterator(store, data, cb) {
  var options = {
    ShardId: utils.makeShardIdShort(data.ShardId),
    ShardIteratorType: data.ShardIteratorType,
    StartingSequenceNumber: data.SequenceNumber,
    StreamName: utils.getTableNameFromStreamArn(data.StreamArn),
  }

  kinesaliteGetShardIterator(store.kinesalite, options, cb)
}
