var once = require('once'),
    streams = require('../db/streams')

module.exports = function getShardIterator(store, data, cb) {
  cb = once(cb)

  streams.createShardIteratorToken(store, data, function(err, token) {
    if (err) return cb(err)

    cb(null, {ShardIterator: token})
  })
}
