var once = require('once')

module.exports = function tagResource(store, data, cb) {
  cb = once(cb)

  var tableName = data.ResourceArn.split('/').pop()

  store.getTable(tableName, false, function(err) {
    if (err && err.name == 'NotFoundError') {
      err.body.message = 'Requested resource not found: ResourcArn: ' + data.ResourceArn + ' not found'
    }
    if (err) return cb(err)

    var batchPuts = data.Tags.map(function(tag) { return {type: 'put', key: tag.Key, value: tag.Value} })
    store.getTagDb(tableName).batch(batchPuts, function(err) {
      if (err) return cb(err)
      cb(null, '')
    })
  })
}
