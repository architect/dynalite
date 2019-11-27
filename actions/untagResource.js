var once = require('once')

module.exports = function untagResource(store, data, cb) {
  cb = once(cb)

  var tableName = data.ResourceArn.split('/').pop()

  store.getTable(tableName, false, function(err) {
    if (err && err.name == 'NotFoundError') {
      err.body.message = 'Requested resource not found'
    }
    if (err) return cb(err)

    var batchDeletes = data.TagKeys.map(function(key) { return {type: 'del', key: key} })
    store.getTagDb(tableName).batch(batchDeletes, function(err) {
      if (err) return cb(err)
      cb(null, '')
    })
  })
}

