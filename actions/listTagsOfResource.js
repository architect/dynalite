var once = require('once'),
    db = require('../db')

module.exports = function listTagsOfResource(store, data, cb) {
  cb = once(cb)

  var tableName = data.ResourceArn.split('/').pop()

  store.getTable(tableName, false, function(err) {
    if (err && err.name == 'NotFoundError') {
      err.body.message = 'Requested resource not found: ResourcArn: ' + data.ResourceArn + ' not found'
    }
    if (err) return cb(err)

    db.lazy(store.getTagDb(tableName).createReadStream(), cb).join(function(tags) {
      cb(null, {Tags: tags.map(function(tag) { return {Key: tag.key, Value: tag.value} })})
    })
  })
}

