var once = require('once'),
  db = require('../db')

module.exports = function listTagsOfResource (store, data, cb) {
  cb = once(cb)

  var tableName = data.ResourceArn.split('/').pop()

  store.getTable(tableName, false, function (err) {
    if (err && err.name == 'NotFoundError') {
      err.body.message = 'Requested resource not found: ResourcArn: ' + data.ResourceArn + ' not found'
    }
    if (err) return cb(err)

    // Get both keys and values from the tag database
    var tagDb = store.getTagDb(tableName)
    var keys = []
    var values = []

    db.lazy(tagDb.createKeyStream(), cb).join(function (tagKeys) {
      keys = tagKeys
      db.lazy(tagDb.createValueStream(), cb).join(function (tagValues) {
        values = tagValues

        // Combine keys and values into tag objects
        var tags = keys.map(function (key, index) {
          return { Key: key, Value: values[index] }
        })

        cb(null, { Tags: tags })
      })
    })
  })
}

