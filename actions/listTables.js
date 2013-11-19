var once = require('once'),
    db = require('../db')

module.exports = function listTables(store, data, cb) {
  cb = once(cb)
  var opts, keys

  if (data.ExclusiveStartTableName)
    opts = {start: data.ExclusiveStartTableName + '\x00'}

  keys = db.lazy(store.tableDb.createKeyStream(opts), cb)

  if (data.Limit) keys = keys.take(data.Limit)

  keys.join(function(names) {
    var result = {TableNames: names}
    if (data.Limit) result.LastEvaluatedTableName = names[names.length - 1]
    cb(null, result)
  })
}

