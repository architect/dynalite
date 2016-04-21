var once = require('once'),
    db = require('../db')

module.exports = function listTables(store, data, cb) {
  cb = once(cb)
  var opts, limit = data.Limit || 100

  if (data.ExclusiveStartTableName)
    opts = {gt: data.ExclusiveStartTableName}

  db.lazy(store.tableDb.createKeyStream(opts), cb)
    .take(limit + 1)
    .join(function(names) {
      var result = {}
      if (names.length > limit) {
        names.splice(limit)
        result.LastEvaluatedTableName = names[names.length - 1]
      }
      result.TableNames = names
      cb(null, result)
    })
}
