var once = require('once'),
    db = require('../db'),
    tableDb = db.tableDb

module.exports = function listTables(data, cb) {
  cb = once(cb)
  var opts, keys

  if (data.ExclusiveStartTableName)
    opts = {start: data.ExclusiveStartTableName + '\x00'}

  keys = db.lazy(tableDb.createKeyStream(opts), cb)

  if (data.Limit) keys = keys.take(data.Limit)

  keys.join(function(names) {
    var result = {TableNames: names}
    if (data.Limit) result.LastEvaluatedTableName = names[names.length - 1]
    cb(null, result)
  })
}

