var lazy = require('lazy'),
    once = require('once'),
    db = require('../db'),
    tableDb = db.tableDb

module.exports = function listTables(data, cb) {
  cb = once(cb)
  var opts

  if (data.ExclusiveStartTableName)
    opts = {start: data.ExclusiveStartTableName + '\x00'}

  var keys = lazy(tableDb.createKeyStream(opts)).on('error', cb)

  if (data.Limit) keys = keys.take(data.Limit)

  keys.join(function(names) {
    var result = {TableNames: names}
    if (data.Limit) result.LastEvaluatedTableName = names[names.length - 1]
    cb(null, result)
  })
}

