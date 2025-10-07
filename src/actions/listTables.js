var once = require('once'),
  db = require('../db')

module.exports = function listTables (store, data, cb) {
  cb = once(cb)
  var opts = {}, limit = data.Limit || 100

  // Don't use opts.gt since it doesn't work in this LevelDB implementation
  // We'll filter manually after getting all results

  db.lazy(store.tableDb.createKeyStream(opts), cb)
    .take(Infinity) // Take all items since we need to filter manually
    .join(function (names) {
      // Filter to implement proper ExclusiveStartTableName behavior
      // LevelDB's gt option doesn't work properly in this implementation
      if (data.ExclusiveStartTableName) {
        names = names.filter(function (name) {
          return name > data.ExclusiveStartTableName
        })
      }

      // Apply limit after filtering
      var result = {}
      if (names.length > limit) {
        names = names.slice(0, limit)
        result.LastEvaluatedTableName = names[names.length - 1]
      }
      result.TableNames = names
      cb(null, result)
    })
}
