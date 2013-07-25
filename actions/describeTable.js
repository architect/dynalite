var db = require('../db')

module.exports = function describeTable(data, cb) {

  db.getTable(data.TableName, false, function(err, table) {
    if (err) return cb(err)

    cb(null, {Table: table})
  })
}


