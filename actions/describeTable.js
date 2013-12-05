
module.exports = function describeTable(store, data, cb) {

  store.getTable(data.TableName, false, function(err, table) {
    if (err) return cb(err)

    cb(null, {Table: table})
  })
}


