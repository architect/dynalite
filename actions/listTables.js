
module.exports = function listTables(data, cb) {
  // needs to be anything > ExclusiveStartTableName
  cb(null, {TableNames: []})
}

