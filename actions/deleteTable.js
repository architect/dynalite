var tableDb = require('../db').tableDb

module.exports = function deleteTable(data, cb) {

  tableDb.get(data.TableName, function(err, table) {
    if (err) {
      if (err.name == 'NotFoundError') {
        err.statusCode = 400
        err.body = {
          __type: 'com.amazonaws.dynamodb.v20120810#ResourceNotFoundException',
          message: 'Requested resource not found: Table: ' + data.TableName + ' not found',
        }
      }
      return cb(err)
    }
    cb(null, {Table: table})
  })
}


