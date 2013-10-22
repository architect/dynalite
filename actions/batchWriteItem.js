var putItem = require('./putItem'),
    deleteItem = require('./deleteItem')

module.exports = function batchWriteItem(data, cb) {
  var remaining = 0, table
  for (table in data.RequestItems) {
    data.RequestItems[table].forEach(function(req) {
      if (req.PutRequest)
        putItem({TableName: table, Item: req.PutRequest.Item}, checkDone)
      else if (req.DeleteRequest)
        deleteItem({TableName: table, Key: req.DeleteRequest.Key}, checkDone)
      else
        throw new Error('Unknown request: ' + JSON.stringify(req))
      remaining++
    })
  }
  function checkDone(err) {
    if (err) return cb(err)
    if (!--remaining) return cb(null, {UnprocessedItems: {}})
  }
}

