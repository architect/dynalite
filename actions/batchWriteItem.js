var putItem = require('./putItem')

module.exports = function batchWriteItem(data, cb) {
  var remaining = 0, table
  for (table in data.RequestItems) {
    data.RequestItems[table].map(function(req) { return req.PutRequest.Item }).forEach(function(item) {
      putItem({TableName: table, Item: item}, checkDone)
      remaining++
    })
  }
  function checkDone(err) {
    if (err) return cb(err)
    if (!--remaining) return cb(null, {})
  }
}

