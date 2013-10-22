var getItem = require('./getItem')

module.exports = function batchGetItem(data, cb) {
  var remaining = 0, responses = {}, table
  for (table in data.RequestItems) {
    responses[table] = []
    data.RequestItems[table].Keys.forEach(function(key) {
      var options = {TableName: table, Key: key}
      if (data.RequestItems[table].AttributesToGet)
        options.AttributesToGet = data.RequestItems[table].AttributesToGet
      getItem(options, checkDone(table))
      remaining++
    })
  }
  function checkDone(table) {
    return function(err, item) {
      if (err) return cb(err)
      if (item.Item) responses[table].push(item.Item)
      if (!--remaining) return cb(null, {Responses: responses, UnprocessedKeys: {}})
    }
  }
}

