var getItem = require('./getItem'),
    db = require('../db')

module.exports = function batchGetItem(data, cb) {
  var remaining = 0, responses = {}, table, seenKeys, i, key, keyStr
  for (table in data.RequestItems) {
    responses[table] = []
    seenKeys = {}
    for (i = 0; i < data.RequestItems[table].Keys.length; i++) {
      key = data.RequestItems[table].Keys[i]
      keyStr = JSON.stringify(key)
      if (seenKeys[keyStr])
        return cb(db.validationError('Provided list of item keys contains duplicates'))
      seenKeys[keyStr] = true
    }
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

