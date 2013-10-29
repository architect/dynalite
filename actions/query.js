var scan = require('./scan')

module.exports = function query(data, cb) {
  var limit
  data.ScanFilter = data.KeyConditions
  delete data.KeyConditions
  if (data.Limit) {
    limit = data.Limit
    delete data.Limit
  }
  scan(data, function(err, result) {
    if (err) return cb(err)
    delete result.ScannedCount
    if (result.Items) {
      if (data.ScanIndexForward === false) result.Items.reverse()
      if (limit) {
        result.Items.splice(limit)
        result.Count = result.Items.length
        if (result.Count) result.LastEvaluatedKey = result.Items[result.Items.length - 1]
      }
    } else if (limit && result.Count > limit) {
      result.Count = limit
    }
    cb(null, result)
  })
}

