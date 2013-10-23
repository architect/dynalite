var scan = require('./scan')

module.exports = function query(data, cb) {
  data.ScanFilter = data.KeyConditions
  delete data.KeyConditions
  scan(data, function(err, response) {
    if (err) return cb(err)
    delete response.ScannedCount
    cb(null, response)
  })
}

