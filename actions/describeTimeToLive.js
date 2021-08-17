
module.exports = function describeTimeToLive (store, data, cb) {
  store.getTable(data.TableName, false, function (err, table) {
    if (err) return cb(err)

    if (table.TimeToLiveDescription !== null && typeof table.TimeToLiveDescription === 'object') {
      cb(null, { TimeToLiveDescription: table.TimeToLiveDescription })
    } else {
      cb(null, { TimeToLiveDescription: { TimeToLiveStatus: 'DISABLED' } })
    }
  })
}
