
module.exports = function describeTimeToLive(store, data, cb) {
  store.getTable(data.TableName, false, function(err) {
    if (err) return cb(err)

    cb(null, {TimeToLiveDescription: {TimeToLiveStatus: 'DISABLED'}})
  })
}
