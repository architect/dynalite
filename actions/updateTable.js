var db = require('../db')

module.exports = function updateTable(store, data, cb) {

  var key = data.TableName, tableDb = store.tableDb

  tableDb.lock(key, function(release) {
    cb = release(cb)

    tableDb.get(key, function(err, table) {
      if (err) return cb(err)

      var readDiff = data.ProvisionedThroughput.ReadCapacityUnits - table.ProvisionedThroughput.ReadCapacityUnits,
          writeDiff = data.ProvisionedThroughput.WriteCapacityUnits - table.ProvisionedThroughput.WriteCapacityUnits

      if (!readDiff && !writeDiff)
        return cb(db.validationError(
          'The provisioned throughput for the table will not change. The requested value equals the current value. ' +
          'Current ReadCapacityUnits provisioned for the table: ' + table.ProvisionedThroughput.ReadCapacityUnits +
          '. Requested ReadCapacityUnits: ' + data.ProvisionedThroughput.ReadCapacityUnits + '. ' +
          'Current WriteCapacityUnits provisioned for the table: ' + table.ProvisionedThroughput.WriteCapacityUnits +
          '. Requested WriteCapacityUnits: ' + data.ProvisionedThroughput.WriteCapacityUnits + '. ' +
          'Refer to the Amazon DynamoDB Developer Guide for current limits and how to request higher limits.'))

      table.TableStatus = 'UPDATING'
      if (readDiff > 0 || writeDiff > 0) table.ProvisionedThroughput.LastIncreaseDateTime = Date.now() / 1000
      if (readDiff < 0 || writeDiff < 0) table.ProvisionedThroughput.LastDecreaseDateTime = Date.now() / 1000

      tableDb.put(key, table, function(err) {
        if (err) return cb(err)

        setTimeout(function() {

          // Shouldn't need to lock/fetch as nothing should have changed
          table.TableStatus = 'ACTIVE'
          if (readDiff < 0 || writeDiff < 0) table.ProvisionedThroughput.NumberOfDecreasesToday++
          table.ProvisionedThroughput.ReadCapacityUnits = data.ProvisionedThroughput.ReadCapacityUnits
          table.ProvisionedThroughput.WriteCapacityUnits = data.ProvisionedThroughput.WriteCapacityUnits

          tableDb.put(key, table, function(err) {
            // TODO: Need to check this
            if (err) console.error(err)
          })

        }, db.updateTableMs)

        cb(null, {TableDescription: table})
      })
    })
  })

}


