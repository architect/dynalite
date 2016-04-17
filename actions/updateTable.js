var db = require('../db')

module.exports = function updateTable(store, data, cb) {

  var key = data.TableName, tableDb = store.tableDb

  tableDb.lock(key, function(release) {
    cb = release(cb)

    tableDb.get(key, function(err, table) {
      if (err) return cb(err)

      var updates = getThroughputUpdates(data, table),
          i, update, dataThroughput, tableThroughput, readDiff, writeDiff

      for (i = 0; i < updates.length; i++) {
        update = updates[i]
        dataThroughput = update.dataThroughput
        tableThroughput = update.tableThroughput
        readDiff = dataThroughput.ReadCapacityUnits - tableThroughput.ReadCapacityUnits
        writeDiff = dataThroughput.WriteCapacityUnits - tableThroughput.WriteCapacityUnits

        if (!readDiff && !writeDiff)
          return cb(db.validationError(
            'The provisioned throughput for the table will not change. The requested value equals the current value. ' +
            'Current ReadCapacityUnits provisioned for the table: ' + tableThroughput.ReadCapacityUnits +
            '. Requested ReadCapacityUnits: ' + dataThroughput.ReadCapacityUnits + '. ' +
            'Current WriteCapacityUnits provisioned for the table: ' + tableThroughput.WriteCapacityUnits +
            '. Requested WriteCapacityUnits: ' + dataThroughput.WriteCapacityUnits + '. ' +
            'Refer to the Amazon DynamoDB Developer Guide for current limits and how to request higher limits.'))

        update.setStatus('UPDATING')

        if (readDiff > 0 || writeDiff > 0) tableThroughput.LastIncreaseDateTime = Date.now() / 1000
        if (readDiff < 0 || writeDiff < 0) tableThroughput.LastDecreaseDateTime = Date.now() / 1000

        update.readDiff = readDiff
        update.writeDiff = writeDiff
      }

      tableDb.put(key, table, function(err) {
        if (err) return cb(err)

        setTimeout(function() {

          // Shouldn't need to lock/fetch as nothing should have changed
          updates.forEach(function(update) {
            dataThroughput = update.dataThroughput
            tableThroughput = update.tableThroughput

            update.setStatus('ACTIVE')

            if (update.readDiff > 0 || update.writeDiff > 0) {
              tableThroughput.LastIncreaseDateTime = Date.now() / 1000
            } else if (update.readDiff < 0 || update.writeDiff < 0) {
              tableThroughput.LastDecreaseDateTime = Date.now() / 1000
              tableThroughput.NumberOfDecreasesToday++
            }

            tableThroughput.ReadCapacityUnits = dataThroughput.ReadCapacityUnits
            tableThroughput.WriteCapacityUnits = dataThroughput.WriteCapacityUnits
          })

          tableDb.put(key, table, function(err) {
            // eslint-disable-next-line no-console
            if (err && !/Database is not open/.test(err)) console.error(err.stack || err)
          })

        }, store.options.updateTableMs)

        cb(null, {TableDescription: table})
      })
    })
  })

}

function getThroughputUpdates(data, table) {
  var updates = []
  if (data.ProvisionedThroughput) {
    updates.push({
      dataThroughput: data.ProvisionedThroughput,
      tableThroughput: table.ProvisionedThroughput,
      setStatus: function(status) { table.TableStatus = status },
    })
  }
  if (data.GlobalSecondaryIndexUpdates && table.GlobalSecondaryIndexes) {
    data.GlobalSecondaryIndexUpdates.forEach(function(update) {
      table.GlobalSecondaryIndexes.forEach(function(index) {
        if (update.Update && index.IndexName == update.Update.IndexName) {
          updates.push({
            dataThroughput: update.Update.ProvisionedThroughput,
            tableThroughput: index.ProvisionedThroughput,
            setStatus: function(status) { index.IndexStatus = status },
          })
        }
      })
    })
  }
  return updates
}
