var tableDb = require('../db').tableDb

module.exports = function createTable(data, cb) {

  data.CreationDateTime = Date.now() / 1000
  data.ItemCount = 0
  data.ProvisionedThroughput.NumberOfDecreasesToday = 0
  data.TableSizeBytes = 0
  data.TableStatus = 'CREATING'
  if (data.LocalSecondaryIndexes) {
    data.LocalSecondaryIndexes.forEach(function(index) {
      index.IndexSizeBytes = 0
      index.ItemCount = 0
    })
  }

  tableDb.put(data.TableName, data, function(err) {
    cb(err, {TableDescription: data})
  })
}


