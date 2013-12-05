var once = require('once'),
    db = require('../db')

module.exports = function scan(store, data, cb) {
  cb = once(cb)

  store.getTable(data.TableName, function(err, table) {
    if (err) return cb(err)

    var opts = {}, valStream, vals, scannedCount = 0, itemDb = store.getItemDb(data.TableName),
        size = 0, capacitySize = 0, exclusiveLexiKey, lastItem

    if (data.TotalSegments > 1) {
      if (data.Segment > 0)
        opts.start = ('00' + Math.ceil(4096 * data.Segment / data.TotalSegments).toString(16)).slice(-3)
      opts.end = ('00' + (Math.ceil(4096 * (data.Segment + 1) / data.TotalSegments) - 1).toString(16)).slice(-3) + '\xff'
    }

    if (data.ExclusiveStartKey) {
      if (table.KeySchema.some(function(schemaPiece) { return !data.ExclusiveStartKey[schemaPiece.AttributeName] })) {
        return cb(db.validationError('The provided starting key is invalid: ' +
          'The provided key element does not match the schema'))
      }
      exclusiveLexiKey = db.validateKey(data.ExclusiveStartKey, table)
      if (data.TotalSegments > 1 && (exclusiveLexiKey < opts.start || exclusiveLexiKey > opts.end)) {
        return cb(db.validationError('The provided starting key is invalid: Invalid ExclusiveStartKey. ' +
          'Please use ExclusiveStartKey with correct Segment. ' +
          'TotalSegments: ' + data.TotalSegments + ' Segment: ' + data.Segment))
      }
      opts.start = exclusiveLexiKey + '\x00'
    }

    valStream = itemDb.createValueStream(opts)
    vals = db.lazy(valStream, cb)

    vals = vals.takeWhile(function(val) {
      if (scannedCount >= data.Limit || size > 1042000) return false

      scannedCount++
      size += db.itemSize(val, true)

      // TODO: Combine this with above
      if (data.ReturnConsumedCapacity == 'TOTAL')
        capacitySize += db.itemSize(val)

      lastItem = val

      return true
    })

    if (data.ScanFilter)
      vals = vals.filter(function(val) { return db.matchesFilter(val, data.ScanFilter) })

    if (data.AttributesToGet) {
      vals = vals.map(function(val) {
        return data.AttributesToGet.reduce(function(item, attr) {
          if (val[attr] != null) item[attr] = val[attr]
          return item
        }, {})
      })
    }

    vals.join(function(items) {
      var result = {Count: items.length, ScannedCount: scannedCount}
      valStream.destroy()
      if (data.Select != 'COUNT') result.Items = items
      if ((data.Limit && data.Limit <= scannedCount) || size > 1042000) {
        result.LastEvaluatedKey = table.KeySchema.reduce(function(key, schemaPiece) {
          key[schemaPiece.AttributeName] = lastItem[schemaPiece.AttributeName]
          return key
        }, {})
      }
      if (data.ReturnConsumedCapacity == 'TOTAL')
        result.ConsumedCapacity = {CapacityUnits: Math.ceil(capacitySize / 1024 / 4) * 0.5, TableName: data.TableName}
      cb(null, result)
    })
  })
}

