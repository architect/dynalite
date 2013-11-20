var once = require('once'),
    Big = require('big.js'),
    db = require('../db')

module.exports = function scan(store, data, cb) {
  cb = once(cb)

  store.getTable(data.TableName, function(err, table) {
    if (err) return cb(err)

    var opts = {}, vals, scannedCount = 0, itemDb = store.getItemDb(data.TableName),
        size = 0, capacitySize = 0, exclusiveLexiKey, lastItem

    if (data.TotalSegments > 1) {
      if (data.Segment > 0)
        opts.start = ('00' + Math.ceil(4096 * data.Segment / data.TotalSegments).toString(16)).slice(-3)
      opts.end = ('00' + (Math.ceil(4096 * (data.Segment + 1) / data.TotalSegments) - 1).toString(16)).slice(-3) + '\xff'
    }

    if (data.ExclusiveStartKey) {
      exclusiveLexiKey = db.validateKey(data.ExclusiveStartKey, table)
      if (data.TotalSegments > 1 && (exclusiveLexiKey < opts.start || exclusiveLexiKey > opts.end)) {
        return cb(db.validationError('The provided starting key is invalid: Invalid ExclusiveStartKey. ' +
          'Please use ExclusiveStartKey with correct Segment. ' +
          'TotalSegments: ' + data.TotalSegments + ' Segment: ' + data.Segment))
      }
      opts.start = exclusiveLexiKey + '\x00'
    }

    vals = db.lazy(itemDb.createValueStream(opts), cb)

    if (data.Limit) vals = vals.take(data.Limit)

    vals = vals.filter(function(val) {
      if (size > 1042000) return false
      scannedCount++
      size += db.itemSize(val, true)

      // TODO: Combine this with above
      if (data.ReturnConsumedCapacity == 'TOTAL')
        capacitySize += db.itemSize(val)

      lastItem = val

      if (!data.ScanFilter) return true

      for (var attr in data.ScanFilter) {
        var comp = data.ScanFilter[attr].ComparisonOperator,
            compVals = data.ScanFilter[attr].AttributeValueList,
            compType = compVals ? Object.keys(compVals[0])[0] : null,
            compVal = compVals ? compVals[0][compType] : null,
            attrType = val[attr] ? Object.keys(val[attr])[0] : null,
            attrVal = val[attr] ? val[attr][attrType] : null
        switch (comp) {
          case 'EQ':
            if (compType != attrType || attrVal != compVal) return false
            break
          case 'NE':
            if (compType == attrType && attrVal == compVal) return false
            break
          case 'LE':
            if (compType != attrType ||
              (attrType == 'N' && !Big(attrVal).lte(compVal)) ||
              (attrType != 'N' && attrVal > compVal)) return false
            break
          case 'LT':
            if (compType != attrType ||
              (attrType == 'N' && !Big(attrVal).lt(compVal)) ||
              (attrType != 'N' && attrVal >= compVal)) return false
            break
          case 'GE':
            if (compType != attrType ||
              (attrType == 'N' && !Big(attrVal).gte(compVal)) ||
              (attrType != 'N' && attrVal < compVal)) return false
            break
          case 'GT':
            if (compType != attrType ||
              (attrType == 'N' && !Big(attrVal).gt(compVal)) ||
              (attrType != 'N' && attrVal <= compVal)) return false
            break
          case 'NOT_NULL':
            if (!attrVal) return false
            break
          case 'NULL':
            if (attrVal) return false
            break
          case 'CONTAINS':
            if (compType == 'S') {
              if (attrType != 'S' && attrType != 'SS') return false
              if (!~attrVal.indexOf(compVal)) return false
            }
            if (compType == 'N') {
              if (attrType != 'NS') return false
              if (!~attrVal.indexOf(compVal)) return false
            }
            if (compType == 'B') {
              if (attrType != 'B' && attrType != 'BS') return false
              if (attrType == 'B') {
                attrVal = new Buffer(attrVal, 'base64').toString()
                compVal = new Buffer(compVal, 'base64').toString()
              }
              if (!~attrVal.indexOf(compVal)) return false
            }
            break
          case 'NOT_CONTAINS':
            if (compType == 'S' && (attrType == 'S' || attrType == 'SS') &&
                ~attrVal.indexOf(compVal)) return false
            if (compType == 'N' && attrType == 'NS' &&
                ~attrVal.indexOf(compVal)) return false
            if (compType == 'B') {
              if (attrType == 'B') {
                attrVal = new Buffer(attrVal, 'base64').toString()
                compVal = new Buffer(compVal, 'base64').toString()
              }
              if ((attrType == 'B' || attrType == 'BS') &&
                  ~attrVal.indexOf(compVal)) return false
            }
            break
          case 'BEGINS_WITH':
            if (compType != attrType) return false
            if (compType == 'B') {
              attrVal = new Buffer(attrVal, 'base64').toString()
              compVal = new Buffer(compVal, 'base64').toString()
            }
            if (attrVal.indexOf(compVal) !== 0) return false
            break
          case 'IN':
            if (!attrVal) return false
            if (!compVals.some(function(compVal) {
              compType = Object.keys(compVal)[0]
              compVal = compVal[compType]
              return compType == attrType && attrVal == compVal
            })) return false
            break
          case 'BETWEEN':
            if (!attrVal || compType != attrType ||
              (attrType == 'N' && (!Big(attrVal).gte(compVal) || !Big(attrVal).lte(compVals[1].N))) ||
              (attrType != 'N' && (attrVal < compVal || attrVal > compVals[1][compType]))) return false
        }
      }
      return true
    })

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

