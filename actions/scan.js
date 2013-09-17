var once = require('once'),
    db = require('../db'),
    itemDb = db.itemDb

module.exports = function scan(data, cb) {
  cb = once(cb)

  db.getTable(data.TableName, function(err, table) {
    if (err) return cb(err)

    var opts, vals, scannedCount = 0

    if (data.ExclusiveStartKey)
      opts = {start: data.ExclusiveStartKey + '\x00'}

    vals = db.lazy(itemDb.createValueStream(opts), cb)

    if (data.Limit) vals = vals.take(data.Limit)

    vals = vals.filter(function(val) {
      scannedCount++
      if (data.ScanFilter) {
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
              if (compType != attrType || attrVal > compVal) return false
              break
            case 'LT':
              if (compType != attrType || attrVal >= compVal) return false
              break
            case 'GE':
              if (compType != attrType || attrVal < compVal) return false
              break
            case 'GT':
              if (compType != attrType || attrVal <= compVal) return false
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
              if (!attrVal || compType != attrType) return false
              if (attrVal < compVal || attrVal > compVals[1][compType]) return false
          }
        }
      }
      return true
    })

    vals.join(function(items) {
      var result = {Items: items, Count: items.length, ScannedCount: scannedCount}
      if (data.Limit) result.LastEvaluatedKey = items[items.length - 1]
      cb(null, result)
    })
  })
}

