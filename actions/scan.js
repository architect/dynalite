var once = require('once'),
    db = require('../db')

module.exports = function scan(store, data, cb) {
  cb = once(cb)

  store.getTable(data.TableName, function(err, table) {
    if (err) return cb(err)

    var keySchema = table.KeySchema, startKeyNames = keySchema.map(function(key) { return key.AttributeName }),
      projectionType, indexAttrs, isLocal

    if (data.IndexName) {
      var index = db.traverseIndexes(table, function(attr, type, index, isGlobal) {
        if (index.IndexName == data.IndexName) {
          isLocal = !isGlobal
          return index
        }
      })
      if (index == null) {
        if (data.ExclusiveStartKey) {
          return cb(db.validationError('The provided starting key is invalid'))
        }
        return cb(db.validationError('The table does not have the specified index: ' + data.IndexName))
      }
      keySchema = index.KeySchema
      projectionType = index.Projection.ProjectionType
      if (projectionType == 'INCLUDE')
        indexAttrs = index.Projection.NonKeyAttributes
      keySchema.forEach(function(key) { if (!~startKeyNames.indexOf(key.AttributeName)) startKeyNames.push(key.AttributeName) })
    }

    if (data.ExclusiveStartKey && Object.keys(data.ExclusiveStartKey).length != startKeyNames.length) {
      return data.IndexName ? cb(db.validationError('The provided starting key is invalid')) :
        cb(db.validationError('The provided starting key is invalid: The provided key element does not match the schema'))
    }

    if (data.IndexName) {
      if (data.ExclusiveStartKey) {
        err = db.traverseKey(table, keySchema, function(attr, type, isHash) {
          if (!data.ExclusiveStartKey[attr]) {
            return db.validationError('The provided starting key is invalid')
          }
          return db.validateKeyPiece(data.ExclusiveStartKey, attr, type, isHash)
        })
        if (err) return cb(err)
      }
      if (data.Select == 'ALL_ATTRIBUTES' && !isLocal && projectionType != 'ALL') {
        return cb(db.validationError('One or more parameter values were invalid: ' +
          'Select type ALL_ATTRIBUTES is not supported for global secondary index ' +
          data.IndexName + ' because its projection type is not ALL'))
      }
      if (indexAttrs && data.Select != 'ALL_ATTRIBUTES') {
        data.AttributesToGet = indexAttrs.concat(startKeyNames)
      }
    }

    var opts = {}

    if (data.TotalSegments > 1) {
      if (data.Segment > 0)
        opts.start = ('00' + Math.ceil(4096 * data.Segment / data.TotalSegments).toString(16)).slice(-3)
      opts.end = ('00' + (Math.ceil(4096 * (data.Segment + 1) / data.TotalSegments) - 1).toString(16)).slice(-3) + '~'
    }

    if (data.ExclusiveStartKey) {
      var tableStartKey = table.KeySchema.reduce(function(obj, attr) {
        obj[attr.AttributeName] = data.ExclusiveStartKey[attr.AttributeName]
        return obj
      }, {})

      var startKey = db.validateKey(tableStartKey, table)
      if (startKey instanceof Error) {
        return cb(db.validationError('The provided starting key is invalid: ' + startKey.message))
      }
      if (data.TotalSegments > 1 && (startKey < opts.start || startKey > opts.end)) {
        return cb(db.validationError('The provided starting key is invalid: Invalid ExclusiveStartKey. ' +
          'Please use ExclusiveStartKey with correct Segment. ' +
          'TotalSegments: ' + data.TotalSegments + ' Segment: ' + data.Segment))
      }
      opts.start = startKey + '\x00'
    }

    if (data._projection) {
      err = db.validateKeyPaths(data._projection.nestedPaths, table)
      if (err) return cb(err)
    }

    if (data._filter && (err = db.validateKeyPaths(data._filter.nestedPaths, table)) != null) {
      return cb(err)
    }

    var itemDb = store.getItemDb(data.TableName), vals, scannedCount = 0, size = 0, capacitySize = 0, lastItem

    vals = db.lazy(itemDb.createValueStream(opts), cb)

    vals = vals.takeWhile(function(val) {
      if (scannedCount >= data.Limit || size > 1042000) return false

      scannedCount++
      size += db.itemSize(val, true)

      // TODO: Combine this with above
      if (~['TOTAL', 'INDEXES'].indexOf(data.ReturnConsumedCapacity))
        capacitySize += db.itemSize(val)

      lastItem = val

      return true
    })

    if (data._filter) {
      vals = vals.filter(function(val) { return db.matchesExprFilter(val, data._filter.expression) })
    } else if (data.ScanFilter) {
      vals = vals.filter(function(val) { return db.matchesFilter(val, data.ScanFilter, data.ConditionalOperator) })
    }

    if (data._projection) {
      vals = vals.map(db.mapPaths.bind(db, data._projection.paths))
    } else if (data.AttributesToGet) {
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
      if (~['TOTAL', 'INDEXES'].indexOf(data.ReturnConsumedCapacity))
        result.ConsumedCapacity = {
          CapacityUnits: Math.ceil(capacitySize / 1024 / 4) * 0.5,
          TableName: data.TableName,
          Table: data.ReturnConsumedCapacity == 'INDEXES' ?
            {CapacityUnits: Math.ceil(capacitySize / 1024 / 4) * 0.5} : undefined,
        }
      cb(null, result)
    })
  })
}
