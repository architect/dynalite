var once = require('once'),
    db = require('../db')

module.exports = function scan(store, data, cb) {
  cb = once(cb)

  store.getTable(data.TableName, function(err, table) {
    if (err) return cb(err)

    var i, keySchema, projectionType, indexAttrs, isLocal, opts = {},
        vals, scannedCount = 0, itemDb = store.getItemDb(data.TableName),
        size = 0, capacitySize = 0, filterFn, lastItem

    if (data.TotalSegments > 1) {
      if (data.Segment > 0)
        opts.start = ('00' + Math.ceil(4096 * data.Segment / data.TotalSegments).toString(16)).slice(-3)
      opts.end = ('00' + (Math.ceil(4096 * (data.Segment + 1) / data.TotalSegments) - 1).toString(16)).slice(-3) + '\xff'
    }

    if (data.IndexName) {
      for (i = 0; i < (table.LocalSecondaryIndexes || []).length; i++) {
        if (table.LocalSecondaryIndexes[i].IndexName != data.IndexName) continue
        keySchema = table.LocalSecondaryIndexes[i].KeySchema
        projectionType = table.LocalSecondaryIndexes[i].Projection.ProjectionType
        if (projectionType == 'INCLUDE')
          indexAttrs = table.LocalSecondaryIndexes[i].Projection.NonKeyAttributes
        isLocal = true
        break
      }
      if (!keySchema) {
        for (i = 0; i < (table.GlobalSecondaryIndexes || []).length; i++) {
          if (table.GlobalSecondaryIndexes[i].IndexName != data.IndexName) continue
          if (data.ConsistentRead)
            return cb(db.validationError('Consistent reads are not supported on global secondary indexes'))
          keySchema = table.GlobalSecondaryIndexes[i].KeySchema
          projectionType = table.GlobalSecondaryIndexes[i].Projection.ProjectionType
          if (projectionType == 'INCLUDE')
            indexAttrs = table.GlobalSecondaryIndexes[i].Projection.NonKeyAttributes
          isLocal = false
          break
        }
        if (!keySchema) {
          if (data.ExclusiveStartKey) {
            return cb(db.validationError('The provided starting key is invalid'))
          }
          return cb(db.validationError('The table does not have the specified index: ' + data.IndexName))
        }
      }
    } else {
      keySchema = table.KeySchema
    }

    if (data.ExclusiveStartKey) {
      var tableKeyNames = table.KeySchema.concat(keySchema).reduce(function(obj, attr) {
        obj[attr.AttributeName] = attr
        return obj
      }, {})
      if (Object.keys(data.ExclusiveStartKey).length != Object.keys(tableKeyNames).length) {
        return data.IndexName ? cb(db.validationError('The provided starting key is invalid')) :
          cb(db.validationError('The provided starting key is invalid: The provided key element does not match the schema'))
      }
    }

    if (data.IndexName) {
      if (data.ExclusiveStartKey) {
        err = db.traverseKey(table, keySchema, function(attr, type, isHash) {
          if (!data.ExclusiveStartKey[attr]) {
            return db.validationError('The provided starting key is invalid')
          }
          var err = db.validateKeyPiece(data.ExclusiveStartKey, attr, type, isHash)
          if (err) return err
        })
        if (err) return cb(err)
      }
      if (data.Select == 'ALL_ATTRIBUTES' && !isLocal && projectionType != 'ALL') {
        return cb(db.validationError('One or more parameter values were invalid: ' +
          'Select type ALL_ATTRIBUTES is not supported for global secondary index ' +
          data.IndexName + ' because its projection type is not ALL'))
      }
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

    if (data._filterExpression) {
      err = db.traverseKey(table, function(attr) {
        var paths = data._filterExpression.nestedPaths
        if (paths[attr]) {
          return db.validationError('Key attributes must be scalars; ' +
            'list random access \'[]\' and map lookup \'.\' are not allowed: Key: ' + attr)
        }
      })
      if (err) return cb(err)
      err = db.traverseIndexes(table, function(attr) {
        var paths = data._filterExpression.nestedPaths
        if (paths[attr]) {
          return db.validationError('Key attributes must be scalars; ' +
            'list random access \'[]\' and map lookup \'.\' are not allowed: IndexKey: ' + attr)
        }
      })
      if (err) return cb(err)
    }

    if (data._projectionPaths) {
      err = db.validateKeyPaths(data._projectionPaths, table)
      if (err) return cb(err)
    }

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

    if (data._filterExpression) {
      filterFn = function(val) { return db.matchesExprFilter(val, data._filterExpression.expression) }
    } else if (data.ScanFilter) {
      filterFn = function(val) { return db.matchesFilter(val, data.ScanFilter, data.ConditionalOperator) }
    }
    if (filterFn) {
      vals = vals.filter(filterFn)
    }

    if (indexAttrs && data.Select != 'ALL_ATTRIBUTES') {
      data.AttributesToGet = indexAttrs
        .concat(keySchema.map(function(schemaPiece) { return schemaPiece.AttributeName }))
        .concat(table.KeySchema.map(function(schemaPiece) { return schemaPiece.AttributeName }))
    }

    if (data._projectionPaths) {
      vals = vals.map(db.mapPaths.bind(db, data._projectionPaths))
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
