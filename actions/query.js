var events = require('events'),
    once = require('once'),
    Lazy = require('lazy'),
    db = require('../db')

module.exports = function query(store, data, cb) {
  cb = once(cb)

  store.getTable(data.TableName, function(err, table) {
    if (err) return cb(err)

    var i, keySchema, comparisonOperator, hashKey, rangeKey, projectionType, indexAttrs, type, isLocal,
        tableHashKey = table.KeySchema[0].AttributeName, tableHashType, tableHashVal,
        opts = {}, vals, itemDb = store.getItemDb(data.TableName),
        size = 0, capacitySize = 0, count = 0, scannedCount = 0, lastItem, em, limited = false

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
        if (!keySchema) return cb(db.validationError('The table does not have the specified index: ' + data.IndexName))
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
        return cb(db.validationError('The provided starting key is invalid'))
      }
    }

    hashKey = keySchema[0].AttributeName
    if (keySchema[1]) rangeKey = keySchema[1].AttributeName

    if (keySchema.length == 1 && Object.keys(data.KeyConditions).length > 1) {
      return cb(db.validationError('Query key condition not supported'))
    }

    err = db.traverseKey(table, keySchema, function(attr, type, isHash) {
      if (data.ExclusiveStartKey) {
        if (!data.ExclusiveStartKey[attr]) {
          return db.validationError('The provided starting key is invalid')
        }
        var err = db.validateKeyPiece(data.ExclusiveStartKey, attr, type, isHash)
        if (err) return err
      }

      if (!data.KeyConditions[attr]) {
        if (isHash || Object.keys(data.KeyConditions).length > 1) {
          return db.validationError('Query condition missed key schema element: ' + attr)
        }
        return
      }

      comparisonOperator = data.KeyConditions[attr].ComparisonOperator

      if (~['NULL', 'NOT_NULL', 'NE', 'CONTAINS', 'NOT_CONTAINS', 'IN'].indexOf(comparisonOperator)) {
        return cb(db.validationError('Attempted conditional constraint is not an indexable operation'))
      }

      if (data.KeyConditions[attr].AttributeValueList.some(function(attrVal) { return attrVal[type] == null })) {
        return cb(db.validationError('One or more parameter values were invalid: Condition parameter type does not match schema type'))
      }

      if (isHash && ~['LE', 'LT', 'GE', 'GT', 'BEGINS_WITH', 'BETWEEN'].indexOf(comparisonOperator)) {
        return cb(db.validationError('Query key condition not supported'))
      }
    })
    if (err) return cb(err)

    if (data.KeyConditions[tableHashKey] && data.KeyConditions[tableHashKey].ComparisonOperator == 'EQ') {
      tableHashType = Object.keys(data.KeyConditions[tableHashKey].AttributeValueList[0])[0]
      tableHashVal = data.KeyConditions[tableHashKey].AttributeValueList[0][tableHashType]
      opts.start = db.hashPrefix(tableHashVal, tableHashType)
      opts.end = db.hashPrefix(tableHashVal, tableHashType) + '~~'
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
      opts.start = startKey + '\x00'
    }

    if (data.ExclusiveStartKey) {
      if (Object.keys(data.KeyConditions).length == 1) {
        comparisonOperator = data.KeyConditions[hashKey].ComparisonOperator
        if (comparisonOperator == 'EQ') {
          type = Object.keys(data.ExclusiveStartKey[hashKey])[0]
          if (data.ExclusiveStartKey[hashKey][type] != data.KeyConditions[hashKey].AttributeValueList[0][type]) {
            return cb(db.validationError('The provided starting key is outside query boundaries based on provided conditions'))
          }
        }
      } else {
        comparisonOperator = data.KeyConditions[rangeKey].ComparisonOperator
        type = Object.keys(data.ExclusiveStartKey[rangeKey])[0]
        // TODO: Need more extensive checking than this
        if (comparisonOperator == 'GT' && data.ExclusiveStartKey[rangeKey][type] <= data.KeyConditions[rangeKey].AttributeValueList[0][type]) {
          return cb(db.validationError('The provided starting key does not match the range key predicate'))
        }
        comparisonOperator = data.KeyConditions[hashKey].ComparisonOperator
        if (comparisonOperator == 'EQ') {
          type = Object.keys(data.ExclusiveStartKey[hashKey])[0]
          if (data.ExclusiveStartKey[hashKey][type] != data.KeyConditions[hashKey].AttributeValueList[0][type]) {
            return cb(db.validationError('The query can return at most one row and cannot be restarted'))
          }
        }
      }
    }

    if (data._projectionPaths) {
      err = db.validateKeyPaths(data._projectionPaths, table)
      if (err) return cb(err)
    }

    if (data._filterExpression) {
      var paths = data._filterExpression.paths
      for (i = 0; i < keySchema.length; i++) {
        for (var j = 0; j < paths.length; j++) {
          if (paths[j][0] == keySchema[i].AttributeName) {
            return cb(db.validationError('Filter Expression can only contain non-primary key attributes: ' +
              'Primary key attribute: ' + keySchema[i].AttributeName))
          }
        }
      }
      err = db.traverseIndexes(table, function(attr) {
        var paths = data._filterExpression.nestedPaths
        if (paths[attr]) {
          return db.validationError('Key attributes must be scalars; ' +
            'list random access \'[]\' and map lookup \'.\' are not allowed: IndexKey: ' + attr)
        }
      })
      if (err) return cb(err)
    }

    if (data.QueryFilter) {
      for (i = 0; i < keySchema.length; i++) {
        if (data.QueryFilter[keySchema[i].AttributeName])
          return cb(db.validationError('QueryFilter can only contain non-primary key attributes: ' +
            'Primary key attribute: ' + keySchema[i].AttributeName))
      }
    }

    if (data.IndexName && data.Select == 'ALL_ATTRIBUTES' && !isLocal && projectionType != 'ALL') {
      return cb(db.validationError('One or more parameter values were invalid: ' +
        'Select type ALL_ATTRIBUTES is not supported for global secondary index ' +
        data.IndexName + ' because its projection type is not ALL'))
    }

    if (indexAttrs) {
      keySchema.map(function(schemaPiece) { return schemaPiece.AttributeName }).forEach(function(attr) {
        if (!data.KeyConditions[attr])
          data.KeyConditions[attr] = {ComparisonOperator: 'NOT_NULL'}
      })
      if (data.Select != 'ALL_ATTRIBUTES') {
        data.AttributesToGet = indexAttrs
          .concat(keySchema.map(function(schemaPiece) { return schemaPiece.AttributeName }))
          .concat(table.KeySchema.map(function(schemaPiece) { return schemaPiece.AttributeName }))
      }
    }

    // TODO: We currently don't deal nicely with indexes or reverse queries
    if (data.ScanIndexForward === false || data.IndexName) {
      em = new events.EventEmitter
      vals = new Lazy(em)

      db.lazy(itemDb.createValueStream(), cb)
        .filter(function(val) { return db.matchesFilter(val, data.KeyConditions) })
        .join(function(items) {
          var compareFn = db.itemCompare(rangeKey, table)
          if (data.IndexName)
            items.sort(compareFn)
          if (data.ScanIndexForward === false)
            items.reverse()
          if (data.ExclusiveStartKey) {
            for (i = 0; i < items.length; i++) {
              if (data.ScanIndexForward === false) {
                if (compareFn(data.ExclusiveStartKey, items[i]) > 0)
                  break
              } else if (compareFn(data.ExclusiveStartKey, items[i]) < 0) {
                break
              }
            }
            items = items.slice(i)
          }

          items.forEach(function(item) { em.emit('data', item) })
          em.emit('end')
        })
    } else {
      vals = db.lazy(itemDb.createValueStream(opts), cb)
    }

    vals = vals.filter(function(val) {
      if (!db.matchesFilter(val, data.KeyConditions)) {
        if (lastItem) lastItem = null
        return false
      }

      lastItem = val
      return true
    })

    vals = vals.takeWhile(function(val) {
      if (count >= data.Limit || size > 1041375) {
        limited = true
        return false
      }

      size += db.itemSize(val, true)
      count++

      // TODO: Combine this with above
      if (~['TOTAL', 'INDEXES'].indexOf(data.ReturnConsumedCapacity))
        capacitySize += db.itemSize(val)

      return true
    })

    if (data._filterExpression) {
      vals = vals.filter(function(val) {
        scannedCount++
        return db.matchesExprFilter(val, data._filterExpression.expression)
      })
    } else if (data.QueryFilter) {
      vals = vals.filter(function(val) {
        scannedCount++
        return db.matchesFilter(val, data.QueryFilter, data.ConditionalOperator)
      })
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
      var result = {Count: items.length, ScannedCount: scannedCount || items.length},
        capacityUnits, tableUnits, indexUnits, indexAttr
      if (limited || (data.Limit && lastItem) || size > 1041575) {
        if (data.Limit) items.splice(data.Limit)
        result.Count = items.length
        result.ScannedCount = scannedCount || items.length
        if (result.Count) {
          result.LastEvaluatedKey = table.KeySchema.concat(keySchema).reduce(function(key, schemaPiece) {
            key[schemaPiece.AttributeName] = items[items.length - 1][schemaPiece.AttributeName]
            return key
          }, {})
        }
      }
      if (data.Select != 'COUNT') result.Items = items
      if (~['TOTAL', 'INDEXES'].indexOf(data.ReturnConsumedCapacity)) {
        capacityUnits = Math.ceil(capacitySize / 1024 / 4) * (data.ConsistentRead ? 1 : 0.5)
        tableUnits = data.IndexName ? 0 : capacityUnits
        indexUnits = data.IndexName ? capacityUnits : 0
        if (indexAttrs && data.Select == 'ALL_ATTRIBUTES')
          tableUnits = indexUnits * 3
        result.ConsumedCapacity = {
          CapacityUnits: tableUnits + indexUnits,
          TableName: data.TableName,
        }
        if (data.ReturnConsumedCapacity == 'INDEXES') {
          result.ConsumedCapacity.Table = {CapacityUnits: tableUnits}
          indexAttr = isLocal ? 'LocalSecondaryIndexes' : 'GlobalSecondaryIndexes'
          result.ConsumedCapacity[indexAttr] = {}
          result.ConsumedCapacity[indexAttr][data.IndexName] = {CapacityUnits: indexUnits}
        }
      }
      cb(null, result)
    })
  })
}
