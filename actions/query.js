var events = require('events'),
    once = require('once'),
    Lazy = require('lazy'),
    db = require('../db')

module.exports = function query(store, data, cb) {
  cb = once(cb)

  store.getTable(data.TableName, function(err, table) {
    if (err) return cb(err)

    var keySchema = table.KeySchema, startKeyNames = keySchema.map(function(key) { return key.AttributeName }),
      hashKey = startKeyNames[0], rangeKey = startKeyNames[1], projectionType, indexAttrs, isLocal

    if (data.IndexName) {
      var index = db.traverseIndexes(table, function(attr, type, index, isGlobal) {
        if (index.IndexName == data.IndexName) {
          isLocal = !isGlobal
          return index
        }
      })
      if (index == null) {
        return cb(db.validationError('The table does not have the specified index: ' + data.IndexName))
      }
      if (!isLocal && data.ConsistentRead) {
        return cb(db.validationError('Consistent reads are not supported on global secondary indexes'))
      }
      keySchema = index.KeySchema
      projectionType = index.Projection.ProjectionType
      if (projectionType == 'INCLUDE')
        indexAttrs = index.Projection.NonKeyAttributes
      keySchema.forEach(function(key) { if (!~startKeyNames.indexOf(key.AttributeName)) startKeyNames.push(key.AttributeName) })
      hashKey = keySchema[0].AttributeName
      rangeKey = keySchema[1] && keySchema[1].AttributeName
    }

    if (data.ExclusiveStartKey && Object.keys(data.ExclusiveStartKey).length != startKeyNames.length) {
      return cb(db.validationError('The provided starting key is invalid'))
    }

    err = db.traverseKey(table, keySchema, function(attr, type, isHash) {
      if (data.ExclusiveStartKey) {
        if (!data.ExclusiveStartKey[attr]) {
          return db.validationError('The provided starting key is invalid')
        }
        err = db.validateKeyPiece(data.ExclusiveStartKey, attr, type, isHash)
        if (err) return err
      }

      if (isHash && keySchema.length == 1 && Object.keys(data.KeyConditions).length > 1) {
        return db.validationError('Query key condition not supported')
      }

      if (!data.KeyConditions[attr]) {
        if (isHash || Object.keys(data.KeyConditions).length > 1) {
          return db.validationError('Query condition missed key schema element: ' + attr)
        }
        return
      }

      var comparisonOperator = data.KeyConditions[attr].ComparisonOperator

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

    var opts = {}, tableHashKey = startKeyNames[0]

    if (data.KeyConditions[tableHashKey] && data.KeyConditions[tableHashKey].ComparisonOperator == 'EQ') {
      var tableHashType = Object.keys(data.KeyConditions[tableHashKey].AttributeValueList[0])[0]
      var tableHashVal = data.KeyConditions[tableHashKey].AttributeValueList[0][tableHashType]
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

      if (Object.keys(data.KeyConditions).length == 1) {
        var comparisonOperator = data.KeyConditions[hashKey].ComparisonOperator
        if (comparisonOperator == 'EQ') {
          var type = Object.keys(data.ExclusiveStartKey[hashKey])[0]
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

    if (data._projection) {
      err = db.validateKeyPaths(data._projection.nestedPaths, table)
      if (err) return cb(err)
    }

    if (data.QueryFilter || data._filter) {
      var pathHeads = data.QueryFilter ? data.QueryFilter : data._filter.pathHeads
      var propertyName = data.QueryFilter ? 'QueryFilter' : 'Filter Expression'
      err = db.traverseKey(table, keySchema, function(attr) {
        if (pathHeads[attr]) {
          return db.validationError(propertyName + ' can only contain non-primary key attributes: ' +
            'Primary key attribute: ' + attr)
        }
      })
      if (err) return cb(err)
    }

    if (data.IndexName) {
      if (data.Select == 'ALL_ATTRIBUTES' && !isLocal && projectionType != 'ALL') {
        return cb(db.validationError('One or more parameter values were invalid: ' +
          'Select type ALL_ATTRIBUTES is not supported for global secondary index ' +
          data.IndexName + ' because its projection type is not ALL'))
      }
      if (indexAttrs && data.Select != 'ALL_ATTRIBUTES') {
        data.AttributesToGet = indexAttrs.concat(startKeyNames)
      }
    }

    if (data._filter && (err = db.validateKeyPaths(data._filter.nestedPaths, table)) != null) {
      return cb(err)
    }

    startKeyNames.forEach(function(attr) {
      if (!data.KeyConditions[attr])
        data.KeyConditions[attr] = {ComparisonOperator: 'NOT_NULL'}
    })

    var itemDb = store.getItemDb(data.TableName), vals, lastItem

    // TODO: We currently don't deal nicely with indexes or reverse queries
    if (data.ScanIndexForward === false || data.IndexName) {
      var em = new events.EventEmitter
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
            for (var i = 0; i < items.length; i++) {
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

    var size = 0, capacitySize = 0, count = 0, scannedCount = 0, limited = false

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

    if (data._filter) {
      vals = vals.filter(function(val) {
        scannedCount++
        return db.matchesExprFilter(val, data._filter.expression)
      })
    } else if (data.QueryFilter) {
      vals = vals.filter(function(val) {
        scannedCount++
        return db.matchesFilter(val, data.QueryFilter, data.ConditionalOperator)
      })
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
        if (indexAttrs && data.Select == 'ALL_ATTRIBUTES') {
          tableUnits = capacityUnits
          indexUnits = Math.floor(capacityUnits / result.ScannedCount)
        } else {
          tableUnits = data.IndexName ? 0 : capacityUnits
          indexUnits = data.IndexName ? capacityUnits : 0
        }
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
