var events = require('events'),
    once = require('once'),
    Lazy = require('lazy'),
    db = require('../db')

module.exports = function query(store, data, cb) {
  cb = once(cb)

  store.getTable(data.TableName, function(err, table) {
    if (err) return cb(err)

    var i, keySchema, key, comparisonOperator, hashKey, rangeKey, indexAttrs, type,
        tableHashKey = table.KeySchema[0].AttributeName, tableHashType, tableHashVal,
        opts = {}, vals, itemDb = store.getItemDb(data.TableName),
        size = 0, capacitySize = 0, count = 0, lastItem, em

    if (data.IndexName) {
      for (i = 0; i < (table.LocalSecondaryIndexes || []).length; i++) {
        if (table.LocalSecondaryIndexes[i].IndexName == data.IndexName) {
          keySchema = table.LocalSecondaryIndexes[i].KeySchema
          if (table.LocalSecondaryIndexes[i].Projection.ProjectionType == 'INCLUDE')
            indexAttrs = table.LocalSecondaryIndexes[i].Projection.NonKeyAttributes
          break
        }
      }
      for (i = 0; i < (table.GlobalSecondaryIndexes || []).length; i++) {
        if (table.GlobalSecondaryIndexes[i].IndexName == data.IndexName) {
          if (data.ConsistentRead)
            return cb(db.validationError('Consistent reads are not supported on global secondary indexes'))
          if (data.Select == 'ALL_ATTRIBUTES' && table.GlobalSecondaryIndexes[i].Projection.ProjectionType != 'ALL')
            return cb(db.validationError('One or more parameter values were invalid: ' +
              'Select type ALL_ATTRIBUTES is not supported for global secondary index index4 ' +
              'because its projection type is not ALL'))
          keySchema = table.GlobalSecondaryIndexes[i].KeySchema
          if (table.GlobalSecondaryIndexes[i].Projection.ProjectionType == 'INCLUDE')
            indexAttrs = table.GlobalSecondaryIndexes[i].Projection.NonKeyAttributes
          break
        }
      }
      if (!keySchema) return cb(db.validationError('The table does not have the specified index'))
    } else {
      keySchema = table.KeySchema
    }

    hashKey = keySchema[0].AttributeName
    if (keySchema[1]) rangeKey = keySchema[1].AttributeName

    if (data.KeyConditions[tableHashKey] && data.KeyConditions[tableHashKey].ComparisonOperator == 'EQ') {
      tableHashType = Object.keys(data.KeyConditions[tableHashKey].AttributeValueList[0])[0]
      tableHashVal = data.KeyConditions[tableHashKey].AttributeValueList[0][tableHashType]
      opts.start = db.hashPrefix(tableHashVal, tableHashType)
      opts.end = db.hashPrefix(tableHashVal, tableHashType) + '~~'
    }

    if (data.ExclusiveStartKey) {
      if (table.KeySchema.concat(keySchema).some(function(schemaPiece) { return !data.ExclusiveStartKey[schemaPiece.AttributeName] })) {
        return cb(db.validationError('The provided starting key is invalid'))
      }
      comparisonOperator = data.KeyConditions[hashKey].ComparisonOperator
      if (comparisonOperator == 'EQ') {
        type = Object.keys(data.ExclusiveStartKey[hashKey])[0]
        if (data.ExclusiveStartKey[hashKey][type] != data.KeyConditions[hashKey].AttributeValueList[0][type]) {
          return cb(db.validationError('The provided starting key is outside query boundaries based on provided conditions'))
        }
      }
      if (data.KeyConditions[rangeKey]) {
        comparisonOperator = data.KeyConditions[rangeKey].ComparisonOperator
        type = Object.keys(data.ExclusiveStartKey[rangeKey])[0]
        // TODO: Need more extensive checking than this
        if (comparisonOperator == 'GT' && data.ExclusiveStartKey[rangeKey][type] <= data.KeyConditions[rangeKey].AttributeValueList[0][type]) {
          return cb(db.validationError('The provided starting key does not match the range key predicate'))
        }
      }
      opts.start = db.validateKey(data.ExclusiveStartKey, table) + '\x00'
    }

    for (i = 0; i < keySchema.length; i++) {
      if (!data.KeyConditions[keySchema[i].AttributeName])
        return cb(db.validationError('Query condition missed key schema element ' + keySchema[i].AttributeName))
      if (Object.keys(data.KeyConditions).length <= 1) break
    }

    for (key in data.KeyConditions) {
      comparisonOperator = data.KeyConditions[key].ComparisonOperator
      if (~['NULL', 'NOT_NULL', 'CONTAINS', 'NOT_CONTAINS', 'IN'].indexOf(comparisonOperator))
        return cb(db.validationError('Attempted conditional constraint is not an indexable operation'))
    }

    comparisonOperator = data.KeyConditions[hashKey].ComparisonOperator
    if (~['LE', 'LT', 'GE', 'GT', 'BEGINS_WITH', 'BETWEEN'].indexOf(comparisonOperator) ||
        (keySchema.length == 1 && Object.keys(data.KeyConditions).length > 1))
      return cb(db.validationError('Query key condition not supported'))

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
          var compareFn = db.itemCompare(rangeKey, table), i
          if (data.IndexName)
            items.sort(compareFn)
          if (data.ScanIndexForward === false)
            items.reverse()
          if (data.ExclusiveStartKey) {
            for (i = 0; i < items.length; i++) {
              if (data.ScanIndexForward === false) {
                if (compareFn(data.ExclusiveStartKey, items[i]) > 0)
                  break
              } else {
                if (compareFn(data.ExclusiveStartKey, items[i]) < 0)
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
      if (count >= data.Limit || size > 1042000) return false

      size += db.itemSize(val, true)
      count++

      // TODO: Combine this with above
      if (data.ReturnConsumedCapacity == 'TOTAL')
        capacitySize += db.itemSize(val)

      return true
    })

    if(data.QueryFilter) {
      for(var i =0; i < keySchema.length; i++) {
          if(data.QueryFilter[keySchema[i].AttributeName])
            return cb(db.validationError('QueryFilter can only contain non-primary key attributes: Primary key attribute: '+keySchema[i].AttributeName))
      }

      vals = vals.filter(function(val) {
        return db.matchesFilter(val, data.QueryFilter);
      })
    }

    if (data.AttributesToGet) {
      vals = vals.map(function(val) {
        return data.AttributesToGet.reduce(function(item, attr) {
          if (val[attr] != null) item[attr] = val[attr]
          return item
        }, {})
      })
    }

    vals.join(function(items) {
      var result = {Count: items.length, ScannedCount: items.length}

      // TODO: Check size?
      // TODO: Does this only happen when we're not doing a COUNT?
      if (data.Limit && (items.length > data.Limit || lastItem)) {
        items.splice(data.Limit)
        result.Count = items.length
        result.ScannedCount = items.length
        if (result.Count) {
          result.LastEvaluatedKey = table.KeySchema.concat(keySchema).reduce(function(key, schemaPiece) {
            key[schemaPiece.AttributeName] = items[items.length - 1][schemaPiece.AttributeName]
            return key
          }, {})
        }
      }
      if (data.Select != 'COUNT') result.Items = items
      if (data.ReturnConsumedCapacity == 'TOTAL')
        result.ConsumedCapacity = {CapacityUnits: Math.ceil(capacitySize / 1024 / 4) * 0.5, TableName: data.TableName}
      if (result.ConsumedCapacity && indexAttrs && data.Select == 'ALL_ATTRIBUTES')
        result.ConsumedCapacity.CapacityUnits *= 4
      cb(null, result)
    })
  })
}

