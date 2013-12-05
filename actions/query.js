var once = require('once'),
    db = require('../db')

module.exports = function query(store, data, cb) {
  cb = once(cb)

  store.getTable(data.TableName, function(err, table) {
    if (err) return cb(err)

    var i, keySchema, key, comparisonOperator, hashKey, rangeKey, indexAttrs, type,
        opts = {}, valStream, vals, itemDb = store.getItemDb(data.TableName),
        size = 0, capacitySize = 0, count = 0, lastItem

    hashKey = table.KeySchema[0].AttributeName
    if (table.KeySchema[1]) rangeKey = table.KeySchema[1].AttributeName

    if (data.ExclusiveStartKey) {
      if (table.KeySchema.some(function(schemaPiece) { return !data.ExclusiveStartKey[schemaPiece.AttributeName] })) {
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

    if (data.IndexName) {
      for (i = 0; i < (table.LocalSecondaryIndexes || []).length; i++) {
        if (table.LocalSecondaryIndexes[i].IndexName == data.IndexName) {
          keySchema = table.LocalSecondaryIndexes[i].KeySchema
          if (table.LocalSecondaryIndexes[i].Projection.ProjectionType == 'INCLUDE')
            indexAttrs = table.LocalSecondaryIndexes[i].Projection.NonKeyAttributes
          break
        }
      }
      if (!keySchema) return cb(db.validationError('The table does not have the specified index'))
    } else {
      keySchema = table.KeySchema
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

    valStream = itemDb.createValueStream(opts)
    vals = db.lazy(valStream, cb)

    vals = vals.filter(function(val) {
      if (!db.matchesFilter(val, data.KeyConditions)) {
        if (lastItem) lastItem = null
        return false
      }

      lastItem = val
      return true
    })

    vals = vals.takeWhile(function(val) {
      // Limits don't currently work for traversing index in reverse
      if ((data.ScanIndexForward !== false && count >= data.Limit) || size > 1042000) return false

      size += db.itemSize(val, true)
      count++

      // TODO: Combine this with above
      if (data.ReturnConsumedCapacity == 'TOTAL')
        capacitySize += db.itemSize(val)

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
      var result = {Count: items.length}
      valStream.destroy()
      if (data.Select != 'COUNT') {
        if (data.IndexName) {
          items.sort(function(item1, item2) {
            var type1 = Object.keys(item1[keySchema[1].AttributeName] || {})[0],
                val1 = type1 ? item1[keySchema[1].AttributeName][type1] : '',
                type2 = Object.keys(item2[keySchema[1].AttributeName] || {})[0],
                val2 = type2 ? item2[keySchema[1].AttributeName][type2] : ''
            return db.toLexiStr(val1, type1).localeCompare(db.toLexiStr(val2, type2))
          })
        }
        if (data.ScanIndexForward === false) items.reverse()
      }
      // TODO: Check size?
      // TODO: Does this only happen when we're not doing a COUNT?
      if (data.Limit && (items.length > data.Limit || lastItem)) {
        items.splice(data.Limit)
        result.Count = items.length
        if (result.Count) {
          result.LastEvaluatedKey = table.KeySchema.reduce(function(key, schemaPiece) {
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

