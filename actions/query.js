var once = require('once'),
    db = require('../db'),
    scan = require('./scan')

module.exports = function query(store, data, cb) {
  cb = once(cb)

  store.getTable(data.TableName, function(err, table) {
    if (err) return cb(err)

    var i, keySchema, key, comparisonOperator, firstKey, indexAttrs,
        opts = {}, vals, itemDb = store.getItemDb(data.TableName),
        size = 0, capacitySize = 0, lastItem

    if (data.ExclusiveStartKey && !Object.keys(data.ExclusiveStartKey).length) {
      return cb(db.validationError('The provided starting key is invalid'))
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

    firstKey = Object.keys(data.KeyConditions)[0]
    comparisonOperator = data.KeyConditions[firstKey].ComparisonOperator
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

    if (data.ExclusiveStartKey) {
      opts.start = db.validateKey(data.ExclusiveStartKey, table) + '\x00'
    }

    vals = db.lazy(itemDb.createValueStream(opts), cb)

    vals = vals.filter(function(val) {

      if (!db.matchesFilter(val, data.KeyConditions)) {
        if (lastItem) lastItem = null
        return false
      }

      if (size > 1042000) return false
      size += db.itemSize(val, true)

      // TODO: Combine this with above
      if (data.ReturnConsumedCapacity == 'TOTAL')
        capacitySize += db.itemSize(val)

      lastItem = val
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
        result.Items = items
      } else if (data.Limit && result.Count > data.Limit) {
        result.Count = data.Limit
      }
      if (data.ReturnConsumedCapacity == 'TOTAL')
        result.ConsumedCapacity = {CapacityUnits: Math.ceil(capacitySize / 1024 / 4) * 0.5, TableName: data.TableName}
      if (result.ConsumedCapacity && indexAttrs && data.Select == 'ALL_ATTRIBUTES')
        result.ConsumedCapacity.CapacityUnits *= 4
      cb(null, result)
    })
  })
}

