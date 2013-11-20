var once = require('once'),
    db = require('../db'),
    scan = require('./scan')

module.exports = function query(store, data, cb) {
  cb = once(cb)

  store.getTable(data.TableName, function(err, table) {
    if (err) return cb(err)

    var i, keySchema, key, comparisonOperator, limit, firstKey, indexAttrs

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

    data.ScanFilter = data.KeyConditions
    delete data.KeyConditions
    if (data.Limit) {
      limit = data.Limit
      delete data.Limit
    }

    scan(store, data, function(err, result) {
      if (err) return cb(err)
      delete result.ScannedCount
      if (result.Items) {
        if (data.IndexName) {
          result.Items.sort(function(item1, item2) {
            var type1 = Object.keys(item1[keySchema[1].AttributeName] || {})[0],
                val1 = type1 ? item1[keySchema[1].AttributeName][type1] : '',
                type2 = Object.keys(item2[keySchema[1].AttributeName] || {})[0],
                val2 = type2 ? item2[keySchema[1].AttributeName][type2] : ''
            return db.toLexiStr(val1, type1).localeCompare(db.toLexiStr(val2, type2))
          })
        }
        if (data.ScanIndexForward === false) result.Items.reverse()
        if (limit && result.Items.length > limit) {
          result.Items.splice(limit)
          result.Count = result.Items.length
          if (result.Count) {
            result.LastEvaluatedKey = table.KeySchema.reduce(function(key, schemaPiece) {
              key[schemaPiece.AttributeName] = result.Items[result.Items.length - 1][schemaPiece.AttributeName]
              return key
            }, {})
          }
        }
      } else if (limit && result.Count > limit) {
        result.Count = limit
      }
      if (result.ConsumedCapacity && indexAttrs && data.Select == 'ALL_ATTRIBUTES')
        result.ConsumedCapacity.CapacityUnits *= 4
      cb(null, result)
    })
  })
}

