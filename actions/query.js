var once = require('once'),
    db = require('../db'),
    scan = require('./scan')

module.exports = function query(data, cb) {
  cb = once(cb)

  db.getTable(data.TableName, function(err, table) {
    if (err) return cb(err)

    var i, keySchema, key, comparisonOperator, limit, firstKey

    if (data.ExclusiveStartKey && !Object.keys(data.ExclusiveStartKey).length) {
      return cb(db.validationError('The provided starting key is invalid'))
    }

    if (data.IndexName) {
      for (i = 0; i < (table.LocalSecondaryIndexes || []).length; i++) {
        if (table.LocalSecondaryIndexes[i].IndexName == data.IndexName) {
          keySchema = table.LocalSecondaryIndexes[i].KeySchema
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
    if (~['LE', 'LT', 'GE', 'GT', 'BEGINS_WITH', 'BETWEEN'].indexOf(comparisonOperator))
      return cb(db.validationError('Query key condition not supported'))

    data.ScanFilter = data.KeyConditions
    delete data.KeyConditions
    if (data.Limit) {
      limit = data.Limit
      delete data.Limit
    }

    scan(data, function(err, result) {
      if (err) return cb(err)
      delete result.ScannedCount
      if (result.Items) {
        if (data.ScanIndexForward === false) result.Items.reverse()
        if (limit) {
          result.Items.splice(limit)
          result.Count = result.Items.length
          if (result.Count) result.LastEvaluatedKey = result.Items[result.Items.length - 1]
        }
      } else if (limit && result.Count > limit) {
        result.Count = limit
      }
      cb(null, result)
    })
  })
}

