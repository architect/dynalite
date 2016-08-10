var once = require('once'),
    db = require('../db')

module.exports = function query(store, data, cb) {
  cb = once(cb)

  store.getTable(data.TableName, function(err, table) {
    if (err) return cb(err)

    var keySchema = table.KeySchema, startKeyNames = keySchema.map(function(key) { return key.AttributeName }),
      hashKey = startKeyNames[0], rangeKey = startKeyNames[1], fetchFromItemDb = false, isLocal

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
      fetchFromItemDb = data.Select == 'ALL_ATTRIBUTES' && index.Projection.ProjectionType != 'ALL'
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
        return db.validationError('Attempted conditional constraint is not an indexable operation')
      }

      if (data.KeyConditions[attr].AttributeValueList.some(function(attrVal) { return attrVal[type] == null })) {
        return db.validationError('One or more parameter values were invalid: Condition parameter type does not match schema type')
      }

      if (isHash && comparisonOperator != 'EQ') {
        return db.validationError('Query key condition not supported')
      }
    })
    if (err) return cb(err)

    var hashType = Object.keys(data.KeyConditions[hashKey].AttributeValueList[0])[0]
    var hashVal = data.KeyConditions[hashKey].AttributeValueList[0][hashType]

    if (data.ExclusiveStartKey) {
      var tableStartKey = table.KeySchema.reduce(function(obj, attr) {
        obj[attr.AttributeName] = data.ExclusiveStartKey[attr.AttributeName]
        return obj
      }, {})
      if ((err = db.validateKey(tableStartKey, table)) != null) {
        return cb(db.validationError('The provided starting key is invalid: ' + err.message))
      }

      if (!rangeKey || !data.KeyConditions[rangeKey]) {
        if (data.ExclusiveStartKey[hashKey][hashType] != hashVal) {
          return cb(db.validationError('The provided starting key is outside query boundaries based on provided conditions'))
        }
      } else {
        var matchesRange = db.compare(data.KeyConditions[rangeKey].ComparisonOperator,
          data.ExclusiveStartKey[rangeKey], data.KeyConditions[rangeKey].AttributeValueList)
        if (!matchesRange) {
          return cb(db.validationError('The provided starting key does not match the range key predicate'))
        }
        if (data.ExclusiveStartKey[hashKey][hashType] != hashVal) {
          return cb(db.validationError('The query can return at most one row and cannot be restarted'))
        }
      }
    }

    if ((err = db.validateKeyPaths((data._projection || {}).nestedPaths, table)) != null) return cb(err)

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

    if (fetchFromItemDb && !isLocal) {
      return cb(db.validationError('One or more parameter values were invalid: ' +
        'Select type ALL_ATTRIBUTES is not supported for global secondary index ' +
        data.IndexName + ' because its projection type is not ALL'))
    }

    if ((err = db.validateKeyPaths((data._filter || {}).nestedPaths, table)) != null) return cb(err)

    var opts = {reverse: data.ScanIndexForward === false, limit: data.Limit ? data.Limit + 1 : -1}

    opts.gte = db.hashPrefix(hashVal, hashType) + '/' + db.toRangeStr(hashVal, hashType) + '/'
    opts.lt = opts.gte + '~'

    if (data.KeyConditions[rangeKey]) {
      var rangeStrPrefix = db.toRangeStr(data.KeyConditions[rangeKey].AttributeValueList[0])
      var rangeStr = rangeStrPrefix + '/'
      var comp = data.KeyConditions[rangeKey].ComparisonOperator
      if (comp == 'EQ') {
        opts.gte += rangeStr
        opts.lte = opts.gte + '~'
        delete opts.lt
      } else if (comp == 'LT') {
        opts.lt = opts.gte + rangeStr
      } else if (comp == 'LE') {
        opts.lte = opts.gte + rangeStr + '~'
        delete opts.lt
      } else if (comp == 'GT') {
        opts.gt = opts.gte + rangeStr + '~'
        delete opts.gte
      } else if (comp == 'GE') {
        opts.gte += rangeStr
      } else if (comp == 'BEGINS_WITH') {
        opts.lt = opts.gte + rangeStrPrefix + '~'
        opts.gte += rangeStr
      } else if (comp == 'BETWEEN') {
        opts.lte = opts.gte + db.toRangeStr(data.KeyConditions[rangeKey].AttributeValueList[1]) + '/~'
        opts.gte += rangeStr
        delete opts.lt
      }
    }

    if (data.ExclusiveStartKey) {
      var startKey = db.createIndexKey(data.ExclusiveStartKey, table, keySchema)

      if (data.ScanIndexForward === false) {
        opts.lt = startKey
        delete opts.lte
      } else {
        opts.gt = startKey
        delete opts.gte
      }
    }

    db.queryTable(store, table, data, opts, isLocal, fetchFromItemDb, startKeyNames, cb)
  })
}
