var once = require('once'),
    db = require('../db')

module.exports = function scan(store, data, cb) {
  cb = once(cb)

  store.getTable(data.TableName, function(err, table) {
    if (err) return cb(err)

    var keySchema = table.KeySchema, startKeyNames = keySchema.map(function(key) { return key.AttributeName }),
      fetchFromItemDb = false, isLocal

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
      if (!isLocal && data.ConsistentRead) {
        return cb(db.validationError('Consistent reads are not supported on global secondary indexes'))
      }
      keySchema = index.KeySchema
      fetchFromItemDb = data.Select == 'ALL_ATTRIBUTES' && index.Projection.ProjectionType != 'ALL'
      keySchema.forEach(function(key) { if (!~startKeyNames.indexOf(key.AttributeName)) startKeyNames.push(key.AttributeName) })
    }

    if (data.ExclusiveStartKey && Object.keys(data.ExclusiveStartKey).length != startKeyNames.length) {
      return data.IndexName ? cb(db.validationError('The provided starting key is invalid')) :
        cb(db.validationError('The provided starting key is invalid: The provided key element does not match the schema'))
    }

    if (data.IndexName && data.ExclusiveStartKey) {
      err = db.traverseKey(table, keySchema, function(attr, type, isHash) {
        if (!data.ExclusiveStartKey[attr]) {
          return db.validationError('The provided starting key is invalid')
        }
        return db.validateKeyPiece(data.ExclusiveStartKey, attr, type, isHash)
      })
      if (err) return cb(err)
    }

    if (fetchFromItemDb && !isLocal) {
      return cb(db.validationError('One or more parameter values were invalid: ' +
        'Select type ALL_ATTRIBUTES is not supported for global secondary index ' +
        data.IndexName + ' because its projection type is not ALL'))
    }

    if (data.ExclusiveStartKey) {
      var tableStartKey = table.KeySchema.reduce(function(obj, attr) {
        obj[attr.AttributeName] = data.ExclusiveStartKey[attr.AttributeName]
        return obj
      }, {})
      if ((err = db.validateKey(tableStartKey, table)) != null) {
        return cb(db.validationError('The provided starting key is invalid: ' + err.message))
      }
    }

    if (data.TotalSegments > 1) {
      if (data.Segment > 0)
        var hashStart = ('00' + Math.ceil(4096 * data.Segment / data.TotalSegments).toString(16)).slice(-3)
      var hashEnd = ('00' + (Math.ceil(4096 * (data.Segment + 1) / data.TotalSegments) - 1).toString(16)).slice(-3) + '~'
    }

    if (data.ExclusiveStartKey) {
      var startKey = db.createIndexKey(data.ExclusiveStartKey, table, keySchema)

      if (data.TotalSegments > 1 && (startKey < hashStart || startKey > hashEnd)) {
        return cb(db.validationError('The provided starting key is invalid: Invalid ExclusiveStartKey. ' +
          'Please use ExclusiveStartKey with correct Segment. ' +
          'TotalSegments: ' + data.TotalSegments + ' Segment: ' + data.Segment))
      }

      hashStart = startKey
    }

    if ((err = db.validateKeyPaths((data._projection || {}).nestedPaths, table)) != null) return cb(err)

    if ((err = db.validateKeyPaths((data._filter || {}).nestedPaths, table)) != null) return cb(err)

    var opts = {limit: data.Limit ? data.Limit + 1 : -1, gt: hashStart, lt: hashEnd}

    db.queryTable(store, table, data, opts, isLocal, fetchFromItemDb, startKeyNames, cb)
  })
}
