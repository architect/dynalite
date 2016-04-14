var Big = require('big.js'),
    db = require('../db')

module.exports = function updateItem(store, data, cb) {

  store.getTable(data.TableName, function(err, table) {
    if (err) return cb(err)

    var key = db.validateKey(data.Key, table), itemDb = store.getItemDb(data.TableName)
    if (key instanceof Error) return cb(key)

    if (data.AttributeUpdates || data._updates) {
      for (var i = 0; i < table.KeySchema.length; i++) {
        var keyName = table.KeySchema[i].AttributeName
        if (data.AttributeUpdates && data.AttributeUpdates[keyName]) {
          return cb(db.validationError('One or more parameter values were invalid: ' +
            'Cannot update attribute ' + keyName + '. This attribute is part of the key'))
        } else if (data._updates) {
          var sections = data._updates.sections
          for (var j = 0; j < sections.length; j++) {
            if (sections[j].path[0] == keyName) {
              return cb(db.validationError('One or more parameter values were invalid: ' +
                'Cannot update attribute ' + keyName + '. This attribute is part of the key'))
            }
          }
        }
      }
      err = db.traverseIndexes(table, function(attr, type, indexName) {
        if (data._updates) {
          sections = data._updates.sections
          for (var i = 0; i < sections.length; i++) {
            var section = sections[i]
            if (section.path.length == 1 && section.path[0] == attr && section.attrType != null && section.attrType != type) {
              return db.validationError('One or more parameter values were invalid: ' +
                'Type mismatch for Index Key ' + attr + ' Expected: ' + type +
                ' Actual: ' + section.attrType + ' IndexName: ' + indexName)
            }
          }
        } else {
          var attrUpdate = data.AttributeUpdates[attr] && data.AttributeUpdates[attr].Value
          if (attrUpdate[type] == null) {
            return db.validationError('One or more parameter values were invalid: ' +
              'Type mismatch for Index Key ' + attr + ' Expected: ' + type +
              ' Actual: ' + Object.keys(attrUpdate)[0] + ' IndexName: ' + indexName)
          }
        }
      })
      if (err) return cb(err)
      if (data._updates) {
        err = db.traverseIndexes(table, function(attr) {
          var paths = data._updates.nestedPaths
          if (paths[attr]) {
            return db.validationError('Key attributes must be scalars; ' +
              'list random access \'[]\' and map lookup \'.\' are not allowed: IndexKey: ' + attr)
          }
        })
        if (err) return cb(err)
      }
    }

    itemDb.lock(key, function(release) {
      cb = release(cb)

      itemDb.get(key, function(err, oldItem) {
        if (err && err.name != 'NotFoundError') return cb(err)

        if ((err = db.checkConditional(data, oldItem)) != null) return cb(err)

        var attr, type, returnObj = {}, item = data.Key

        if (oldItem) {
          for (attr in oldItem) {
            item[attr] = oldItem[attr]
          }
          if (data.ReturnValues == 'ALL_OLD') {
            returnObj.Attributes = oldItem
          } else if (data.ReturnValues == 'UPDATED_OLD') {
            if (data._updates) {
              returnObj.Attributes = db.mapPaths(data._updates.paths, oldItem)
            } else {
              returnObj.Attributes = {}
              for (attr in data.AttributeUpdates) {
                if (oldItem[attr] != null) {
                  returnObj.Attributes[attr] = oldItem[attr]
                }
              }
            }
          }
        }

        function resolve(val, item) {
          if (Array.isArray(val)) {
            val = db.mapPath(val, item)
          } else if (val.type == 'add' || val.type == 'subtract') {
            var val1 = resolve(val.args[0], item)
            if (typeof val1 == 'string') return val1
            if (val1.N == null) {
              return 'An operand in the update expression has an incorrect data type'
            }
            var val2 = resolve(val.args[1], item)
            if (typeof val2 == 'string') return val2
            if (val2.N == null) {
              return 'An operand in the update expression has an incorrect data type'
            }
            val = {N: new Big(val1.N)[val.type == 'add' ? 'plus' : 'minus'](val2.N).toFixed()}
          } else if (val.type == 'function' && val.name == 'if_not_exists') {
            val = db.mapPath(val.args[0], item) || resolve(val.args[1], item)
          } else if (val.type == 'function' && val.name == 'list_append') {
            val1 = resolve(val.args[0], item)
            if (typeof val1 == 'string') return val1
            if (val1.L == null) {
              return 'An operand in the update expression has an incorrect data type'
            }
            val2 = resolve(val.args[1], item)
            if (typeof val2 == 'string') return val2
            if (val2.L == null) {
              return 'An operand in the update expression has an incorrect data type'
            }
            return {L: val1.L.concat(val2.L)}
          }
          return val || 'The provided expression refers to an attribute that does not exist in the item'
        }

        if (data._updates) {
          var toSquash = []
          var sections = data._updates.sections
          for (var i = 0; i < sections.length; i++) {
            var section = sections[i]
            if (section.type == 'set') {
              section.val = resolve(section.val, item)
              if (typeof section.val == 'string') {
                return cb(db.validationError(section.val))
              }
            }
          }
          for (i = 0; i < sections.length; i++) {
            section = sections[i]
            var parent = db.mapPath(section.path.slice(0, -1), item)
            attr = section.path[section.path.length - 1]
            if (parent == null || (typeof attr == 'number' ? parent.L : parent.M) == null) {
              return cb(db.validationError('The document path provided in the update expression is invalid for update'))
            }
            var existing = parent.M ? parent.M[attr] : parent.L[attr]
            var alreadyExists = existing != null
            if (section.type == 'remove') {
              if (parent.M) {
                delete parent.M[attr]
              } else if (parent.L) {
                parent.L.splice(attr, 1)
              }
            } else if (section.type == 'delete') {
              if (alreadyExists && Object.keys(existing)[0] != section.attrType) {
                return cb(db.validationError('An operand in the update expression has an incorrect data type'))
              }
              if (alreadyExists) {
                existing[section.attrType] = existing[section.attrType].filter(function(val) { // eslint-disable-line no-loop-func
                  return !~section.val[section.attrType].indexOf(val)
                })
                if (!existing[section.attrType].length) {
                  if (parent.M) {
                    delete parent.M[attr]
                  } else if (parent.L) {
                    parent.L.splice(attr, 1)
                  }
                }
              }
            } else if (section.type == 'add') {
              if (alreadyExists && Object.keys(existing)[0] != section.attrType) {
                return cb(db.validationError('An operand in the update expression has an incorrect data type'))
              }
              if (section.attrType == 'N') {
                if (!existing) existing = {N: '0'}
                existing.N = new Big(existing.N).plus(section.val.N).toFixed()
              } else {
                if (!existing) existing = {}
                if (!existing[section.attrType]) existing[section.attrType] = []
                existing[section.attrType] = existing[section.attrType].concat(section.val[section.attrType].filter(function(a) { // eslint-disable-line no-loop-func
                  return !~existing[section.attrType].indexOf(a)
                }))
              }
              if (!alreadyExists) {
                if (parent.M) {
                  parent.M[attr] = existing
                } else if (parent.L) {
                  if (attr > parent.L.length && !~toSquash.indexOf(parent)) {
                    toSquash.push(parent)
                  }
                  parent.L[attr] = existing
                }
              }
            } else if (section.type == 'set') {
              if (section.path.length == 1) {
                err = db.traverseIndexes(table, function(attr, type) {
                  if (section.path[0] == attr && section.val[type] == null) {
                    return db.validationError('The update expression attempted to update the secondary index key to unsupported type')
                  }
                })
                if (err) return cb(err)
              }
              if (parent.M) {
                parent.M[attr] = section.val
              } else if (parent.L) {
                if (attr > parent.L.length && !~toSquash.indexOf(parent)) {
                  toSquash.push(parent)
                }
                parent.L[attr] = section.val
              }
            }
          }
          toSquash.forEach(function(obj) { obj.L = obj.L.filter(Boolean) })
        }

        for (attr in data.AttributeUpdates) {
          if (data.AttributeUpdates[attr].Action == 'PUT' || data.AttributeUpdates[attr].Action == null) {
            item[attr] = data.AttributeUpdates[attr].Value
          } else if (data.AttributeUpdates[attr].Action == 'ADD') {
            if (data.AttributeUpdates[attr].Value.N) {
              if (item[attr] && !item[attr].N)
                return cb(db.validationError('Type mismatch for attribute to update'))
              if (!item[attr]) item[attr] = {N: '0'}
              item[attr].N = new Big(item[attr].N).plus(data.AttributeUpdates[attr].Value.N).toFixed()
            } else {
              type = Object.keys(data.AttributeUpdates[attr].Value)[0]
              if (item[attr] && !item[attr][type])
                return cb(db.validationError('Type mismatch for attribute to update'))
              if (!item[attr]) item[attr] = {}
              if (!item[attr][type]) item[attr][type] = []
              item[attr][type] = item[attr][type].concat(data.AttributeUpdates[attr].Value[type].filter(function(a) { // eslint-disable-line no-loop-func
                return !~item[attr][type].indexOf(a)
              }))
            }
          } else if (data.AttributeUpdates[attr].Action == 'DELETE') {
            if (data.AttributeUpdates[attr].Value) {
              type = Object.keys(data.AttributeUpdates[attr].Value)[0]
              if (item[attr] && !item[attr][type])
                return cb(db.validationError('Type mismatch for attribute to update'))
              if (item[attr] && item[attr][type]) {
                item[attr][type] = item[attr][type].filter(function(val) { // eslint-disable-line no-loop-func
                  return !~data.AttributeUpdates[attr].Value[type].indexOf(val)
                })
                if (!item[attr][type].length) delete item[attr]
              }
            } else {
              delete item[attr]
            }
          }
        }

        if (db.itemSize(item) > db.MAX_SIZE)
          return cb(db.validationError('Item size to update has exceeded the maximum allowed size'))

        if (data.ReturnValues == 'ALL_NEW') {
          returnObj.Attributes = item
        } else if (data.ReturnValues == 'UPDATED_NEW') {
          if (data._updates) {
            returnObj.Attributes = db.mapPaths(data._updates.paths, item)
          } else {
            returnObj.Attributes = {}
            for (attr in data.AttributeUpdates) {
              if (item[attr] != null) {
                returnObj.Attributes[attr] = item[attr]
              }
            }
          }
        }

        if (~['TOTAL', 'INDEXES'].indexOf(data.ReturnConsumedCapacity)) {
          returnObj.ConsumedCapacity = {
            CapacityUnits: Math.max(db.capacityUnits(oldItem), db.capacityUnits(item)),
            TableName: data.TableName,
            Table: data.ReturnConsumedCapacity == 'INDEXES' ?
              {CapacityUnits: Math.max(db.capacityUnits(oldItem), db.capacityUnits(item))} : undefined,
          }
        }

        itemDb.put(key, item, function(err) {
          if (err) return cb(err)
          cb(null, returnObj)
        })
      })
    })
  })
}
