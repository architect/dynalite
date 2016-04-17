var Big = require('big.js'),
    db = require('../db')

module.exports = function updateItem(store, data, cb) {

  store.getTable(data.TableName, function(err, table) {
    if (err) return cb(err)

    var key = db.validateKey(data.Key, table)
    if (key instanceof Error) return cb(key)

    if (data.AttributeUpdates || data._updates) {
      err = db.traverseKey(table, function(attr) {
        var hasKey = false
        if (data._updates) {
          var sections = data._updates.sections
          for (var j = 0; j < sections.length; j++) {
            if (sections[j].path[0] == attr) {
              hasKey = true
              break
            }
          }
        } else {
          hasKey = data.AttributeUpdates[attr] != null
        }
        if (hasKey) {
          return db.validationError('One or more parameter values were invalid: ' +
            'Cannot update attribute ' + attr + '. This attribute is part of the key')
        }
      })
      if (err) return cb(err)
      err = db.traverseIndexes(table, function(attr, type, index) {
        var actualType
        if (data._updates) {
          var sections = data._updates.sections
          for (var i = 0; i < sections.length; i++) {
            var section = sections[i]
            if (section.path.length == 1 && section.path[0] == attr) {
              actualType = section.attrType
              break
            }
          }
        } else {
          actualType = data.AttributeUpdates[attr] ? Object.keys(data.AttributeUpdates[attr].Value)[0] : null
        }
        if (actualType != null && actualType != type) {
          return db.validationError('One or more parameter values were invalid: ' +
            'Type mismatch for Index Key ' + attr + ' Expected: ' + type +
            ' Actual: ' + actualType + ' IndexName: ' + index.IndexName)
        }
      })
      if (err) return cb(err)
    }

    if (data._updates && (err = db.validateKeyPaths(data._updates.nestedPaths, table)) != null) {
      return cb(err)
    }

    var itemDb = store.getItemDb(data.TableName)

    itemDb.lock(key, function(release) {
      cb = release(cb)

      itemDb.get(key, function(err, oldItem) {
        if (err && err.name != 'NotFoundError') return cb(err)

        if ((err = db.checkConditional(data, oldItem)) != null) return cb(err)

        var returnObj = {}, item = data.Key

        if (oldItem) {
          for (var attr in oldItem) {
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

        err = data._updates ? applyUpdateExpression(data._updates.sections, table, item) :
          applyAttributeUpdates(data.AttributeUpdates, table, item)
        if (err) return cb(err)

        if (db.itemSize(item) > store.options.maxItemSize)
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

        returnObj.ConsumedCapacity = db.addConsumedCapacity(data, false, oldItem, item)

        itemDb.put(key, item, function(err) {
          if (err) return cb(err)
          cb(null, returnObj)
        })
      })
    })
  })
}

function applyAttributeUpdates(updates, table, item) {
  for (var attr in updates) {
    var update = updates[attr]
    if (update.Action == 'PUT' || update.Action == null) {
      item[attr] = update.Value
    } else if (update.Action == 'ADD') {
      if (update.Value.N) {
        if (item[attr] && !item[attr].N)
          return db.validationError('Type mismatch for attribute to update')
        if (!item[attr]) item[attr] = {N: '0'}
        item[attr].N = new Big(item[attr].N).plus(update.Value.N).toFixed()
      } else {
        var type = Object.keys(update.Value)[0]
        if (item[attr] && !item[attr][type])
          return db.validationError('Type mismatch for attribute to update')
        if (!item[attr]) item[attr] = {}
        if (!item[attr][type]) item[attr][type] = []
        item[attr][type] = item[attr][type].concat(update.Value[type].filter(function(a) { // eslint-disable-line no-loop-func
          return !~item[attr][type].indexOf(a)
        }))
      }
    } else if (update.Action == 'DELETE') {
      if (update.Value) {
        type = Object.keys(update.Value)[0]
        if (item[attr] && !item[attr][type])
          return db.validationError('Type mismatch for attribute to update')
        if (item[attr] && item[attr][type]) {
          item[attr][type] = item[attr][type].filter(function(val) { // eslint-disable-line no-loop-func
            return !~update.Value[type].indexOf(val)
          })
          if (!item[attr][type].length) delete item[attr]
        }
      } else {
        delete item[attr]
      }
    }
  }
}

function applyUpdateExpression(sections, table, item) {
  var toSquash = []
  for (var i = 0; i < sections.length; i++) {
    var section = sections[i]
    if (section.type == 'set') {
      section.val = resolveValue(section.val, item)
      if (typeof section.val == 'string') {
        return db.validationError(section.val)
      }
    }
  }
  for (i = 0; i < sections.length; i++) {
    section = sections[i]
    var parent = db.mapPath(section.path.slice(0, -1), item)
    var attr = section.path[section.path.length - 1]
    if (parent == null || (typeof attr == 'number' ? parent.L : parent.M) == null) {
      return db.validationError('The document path provided in the update expression is invalid for update')
    }
    var existing = parent.M ? parent.M[attr] : parent.L[attr]
    var alreadyExists = existing != null
    if (section.type == 'remove') {
      deleteFromParent(parent, attr)
    } else if (section.type == 'delete') {
      if (alreadyExists && Object.keys(existing)[0] != section.attrType) {
        return db.validationError('An operand in the update expression has an incorrect data type')
      }
      if (alreadyExists) {
        existing[section.attrType] = existing[section.attrType].filter(function(val) { // eslint-disable-line no-loop-func
          return !~section.val[section.attrType].indexOf(val)
        })
        if (!existing[section.attrType].length) {
          deleteFromParent(parent, attr)
        }
      }
    } else if (section.type == 'add') {
      if (alreadyExists && Object.keys(existing)[0] != section.attrType) {
        return db.validationError('An operand in the update expression has an incorrect data type')
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
        addToParent(parent, attr, existing, toSquash)
      }
    } else if (section.type == 'set') {
      if (section.path.length == 1) {
        var err = db.traverseIndexes(table, function(attr, type) {
          if (section.path[0] == attr && section.val[type] == null) {
            return db.validationError('The update expression attempted to update the secondary index key to unsupported type')
          }
        })
        if (err) return err
      }
      addToParent(parent, attr, section.val, toSquash)
    }
  }
  toSquash.forEach(function(obj) { obj.L = obj.L.filter(Boolean) })
}

function resolveValue(val, item) {
  if (Array.isArray(val)) {
    val = db.mapPath(val, item)
  } else if (val.type == 'add' || val.type == 'subtract') {
    var val1 = resolveValue(val.args[0], item)
    if (typeof val1 == 'string') return val1
    if (val1.N == null) {
      return 'An operand in the update expression has an incorrect data type'
    }
    var val2 = resolveValue(val.args[1], item)
    if (typeof val2 == 'string') return val2
    if (val2.N == null) {
      return 'An operand in the update expression has an incorrect data type'
    }
    val = {N: new Big(val1.N)[val.type == 'add' ? 'plus' : 'minus'](val2.N).toFixed()}
  } else if (val.type == 'function' && val.name == 'if_not_exists') {
    val = db.mapPath(val.args[0], item) || resolveValue(val.args[1], item)
  } else if (val.type == 'function' && val.name == 'list_append') {
    val1 = resolveValue(val.args[0], item)
    if (typeof val1 == 'string') return val1
    if (val1.L == null) {
      return 'An operand in the update expression has an incorrect data type'
    }
    val2 = resolveValue(val.args[1], item)
    if (typeof val2 == 'string') return val2
    if (val2.L == null) {
      return 'An operand in the update expression has an incorrect data type'
    }
    return {L: val1.L.concat(val2.L)}
  }
  return val || 'The provided expression refers to an attribute that does not exist in the item'
}

function deleteFromParent(parent, attr) {
  if (parent.M) {
    delete parent.M[attr]
  } else if (parent.L) {
    parent.L.splice(attr, 1)
  }
}

function addToParent(parent, attr, val, toSquash) {
  if (parent.M) {
    parent.M[attr] = val
  } else if (parent.L) {
    if (attr > parent.L.length && !~toSquash.indexOf(parent)) {
      toSquash.push(parent)
    }
    parent.L[attr] = val
  }
}
