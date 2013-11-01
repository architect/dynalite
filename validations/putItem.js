var db = require('../db'),
    validateAttributeValue = require('./index').validateAttributeValue

exports.types = {
  ReturnConsumedCapacity: {
    type: 'String',
    enum: ['INDEXES', 'TOTAL', 'NONE']
  },
  TableName: {
    type: 'String',
    notNull: true,
    regex: '[a-zA-Z0-9_.-]+',
    lengthGreaterThanOrEqual: 3,
    lengthLessThanOrEqual: 255,
  },
  Item: {
    type: 'Map',
    notNull: true,
    children: {
      type: 'Structure',
      children: {
        S: 'String',
        B: 'Blob',
        N: 'String',
        BS: {
          type: 'List',
          children: 'Blob',
        },
        NS: {
          type: 'List',
          children: 'String',
        },
        SS: {
          type: 'List',
          children: 'String',
        }
      }
    }
  },
  Expected: {
    type: 'Map',
    children: {
      type: 'Structure',
      children: {
        Exists: 'Boolean',
        Value: {
          type: 'Structure',
          children: {
            S: 'String',
            B: 'Blob',
            N: 'String',
            BS: {
              type: 'List',
              children: 'Blob',
            },
            NS: {
              type: 'List',
              children: 'String',
            },
            SS: {
              type: 'List',
              children: 'String',
            }
          }
        }
      }
    }
  },
  ReturnValues: {
    type: 'String',
    enum: ['ALL_NEW', 'UPDATED_OLD', 'ALL_OLD', 'NONE', 'UPDATED_NEW']
  },
  ReturnItemCollectionMetrics: {
    type: 'String',
    enum: ['SIZE', 'NONE']
  },
}

exports.custom = function(data) {
  for (var key in data.Item) {
    var msg = validateAttributeValue(data.Item[key])
    if (msg) return msg
  }
  if (data.Expected) {
    for (var key in data.Expected) {
      var exists = data.Expected[key].Exists == null || data.Expected[key].Exists
      if (exists && data.Expected[key].Value == null)
        return 'One or more parameter values were invalid: ' +
          '\'Exists\' is set to ' + (data.Expected[key].Exists == null ? 'null' : data.Expected[key].Exists) + '. ' +
          '\'Exists\' must be set to false when no Attribute value is specified'
      else if (!exists && data.Expected[key].Value != null)
        return 'One or more parameter values were invalid: ' +
          'Cannot expect an attribute to have a specified value while expecting it to not exist'
      if (data.Expected[key].Value != null) {
        var msg = validateAttributeValue(data.Expected[key].Value)
        if (msg) return msg
      }
    }
  }
  if (data.ReturnValues && data.ReturnValues != 'ALL_OLD' && data.ReturnValues != 'NONE')
    return 'ReturnValues can only be ALL_OLD or NONE'

  if (db.itemSize(data.Item) > 65536)
    return 'Item size has exceeded the maximum allowed size'
}

