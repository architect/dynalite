var validateAttributeValue = require('./index').validateAttributeValue

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
  ReturnItemCollectionMetrics: {
    type: 'String',
    enum: ['SIZE', 'NONE']
  },
  ReturnValues: {
    type: 'String',
    enum: ['ALL_NEW', 'UPDATED_OLD', 'ALL_OLD', 'NONE', 'UPDATED_NEW']
  },
  Key: {
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
  AttributeUpdates: {
    type: 'Map',
    children: {
      type: 'Structure',
      children: {
        Action: 'String',
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
}

exports.custom = function(data) {

  for (var key in data.Key) {
    var msg = validateAttributeValue(data.Key[key])
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

  if (data.AttributeUpdates) {
    for (var key in data.AttributeUpdates) {
      if (data.AttributeUpdates[key].Value != null) {
        var msg = validateAttributeValue(data.AttributeUpdates[key].Value)
        if (msg) return msg
      }
      if (data.AttributeUpdates[key].Value == null && data.AttributeUpdates[key].Action != 'DELETE')
        return 'One or more parameter values were invalid: ' +
          'Only DELETE action is allowed when no attribute value is specified'
      if (data.AttributeUpdates[key].Value != null && data.AttributeUpdates[key].Action == 'DELETE') {
        var type = Object.keys(data.AttributeUpdates[key].Value)[0]
        if (type != 'SS' && type != 'NS' && type != 'BS')
          return 'One or more parameter values were invalid: ' +
            'Action DELETE is not supported for the type ' + type
      }
      if (data.AttributeUpdates[key].Value != null && data.AttributeUpdates[key].Action == 'ADD') {
        var type = Object.keys(data.AttributeUpdates[key].Value)[0]
        if (type != 'SS' && type != 'NS' && type != 'BS' && type != 'N')
          return 'One or more parameter values were invalid: ' +
            'Action ADD is not supported for the type ' + type
      }
    }
  }
}

