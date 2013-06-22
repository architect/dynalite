exports.types = {
  TableName: 'String',
  Key: {
    type: 'Map',
    children: {
      type: 'Structure',
      children: {
        S: 'String',
        B: 'Blob',
        N: 'String',
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
  ReturnConsumedCapacity: 'String',
  ReturnItemCollectionMetrics: 'String',
  ReturnValues: 'String',
}

exports.validations = {
  ReturnConsumedCapacity: {
    enum: ['TOTAL', 'NONE']
  },
  TableName: {
    required: true,
    tableName: true,
    regex: '[a-zA-Z0-9_.-]+',
    lengthGreaterThanOrEqual: 3,
    lengthLessThanOrEqual: 255,
  },
  Key: {
    notNull: true,
  },
}

exports.custom = function(data) {
  for (var key in data.Key) {
    if (!Object.keys(data.Key[key]).length)
      return 'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes'

    for (var type in data.Key[key]) {
      if (!data.Key[key][type])
        return 'One or more parameter values were invalid: An AttributeValue may not contain an empty string.'

      if (type == 'N' && isNaN(Number(data.Key[key][type])))
        return 'The parameter cannot be converted to a numeric value: ' + data.Key[key][type]
    }
  }
}

