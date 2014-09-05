var db = require('../db'),
    validateAttributeValue = require('./index').validateAttributeValue,
    validateAttributeConditions = require('./index').validateAttributeConditions;

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
  ConditionalOperator: {
    type: 'String',
    enum: ['OR', 'AND']
  },
  Expected: {
    type: 'Map',
    children: {
      type: 'Structure',
      children: {
        AttributeValueList: {
          type: 'List',
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
        ComparisonOperator: {
          type: 'String',
          enum: ['IN', 'NULL', 'BETWEEN', 'LT', 'NOT_CONTAINS', 'EQ', 'GT', 'NOT_NULL', 'NE', 'LE', 'BEGINS_WITH', 'GE', 'CONTAINS']
        },
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
  var msg;
  for (var key in data.Item) {
    msg = validateAttributeValue(data.Item[key])
    if (msg) return msg
  }
  
  msg = validateAttributeConditions(data);
  if (msg) return msg;

  if (data.ReturnValues && data.ReturnValues != 'ALL_OLD' && data.ReturnValues != 'NONE')
    return 'ReturnValues can only be ALL_OLD or NONE'

  if (db.itemSize(data.Item) > 65536)
    return 'Item size has exceeded the maximum allowed size'
}

