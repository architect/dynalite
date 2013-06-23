var validateAttributeValue = require('./index').validateAttributeValue

exports.types = {
  Limit: {
    type: 'Integer',
    greaterThanOrEqual: 1,
    lessThanOrEqual: 100,
  },
  ExclusiveStartKey: {
    type: 'Map',
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
  KeyConditions: {
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
          notNull: true,
          enum: ['IN', 'NULL', 'BETWEEN', 'LT', 'NOT_CONTAINS', 'EQ', 'GT', 'NOT_NULL', 'NE', 'LE', 'BEGINS_WITH', 'GE', 'CONTAINS']
        }
      }
    }
  },
  ReturnConsumedCapacity: {
    type: 'String',
    enum: ['TOTAL', 'NONE']
  },
  AttributesToGet: {
    type: 'List',
    lengthGreaterThanOrEqual: 1,
    lengthLessThanOrEqual: 255,
  },
  TableName: {
    type: 'String',
    required: true,
    tableName: true,
    regex: '[a-zA-Z0-9_.-]+',
  },
  Select: {
    type: 'String',
    enum: ['SPECIFIC_ATTRIBUTES', 'COUNT', 'ALL_ATTRIBUTES', 'ALL_PROJECTED_ATTRIBUTES']
  },
  ConsistentRead: 'Boolean',
  IndexName: {
    type: 'String',
    tableName: true,
    regex: '[a-zA-Z0-9_.-]+',
  },
  ScanIndexForward: 'Boolean',
}

exports.custom = function(data) {
  if (!data.KeyConditions)
    return 'Conditions must not be null'
  var conditionKeys = Object.keys(data.KeyConditions)

  var msg = ''
  var lengths = {
    NULL: 0,
    EQ: 1,
  }
  for (var key in data.KeyConditions) {
    var comparisonOperator = data.KeyConditions[key].ComparisonOperator
    var attrValList = data.KeyConditions[key].AttributeValueList || []
    for (var i = 0; i < attrValList.length; i++) {
      msg = validateAttributeValue(attrValList[i])
      if (msg) return msg
    }
    if (lengths[comparisonOperator] != attrValList.length)
      return 'The attempted filter operation is not supported for the provided filter argument count'
  }

  if (conditionKeys.length != 1 && conditionKeys.length != 2) {
    return 'Conditions can be of length 1 or 2 only'
  }

  if (data.ExclusiveStartKey) {
    for (key in data.ExclusiveStartKey) {
      msg = validateAttributeValue(data.ExclusiveStartKey[key])
      if (msg) return 'The provided starting key is invalid: ' + msg
    }
  }
}

