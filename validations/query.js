var validateAttributeValue = require('./index').validateAttributeValue

exports.types = {
  Limit: {
    type: 'Integer',
    greaterThanOrEqual: 1,
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
  QueryFilter: {
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
    enum: ['INDEXES', 'TOTAL', 'NONE']
  },
  AttributesToGet: {
    type: 'List',
    lengthGreaterThanOrEqual: 1,
    lengthLessThanOrEqual: 255,
    children: 'String',
  },
  TableName: {
    type: 'String',
    notNull: true,
    regex: '[a-zA-Z0-9_.-]+',
    lengthGreaterThanOrEqual: 3,
    lengthLessThanOrEqual: 255,
  },
  Select: {
    type: 'String',
    enum: ['SPECIFIC_ATTRIBUTES', 'COUNT', 'ALL_ATTRIBUTES', 'ALL_PROJECTED_ATTRIBUTES']
  },
  ConsistentRead: 'Boolean',
  IndexName: {
    type: 'String',
    regex: '[a-zA-Z0-9_.-]+',
    lengthGreaterThanOrEqual: 3,
    lengthLessThanOrEqual: 255,
  },
  ScanIndexForward: 'Boolean',
}

exports.custom = function(data) {
  if (!data.KeyConditions)
    return 'Conditions must not be null'
  var conditionKeys = Object.keys(data.KeyConditions)

  var msg = '', i
  var lengths = {
    NULL: 0,
    NOT_NULL: 0,
    EQ: 1,
    NE: 1,
    LE: 1,
    LT: 1,
    GE: 1,
    GT: 1,
    CONTAINS: 1,
    NOT_CONTAINS: 1,
    BEGINS_WITH: 1,
    IN: [1],
    BETWEEN: 2,
  }
  for (var key in data.KeyConditions) {
    var comparisonOperator = data.KeyConditions[key].ComparisonOperator
    var attrValList = data.KeyConditions[key].AttributeValueList || []
    for (i = 0; i < attrValList.length; i++) {
      msg = validateAttributeValue(attrValList[i])
      if (msg) return msg
    }
    if ((typeof lengths[comparisonOperator] == 'number' && attrValList.length != lengths[comparisonOperator]) ||
        (attrValList.length < lengths[comparisonOperator][0] || attrValList.length > lengths[comparisonOperator][1]))
      return 'One or more parameter values were invalid: Invalid number of argument(s) for the ' +
        comparisonOperator + ' ComparisonOperator'
  }

  if (conditionKeys.length != 1 && conditionKeys.length != 2) {
    return 'Conditions can be of length 1 or 2 only'
  }

  for (var key in data.QueryFilter) {
    var comparisonOperator = data.QueryFilter[key].ComparisonOperator
    var attrValList = data.QueryFilter[key].AttributeValueList || []
    for (i = 0; i < attrValList.length; i++) {
      msg = validateAttributeValue(attrValList[i])
      if (msg) return msg
    }
    if ((typeof lengths[comparisonOperator] == 'number' && attrValList.length != lengths[comparisonOperator]) ||
        (attrValList.length < lengths[comparisonOperator][0] || attrValList.length > lengths[comparisonOperator][1]))
      return 'One or more parameter values were invalid: Invalid number of argument(s) for the ' +
        comparisonOperator + ' ComparisonOperator'
  }

  if (data.ExclusiveStartKey) {
    for (key in data.ExclusiveStartKey) {
      msg = validateAttributeValue(data.ExclusiveStartKey[key])
      if (msg) return 'The provided starting key is invalid: ' + msg
    }
  }

  if (data.AttributesToGet) {
    var attrs = Object.create(null)
    for (i = 0; i < data.AttributesToGet.length; i++) {
      if (attrs[data.AttributesToGet[i]])
        return 'One or more parameter values were invalid: Duplicate value in attribute name: ' +
          data.AttributesToGet[i]
      attrs[data.AttributesToGet[i]] = true
    }
  }
}

