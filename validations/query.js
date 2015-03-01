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
  Select: {
    type: 'String',
    enum: ['SPECIFIC_ATTRIBUTES', 'COUNT', 'ALL_ATTRIBUTES', 'ALL_PROJECTED_ATTRIBUTES']
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
  TableName: {
    type: 'String',
    notNull: true,
    regex: '[a-zA-Z0-9_.-]+',
    lengthGreaterThanOrEqual: 3,
    lengthLessThanOrEqual: 255,
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
  var conditionKeys = Object.keys(data.KeyConditions), key, comparisonOperator, attrValList

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
  var types = {
    EQ: ['S', 'N', 'B', 'SS', 'NS', 'BS'],
    NE: ['S', 'N', 'B', 'SS', 'NS', 'BS'],
    LE: ['S', 'N', 'B'],
    LT: ['S', 'N', 'B'],
    GE: ['S', 'N', 'B'],
    GT: ['S', 'N', 'B'],
    CONTAINS: ['S', 'N', 'B'],
    NOT_CONTAINS: ['S', 'N', 'B'],
    BEGINS_WITH: ['S', 'B'],
    IN: ['S', 'N', 'B'],
    BETWEEN: ['S', 'N', 'B'],
  }
  for (key in data.KeyConditions) {
    comparisonOperator = data.KeyConditions[key].ComparisonOperator
    attrValList = data.KeyConditions[key].AttributeValueList || []
    for (i = 0; i < attrValList.length; i++) {
      msg = validateAttributeValue(attrValList[i])
      if (msg) return msg
    }

    if ((typeof lengths[comparisonOperator] == 'number' && attrValList.length != lengths[comparisonOperator]) ||
        (attrValList.length < lengths[comparisonOperator][0] || attrValList.length > lengths[comparisonOperator][1]))
      return 'One or more parameter values were invalid: Invalid number of argument(s) for the ' +
        comparisonOperator + ' ComparisonOperator'

    if (types[comparisonOperator]) {
      for (i = 0; i < attrValList.length; i++) {
        if (!~types[comparisonOperator].indexOf(Object.keys(attrValList[i])[0]))
          return 'One or more parameter values were invalid: ComparisonOperator ' + comparisonOperator +
            ' is not valid for ' + Object.keys(attrValList[i])[0] + ' AttributeValue type'
      }
    }
  }

  if (conditionKeys.length != 1 && conditionKeys.length != 2) {
    return 'Conditions can be of length 1 or 2 only'
  }

  for (key in data.QueryFilter) {
    comparisonOperator = data.QueryFilter[key].ComparisonOperator
    attrValList = data.QueryFilter[key].AttributeValueList || []
    for (i = 0; i < attrValList.length; i++) {
      msg = validateAttributeValue(attrValList[i])
      if (msg) return msg
    }

    if ((typeof lengths[comparisonOperator] == 'number' && attrValList.length != lengths[comparisonOperator]) ||
        (attrValList.length < lengths[comparisonOperator][0] || attrValList.length > lengths[comparisonOperator][1]))
      return 'One or more parameter values were invalid: Invalid number of argument(s) for the ' +
        comparisonOperator + ' ComparisonOperator'

    if (types[comparisonOperator]) {
      for (i = 0; i < attrValList.length; i++) {
        if (!~types[comparisonOperator].indexOf(Object.keys(attrValList[i])[0]))
          return 'One or more parameter values were invalid: ComparisonOperator ' + comparisonOperator +
            ' is not valid for ' + Object.keys(attrValList[i])[0] + ' AttributeValue type'
      }
    }
  }

  for (key in data.ExclusiveStartKey) {
    msg = validateAttributeValue(data.ExclusiveStartKey[key])
    if (msg) return 'The provided starting key is invalid: ' + msg
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

