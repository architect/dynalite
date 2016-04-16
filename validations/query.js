var validateAttributeValue = require('./index').validateAttributeValue,
    validateExpressionParams = require('./index').validateExpressionParams,
    validateExpressions = require('./index').validateExpressions,
    convertKeyCondition = require('./index').convertKeyCondition

exports.types = {
  Limit: {
    type: 'Integer',
    greaterThanOrEqual: 1,
  },
  ExclusiveStartKey: {
    type: 'Map',
    children: 'AttrStructure',
  },
  ReturnConsumedCapacity: {
    type: 'String',
    enum: ['INDEXES', 'TOTAL', 'NONE'],
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
          children: 'AttrStructure',
        },
        ComparisonOperator: {
          type: 'String',
          notNull: true,
          enum: ['IN', 'NULL', 'BETWEEN', 'LT', 'NOT_CONTAINS', 'EQ', 'GT', 'NOT_NULL', 'NE', 'LE', 'BEGINS_WITH', 'GE', 'CONTAINS'],
        },
      },
    },
  },
  Select: {
    type: 'String',
    enum: ['SPECIFIC_ATTRIBUTES', 'COUNT', 'ALL_ATTRIBUTES', 'ALL_PROJECTED_ATTRIBUTES'],
  },
  ConditionalOperator: {
    type: 'String',
    enum: ['OR', 'AND'],
  },
  KeyConditions: {
    type: 'Map',
    children: {
      type: 'Structure',
      children: {
        AttributeValueList: {
          type: 'List',
          children: 'AttrStructure',
        },
        ComparisonOperator: {
          type: 'String',
          notNull: true,
          enum: ['IN', 'NULL', 'BETWEEN', 'LT', 'NOT_CONTAINS', 'EQ', 'GT', 'NOT_NULL', 'NE', 'LE', 'BEGINS_WITH', 'GE', 'CONTAINS'],
        },
      },
    },
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
  KeyConditionExpression: {
    type: 'String',
  },
  FilterExpression: {
    type: 'String',
  },
  ProjectionExpression: {
    type: 'String',
  },
  ExpressionAttributeValues: {
    type: 'Map',
    children: 'AttrStructure',
  },
  ExpressionAttributeNames: {
    type: 'Map',
    children: 'String',
  },
}

exports.custom = function(data) {

  var msg = validateExpressionParams(data,
    ['ProjectionExpression', 'FilterExpression', 'KeyConditionExpression'],
    ['AttributesToGet', 'QueryFilter', 'ConditionalOperator', 'KeyConditions'])
  if (msg) return msg

  var i, key, comparisonOperator, attrValList, lengths = {
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

  if (data.AttributesToGet) {
    var attrs = Object.create(null)
    for (i = 0; i < data.AttributesToGet.length; i++) {
      if (attrs[data.AttributesToGet[i]])
        return 'One or more parameter values were invalid: Duplicate value in attribute name: ' +
          data.AttributesToGet[i]
      attrs[data.AttributesToGet[i]] = true
    }
  }

  for (key in data.ExclusiveStartKey) {
    msg = validateAttributeValue(data.ExclusiveStartKey[key])
    if (msg) return 'The provided starting key is invalid: ' + msg
  }

  if (data.KeyConditions == null && data.KeyConditionExpression == null) {
    return 'Either the KeyConditions or KeyConditionExpression parameter must be specified in the request.'
  }

  msg = validateExpressions(data)
  if (msg) return msg

  if (data._keyCondition != null) {
    data.KeyConditions = convertKeyCondition(data._keyCondition.expression)
    if (typeof data.KeyConditions == 'string') {
      return data.KeyConditions
    }
  }

  var numConditions = 0
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
    numConditions++
  }
  if (numConditions != 1 && numConditions != 2) {
    return 'Conditions can be of length 1 or 2 only'
  }
}
