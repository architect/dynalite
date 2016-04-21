var validations = require('./index')

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

  var msg = validations.validateExpressionParams(data,
    ['ProjectionExpression', 'FilterExpression', 'KeyConditionExpression'],
    ['AttributesToGet', 'QueryFilter', 'ConditionalOperator', 'KeyConditions'])
  if (msg) return msg

  var i, key
  msg = validations.validateConditions(data.QueryFilter)
  if (msg) return msg

  if (data.AttributesToGet) {
    msg = validations.findDuplicate(data.AttributesToGet)
    if (msg) return 'One or more parameter values were invalid: Duplicate value in attribute name: ' + msg
  }

  for (key in data.ExclusiveStartKey) {
    msg = validations.validateAttributeValue(data.ExclusiveStartKey[key])
    if (msg) return 'The provided starting key is invalid: ' + msg
  }

  if (data.KeyConditions == null && data.KeyConditionExpression == null) {
    return 'Either the KeyConditions or KeyConditionExpression parameter must be specified in the request.'
  }

  msg = validations.validateExpressions(data)
  if (msg) return msg

  if (data._keyCondition != null) {
    data.KeyConditions = validations.convertKeyCondition(data._keyCondition.expression)
    if (typeof data.KeyConditions == 'string') {
      return data.KeyConditions
    }
  }

  msg = validations.validateConditions(data.KeyConditions)
  if (msg) return msg

  var numConditions = Object.keys(data.KeyConditions || {}).length
  if (numConditions != 1 && numConditions != 2) {
    return 'Conditions can be of length 1 or 2 only'
  }
}
