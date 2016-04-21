var validations = require('./index')

exports.types = {
  Limit: {
    type: 'Integer',
    greaterThanOrEqual: 1,
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
  Segment: {
    type: 'Integer',
    greaterThanOrEqual: 0,
  },
  Select: {
    type: 'String',
    enum: ['SPECIFIC_ATTRIBUTES', 'COUNT', 'ALL_ATTRIBUTES', 'ALL_PROJECTED_ATTRIBUTES'],
  },
  ScanFilter: {
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
  ConditionalOperator: {
    type: 'String',
    enum: ['OR', 'AND'],
  },
  TotalSegments: {
    type: 'Integer',
    greaterThanOrEqual: 1,
  },
  TableName: {
    type: 'String',
    notNull: true,
    regex: '[a-zA-Z0-9_.-]+',
    lengthGreaterThanOrEqual: 3,
    lengthLessThanOrEqual: 255,
  },
  ExclusiveStartKey: {
    type: 'Map',
    children: 'AttrStructure',
  },
  IndexName: {
    type: 'String',
    regex: '[a-zA-Z0-9_.-]+',
    lengthGreaterThanOrEqual: 3,
    lengthLessThanOrEqual: 255,
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
    ['ProjectionExpression', 'FilterExpression'],
    ['AttributesToGet', 'ScanFilter', 'ConditionalOperator'])
  if (msg) return msg

  if (data.AttributesToGet) {
    msg = validations.findDuplicate(data.AttributesToGet)
    if (msg) return 'One or more parameter values were invalid: Duplicate value in attribute name: ' + msg
  }

  msg = validations.validateConditions(data.ScanFilter)
  if (msg) return msg

  for (var key in data.ExclusiveStartKey) {
    msg = validations.validateAttributeValue(data.ExclusiveStartKey[key])
    if (msg) return 'The provided starting key is invalid: ' + msg
  }

  if (data.Segment && data.TotalSegments == null) {
    return 'The TotalSegments parameter is required but was not present in the request when Segment parameter is present'
  }

  if (data.TotalSegments) {
    if (data.Segment == null) {
      return 'The Segment parameter is required but was not present in the request when parameter TotalSegments is present'
    }
    if (data.Segment >= data.TotalSegments) {
      return 'The Segment parameter is zero-based and must be less than parameter TotalSegments: ' +
        'Segment: ' + data.Segment + ' is not less than TotalSegments: ' + data.TotalSegments
    }
  }

  msg = validations.validateExpressions(data, ['ProjectionExpression', 'FilterExpression'])
  if (msg) return msg
}
