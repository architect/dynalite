var validations = require('./index')

exports.types = {
  Select: {
    type: 'String',
    enum: ['SPECIFIC_ATTRIBUTES', 'COUNT', 'ALL_ATTRIBUTES', 'ALL_PROJECTED_ATTRIBUTES'],
  },
  IndexName: {
    type: 'String',
    regex: '[a-zA-Z0-9_.-]+',
    lengthGreaterThanOrEqual: 3,
    lengthLessThanOrEqual: 255,
  },
  TotalSegments: {
    type: 'Integer',
    greaterThanOrEqual: 1,
  },
  ReturnConsumedCapacity: {
    type: 'String',
    enum: ['INDEXES', 'TOTAL', 'NONE'],
  },
  TableName: {
    type: 'String',
    notNull: true,
    regex: '[a-zA-Z0-9_.-]+',
    lengthGreaterThanOrEqual: 3,
    lengthLessThanOrEqual: 255,
  },
  ConditionalOperator: {
    type: 'String',
    enum: ['OR', 'AND'],
  },
  ScanFilter: {
    type: 'Map<Condition>',
    children: {
      type: 'ValueStruct<Condition>',
      children: {
        AttributeValueList: {
          type: 'List',
          children: 'AttrStruct<ValueStruct>',
        },
        ComparisonOperator: {
          type: 'String',
          notNull: true,
          enum: ['IN', 'NULL', 'BETWEEN', 'LT', 'NOT_CONTAINS', 'EQ', 'GT', 'NOT_NULL', 'NE', 'LE', 'BEGINS_WITH', 'GE', 'CONTAINS'],
        },
      },
    },
  },
  Segment: {
    type: 'Integer',
    greaterThanOrEqual: 0,
  },
  Limit: {
    type: 'Integer',
    greaterThanOrEqual: 1,
  },
  AttributesToGet: {
    type: 'List',
    lengthGreaterThanOrEqual: 1,
    lengthLessThanOrEqual: 255,
    children: 'String',
  },
  ExclusiveStartKey: {
    type: 'Map<AttributeValue>',
    children: 'AttrStruct<ValueStruct>',
  },
  FilterExpression: {
    type: 'String',
  },
  ProjectionExpression: {
    type: 'String',
  },
  ExpressionAttributeValues: {
    type: 'Map<AttributeValue>',
    children: 'AttrStruct<ValueStruct>',
  },
  ExpressionAttributeNames: {
    type: 'Map<java.lang.String>',
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
    // For some reason this message is only added to some messages...?
    var prepend = /contains duplicates|number set|numeric value|significant digits|number with magnitude/.test(msg) ? '' : 'The provided starting key is invalid: '
    if (msg) return prepend + msg
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
