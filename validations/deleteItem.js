var validations = require('./index')

exports.types = {
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
  ReturnValues: {
    type: 'String',
    enum: ['ALL_NEW', 'UPDATED_OLD', 'ALL_OLD', 'NONE', 'UPDATED_NEW'],
  },
  ReturnItemCollectionMetrics: {
    type: 'String',
    enum: ['SIZE', 'NONE'],
  },
  Key: {
    type: 'Map<AttributeValue>',
    notNull: true,
    children: 'AttrStruct<ValueStruct>',
  },
  ConditionalOperator: {
    type: 'String',
    enum: ['OR', 'AND'],
  },
  Expected: {
    type: 'Map<ExpectedAttributeValue>',
    children: {
      type: 'ValueStruct<ExpectedAttributeValue>',
      children: {
        AttributeValueList: {
          type: 'List',
          children: 'AttrStruct<ValueStruct>',
        },
        ComparisonOperator: {
          type: 'String',
          enum: ['IN', 'NULL', 'BETWEEN', 'LT', 'NOT_CONTAINS', 'EQ', 'GT', 'NOT_NULL', 'NE', 'LE', 'BEGINS_WITH', 'GE', 'CONTAINS'],
        },
        Exists: 'Boolean',
        Value: 'AttrStruct<FieldStruct>',
      },
    },
  },
  ConditionExpression: {
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

  var msg = validations.validateExpressionParams(data, ['ConditionExpression'], ['Expected'])
  if (msg) return msg

  for (var key in data.Key) {
    msg = validations.validateAttributeValue(data.Key[key])
    if (msg) return msg
  }

  msg = validations.validateAttributeConditions(data)
  if (msg) return msg

  msg = validations.validateExpressions(data)
  if (msg) return msg
}
