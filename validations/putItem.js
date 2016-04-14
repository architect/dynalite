var db = require('../db'),
    validateAttributeValue = require('./index').validateAttributeValue,
    validateAttributeConditions = require('./index').validateAttributeConditions,
    validateExpressionParams = require('./index').validateExpressionParams,
    validateExpressions = require('./index').validateExpressions

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
  Item: {
    type: 'Map',
    notNull: true,
    children: 'AttrStructure',
  },
  ConditionalOperator: {
    type: 'String',
    enum: ['OR', 'AND'],
  },
  Expected: {
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
          enum: ['IN', 'NULL', 'BETWEEN', 'LT', 'NOT_CONTAINS', 'EQ', 'GT', 'NOT_NULL', 'NE', 'LE', 'BEGINS_WITH', 'GE', 'CONTAINS'],
        },
        Exists: 'Boolean',
        Value: 'AttrStructure',
      },
    },
  },
  ReturnValues: {
    type: 'String',
    enum: ['ALL_NEW', 'UPDATED_OLD', 'ALL_OLD', 'NONE', 'UPDATED_NEW'],
  },
  ReturnItemCollectionMetrics: {
    type: 'String',
    enum: ['SIZE', 'NONE'],
  },
  ConditionExpression: {
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

  var msg = validateExpressionParams(data, ['ConditionExpression'], ['Expected'])
  if (msg) return msg

  for (var key in data.Item) {
    msg = validateAttributeValue(data.Item[key])
    if (msg) return msg
  }

  if (data.ReturnValues && data.ReturnValues != 'ALL_OLD' && data.ReturnValues != 'NONE')
    return 'ReturnValues can only be ALL_OLD or NONE'

  if (db.itemSize(data.Item) > db.MAX_SIZE)
    return 'Item size has exceeded the maximum allowed size'

  msg = validateAttributeConditions(data)
  if (msg) return msg

  msg = validateExpressions(data)
  if (msg) return msg
}
