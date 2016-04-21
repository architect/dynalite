var validations = require('./index'),
    db = require('../db')

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

exports.custom = function(data, store) {

  var msg = validations.validateExpressionParams(data, ['ConditionExpression'], ['Expected'])
  if (msg) return msg

  for (var key in data.Item) {
    msg = validations.validateAttributeValue(data.Item[key])
    if (msg) return msg
  }

  if (data.ReturnValues && data.ReturnValues != 'ALL_OLD' && data.ReturnValues != 'NONE')
    return 'ReturnValues can only be ALL_OLD or NONE'

  if (db.itemSize(data.Item) > store.options.maxItemSize)
    return 'Item size has exceeded the maximum allowed size'

  msg = validations.validateAttributeConditions(data)
  if (msg) return msg

  msg = validations.validateExpressions(data)
  if (msg) return msg
}
