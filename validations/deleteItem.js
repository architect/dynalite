var validateAttributeValue = require('./index').validateAttributeValue,
    validateAttributeConditions = require('./index').validateAttributeConditions

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
    enum: ['ALL_NEW', 'UPDATED_OLD', 'ALL_OLD', 'NONE', 'UPDATED_NEW']
  },
  ReturnItemCollectionMetrics: {
    type: 'String',
    enum: ['SIZE', 'NONE']
  },
  Key: {
    type: 'Map',
    notNull: true,
    children: 'AttrStructure',
  },
  ConditionalOperator: {
    type: 'String',
    enum: ['OR', 'AND']
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
          enum: ['IN', 'NULL', 'BETWEEN', 'LT', 'NOT_CONTAINS', 'EQ', 'GT', 'NOT_NULL', 'NE', 'LE', 'BEGINS_WITH', 'GE', 'CONTAINS']
        },
        Exists: 'Boolean',
        Value: 'AttrStructure',
      }
    }
  },
}

exports.custom = function(data) {
  var msg
  for (var key in data.Key) {
    msg = validateAttributeValue(data.Key[key])
    if (msg) return msg
  }

  msg = validateAttributeConditions(data)
  if (msg) return msg
}

