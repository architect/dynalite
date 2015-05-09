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
  ReturnItemCollectionMetrics: {
    type: 'String',
    enum: ['SIZE', 'NONE'],
  },
  ReturnValues: {
    type: 'String',
    enum: ['ALL_NEW', 'UPDATED_OLD', 'ALL_OLD', 'NONE', 'UPDATED_NEW'],
  },
  Key: {
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
  AttributeUpdates: {
    type: 'Map',
    children: {
      type: 'Structure',
      children: {
        Action: 'String',
        Value: 'AttrStructure',
      },
    },
  },
}

exports.custom = function(data) {
  var msg, key, type
  for (key in data.Key) {
    msg = validateAttributeValue(data.Key[key])
    if (msg) return msg
  }

  msg = validateAttributeConditions(data)
  if (msg) return msg

  if (data.AttributeUpdates) {
    for (key in data.AttributeUpdates) {
      if (data.AttributeUpdates[key].Value != null) {
        msg = validateAttributeValue(data.AttributeUpdates[key].Value)
        if (msg) return msg
      }
      if (data.AttributeUpdates[key].Value == null && data.AttributeUpdates[key].Action != 'DELETE')
        return 'One or more parameter values were invalid: ' +
          'Only DELETE action is allowed when no attribute value is specified'
      if (data.AttributeUpdates[key].Value != null && data.AttributeUpdates[key].Action == 'DELETE') {
        type = Object.keys(data.AttributeUpdates[key].Value)[0]
        if (type != 'SS' && type != 'NS' && type != 'BS')
          return 'One or more parameter values were invalid: ' +
            'DELETE action with value is not supported for the type ' + type
      }
      if (data.AttributeUpdates[key].Value != null && data.AttributeUpdates[key].Action == 'ADD') {
        type = Object.keys(data.AttributeUpdates[key].Value)[0]
        if (type != 'SS' && type != 'NS' && type != 'BS' && type != 'N')
          return 'One or more parameter values were invalid: ' +
            'ADD action is not supported for the type ' + type
      }
    }
  }
}

