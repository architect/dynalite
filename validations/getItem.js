var validations = require('./index')

exports.types = {
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
  TableName: {
    type: 'String',
    notNull: true,
    regex: '[a-zA-Z0-9_.-]+',
    lengthGreaterThanOrEqual: 3,
    lengthLessThanOrEqual: 255,
  },
  Key: {
    type: 'Map',
    notNull: true,
    children: 'AttrStructure',
  },
  ConsistentRead: 'Boolean',
  ProjectionExpression: {
    type: 'String',
  },
  ExpressionAttributeNames: {
    type: 'Map',
    children: 'String',
  },
}

exports.custom = function(data) {

  var msg = validations.validateExpressionParams(data, ['ProjectionExpression'], ['AttributesToGet'])
  if (msg) return msg

  for (var key in data.Key) {
    msg = validations.validateAttributeValue(data.Key[key])
    if (msg) return msg
  }
  if (data.AttributesToGet) {
    msg = validations.findDuplicate(data.AttributesToGet)
    if (msg) return 'One or more parameter values were invalid: Duplicate value in attribute name: ' + msg
  }
  msg = validations.validateExpressions(data)
  if (msg) return msg
}
