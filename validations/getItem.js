var validateAttributeValue = require('./index').validateAttributeValue

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
    children: {
      type: 'Structure',
      children: {
        S: 'String',
        B: 'Blob',
        N: 'String',
        BS: {
          type: 'List',
          children: 'Blob',
        },
        NS: {
          type: 'List',
          children: 'String',
        },
        SS: {
          type: 'List',
          children: 'String',
        }
      }
    }
  },
  ConsistentRead: 'Boolean',
}

exports.custom = function(data) {
  for (var key in data.Key) {
    var msg = validateAttributeValue(data.Key[key])
    if (msg) return msg
  }
}

