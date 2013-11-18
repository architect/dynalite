var validateAttributeValue = require('./index').validateAttributeValue

exports.types = {
  ReturnConsumedCapacity: {
    type: 'String',
    enum: ['INDEXES', 'TOTAL', 'NONE'],
  },
  RequestItems: {
    type: 'Map',
    notNull: true,
    lengthGreaterThanOrEqual: 1,
    children: {
      type: 'Structure',
      children: {
        Keys: {
          type: 'List',
          notNull: true,
          lengthGreaterThanOrEqual: 1,
          children: {
            type: 'Map',
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
          }
        },
        AttributesToGet: {
          type: 'List',
          lengthGreaterThanOrEqual: 1,
          lengthLessThanOrEqual: 255,
          children: 'String',
        },
        ConsistentRead: 'Boolean',
      }
    }
  },
}

exports.custom = function(data) {
  for (var table in data.RequestItems) {
    for (var i = 0; i < data.RequestItems[table].Keys.length; i++) {
      for (var key in data.RequestItems[table].Keys[i]) {
        var msg = validateAttributeValue(data.RequestItems[table].Keys[i][key])
        if (msg) return msg
      }
    }
  }
}

