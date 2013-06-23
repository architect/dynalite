exports.types = {
  ReturnConsumedCapacity: {
    type: 'String',
    enum: ['TOTAL', 'NONE'],
  },
  RequestItems: {
    type: 'Map',
    requiredMap: true,
    tableMap: true,
    children: {
      type: 'Structure',
      children: {
        Keys: {
          type: 'List',
          notNull: true,
          children: {
            type: 'Map',
            children: {
              type: 'Structure',
              children: {
                S: 'String',
                B: 'Blob',
                N: 'String',
              }
            }
          }
        },
        AttributesToGet: 'List',
        ConsistentRead: 'Boolean',
      }
    }
  },
}

exports.custom = function(data) {
}

