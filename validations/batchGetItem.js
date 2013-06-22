exports.types = {
  RequestItems: {
    type: 'Map',
    children: {
      type: 'Structure',
      children: {
        Keys: {
          type: 'List',
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
  ReturnConsumedCapacity: 'String',
}

exports.validations = {
  ReturnConsumedCapacity: {
    enum: ['TOTAL', 'NONE']
  },
}

exports.custom = function(data) {
}

