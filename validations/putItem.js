exports.types = {
  TableName: 'String',
  Item: {
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
  },
  Expected: {
    type: 'Map',
    children: {
      type: 'Structure',
      children: {
        Exists: 'Boolean',
        Value: {
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
    }
  },
  ReturnConsumedCapacity: 'String',
  ReturnItemCollectionMetrics: 'String',
  ReturnValues: 'String',
}

exports.validations = {
  ReturnConsumedCapacity: {
    enum: ['TOTAL', 'NONE']
  },
  TableName: {
    required: true,
    tableName: true,
    regex: '[a-zA-Z0-9_.-]+',
    lengthGreaterThanOrEqual: 3,
    lengthLessThanOrEqual: 255,
  },
  Item: {
    notNull: true
  },
}

exports.custom = function(data) {
}

