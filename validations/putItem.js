exports.types = {
  ReturnConsumedCapacity: {
    type: 'String',
    enum: ['TOTAL', 'NONE']
  },
  TableName: {
    type: 'String',
    required: true,
    tableName: true,
    regex: '[a-zA-Z0-9_.-]+',
  },
  Item: {
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
  ReturnValues: {
    type: 'String',
    enum: ['ALL_NEW', 'UPDATED_OLD', 'ALL_OLD', 'NONE', 'UPDATED_NEW']
  },
  ReturnItemCollectionMetrics: {
    type: 'String',
    enum: ['SIZE', 'NONE']
  },
}

exports.validations = {
}

exports.custom = function(data) {
}

