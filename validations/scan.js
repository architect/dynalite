exports.types = {
  TableName: 'String',
  ExclusiveStartKey: {
    type: 'Map',
    children: {
      type: 'Structure',
      children: {
        S: 'String',
        B: 'Blob',
        N: 'String',
      }
    }
  },
  ScanFilter: {
    type: 'Map',
    children: {
      type: 'Structure',
      children: {
        AttributeValueList: {
          type: 'List',
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
        ComparisonOperator: 'String'
      }
    }
  },
  AttributesToGet: 'List',
  ReturnConsumedCapacity: 'String',
  Select: 'String',
  Limit: 'Integer',
  Segment: 'Integer',
  TotalSegments: 'Integer',
}

exports.validations = {
  ReturnConsumedCapacity: {
    enum: ['TOTAL', 'NONE']
  },
  AttributesToGet: {
    lengthGreaterThanOrEqual: 1,
    lengthLessThanOrEqual: 255,
  },
  TableName: {
    required: true,
    tableName: true,
    regex: '[a-zA-Z0-9_.-]+',
    lengthGreaterThanOrEqual: 3,
    lengthLessThanOrEqual: 255,
  },
  Key: {
    notNull: true,
  },
}

exports.custom = function(data) {
}

