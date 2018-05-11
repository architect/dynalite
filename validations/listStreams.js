exports.types = {
  Limit: {
    type: 'Integer',
    greaterThanOrEqual: 1,
    lessThanOrEqual: 100,
  },
  ExclusiveStartStreamArn: {
    type: 'String',
    regex: '[a-zA-Z0-9\-\.:/]+',
    lengthGreaterThanOrEqual: 37,
    lengthLessThanOrEqual: 1024,
  },
  TableName: {
    type: 'String',
    tableName: true,
    regex: '[a-zA-Z0-9_.-]+',
  },
}
