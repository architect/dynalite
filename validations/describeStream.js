exports.types = {
  StreamArn: {
    required: true,
    type: 'String',
    regex: '[a-zA-Z0-9\-\.:/_]+',
    lengthGreaterThanOrEqual: 37,
    lengthLessThanOrEqual: 1024,
  },
  Limit: {
    type: 'Integer',
    greaterThanOrEqual: 1,
    lessThanOrEqual: 100,
  },
  ExclusiveStartShardId: {
    type: 'String',
    regex: '[a-zA-Z0-9\-]+',
    lengthGreaterThanOrEqual: 28,
    lengthLessThanOrEqual: 65,
  },
}
