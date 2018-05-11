exports.types = {
  ShardIterator: {
    required: true,
    type: 'String',
    lengthGreaterThanOrEqual: 1,
    lengthLessThanOrEqual: 2048,
  },
  Limit: {
    type: 'Integer',
    greaterThanOrEqual: 1,
    lessThanOrEqual: 1000,
  },
}
