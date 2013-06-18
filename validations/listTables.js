exports.types = {
  ExclusiveStartTableName: 'String',
  Limit: 'Integer',
}

exports.validations = {
  Limit: {
    greaterThanOrEqual: 1,
    lessThanOrEqual: 100,
  },
  ExclusiveStartTableName: {
    regex: '[a-zA-Z0-9_.-]+',
    lengthGreaterThanOrEqual: 3,
    lengthLessThanOrEqual: 255,
  },
}

