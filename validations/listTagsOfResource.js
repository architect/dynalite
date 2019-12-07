exports.types = {
  ResourceArns: {
    type: 'String',
    required: false,
    tableName: false,
    regex: 'arn:aws:dynamodb:(.+):(.+):table/(.+)',
  },
}
