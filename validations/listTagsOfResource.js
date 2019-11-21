exports.types = {
  ResourceArns: {
    type: 'String',
    required: true,
    tableName: false,
    regex: 'arn:aws:dynamodb:(.+):(.+):table/(.+)',
  },
}
