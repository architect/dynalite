exports.types = {
  ResourceArns: {
    type: 'String',
    required: true,
    tableName: false,
    regex: 'arn:(aws[a-zA-Z-]*)?:dynamodb:[a-z]{2}((-gov)|(-iso(b?)))?-[a-z]+-\\d{1}:\\d{12}:table:[a-zA-Z0-9-_]+(:(\\$LATEST|[a-zA-Z0-9-_]+))?',
  },
}
