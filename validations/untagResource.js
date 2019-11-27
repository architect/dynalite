exports.types = {
  ResourceArn: {
    type: 'String',
  },
  TagKeys: {
    type: 'List',
    children: 'String',
  },
}

exports.custom = function(data, store) {
  if (data.ResourceArn == null) {
    return 'Invalid TableArn'
  }

  if (!/^.+:.+:.+:.+:.+:.+\/.+$/.test(data.ResourceArn)) {
    var username = 'dynalite'

    var accessDeniedError = new Error
    accessDeniedError.statusCode = 400
    accessDeniedError.body = {
      __type: 'com.amazon.coral.service#AccessDeniedException',
      Message: 'User: arn:aws:iam::' + store.tableDb.awsAccountId + ':' + username + ' is not authorized to perform: ' +
        'dynamodb:UntagResource on resource: ' + (data.ResourceArn || '*'),
    }
    throw accessDeniedError
  }

  if (data.TagKeys == null) {
    return '1 validation error detected: Value null at \'tagKeys\' failed to satisfy constraint: Member must not be null'
  }

  if (!/^arn:aws:dynamodb:.+:\d+:table\/[^/]{2}[^/]+$/.test(data.ResourceArn)) {
    return 'Invalid TableArn: Invalid ResourceArn provided as input ' + data.ResourceArn
  }

  if (!data.TagKeys.length) {
    return 'Atleast one Tag Key needs to be provided as Input.'
  }
}
