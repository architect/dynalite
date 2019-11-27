exports.types = {
  ResourceArn: {
    type: 'String',
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
        'dynamodb:ListTagsOfResource on resource: ' + (data.ResourceArn || '*'),
    }
    throw accessDeniedError
  }

  if (!/^arn:aws:dynamodb:.+:\d+:table\/[^/]{2}[^/]+$/.test(data.ResourceArn)) {
    return 'Invalid TableArn: Invalid ResourceArn provided as input ' + data.ResourceArn
  }
}
