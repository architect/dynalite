exports.types = {
  ResourceArn: {
    type: 'String',
  },
  Tags: {
    type: 'List',
    children: {
      type: 'ValueStruct<Tag>',
      children: {
        Key: 'String',
        Value: 'String',
      },
    },
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
        'dynamodb:TagResource on resource: ' + (data.ResourceArn || '*'),
    }
    throw accessDeniedError
  }

  if (data.Tags == null) {
    return '1 validation error detected: Value null at \'tags\' failed to satisfy constraint: Member must not be null'
  }

  if (!/^arn:aws:dynamodb:.+:\d+:table\/[^/]{2}[^/]+$/.test(data.ResourceArn)) {
    return 'Invalid TableArn: Invalid ResourceArn provided as input ' + data.ResourceArn
  }

  if (!data.Tags.length) {
    return 'Atleast one Tag needs to be provided as Input.'
  }
}
