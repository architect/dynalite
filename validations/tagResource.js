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

exports.custom = function(data) {
  if (data.ResourceArn == null) {
    return 'Invalid TableArn'
  }
}
