exports.types = {
  ResourceArn: {
    type: 'String',
  },
  TagKeys: {
    type: 'List',
    children: 'String',
  },
}

exports.custom = function(data) {
  if (data.ResourceArn == null) {
    return 'Invalid TableArn'
  }
}
