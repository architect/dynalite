exports.types = {
  TableName: {
    type: 'String',
    required: true,
    tableName: true,
    regex: '[a-zA-Z0-9_.-]+',
  },
  TimeToLiveSpecification: {
    type: 'FieldStruct<TimeToLiveSpecification>',
    children: {
      AttributeName: {
        type: 'String',
        required: true,
        notNull: true,
      },
      Enabled: {
        type: 'Boolean',
        required: true,
        notNull: true,
      },
    },
  },
}


exports.custom = function(data) {
  if (data.TimeToLiveSpecification.AttributeName === '') {
    return 'TimeToLiveSpecification.AttributeName must be non empty';
  }
}
