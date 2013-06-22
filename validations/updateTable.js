exports.types = {
  TableName: 'String',
  ProvisionedThroughput: {
    type: 'Structure',
    children: {
      WriteCapacityUnits: 'Long',
      ReadCapacityUnits: 'Long',
    },
  },
}

exports.validations = {
  TableName: {
    required: true,
    tableName: true,
    regex: '[a-zA-Z0-9_.-]+',
    lengthGreaterThanOrEqual: 3,
    lengthLessThanOrEqual: 255,
  },
  ProvisionedThroughput: {
    notNull: true,
    children: {
      WriteCapacityUnits: {
        notNull: true,
        greaterThanOrEqual: 1,
      },
      ReadCapacityUnits: {
        notNull: true,
        greaterThanOrEqual: 1,
      },
    },
  },
}

exports.custom = function(data) {

  if (data.ProvisionedThroughput.ReadCapacityUnits > 1000000000000)
    return 'Given value ' + data.ProvisionedThroughput.ReadCapacityUnits + ' for ReadCapacityUnits is out of bounds'
  if (data.ProvisionedThroughput.WriteCapacityUnits > 1000000000000)
    return 'Given value ' + data.ProvisionedThroughput.WriteCapacityUnits + ' for WriteCapacityUnits is out of bounds'

}

