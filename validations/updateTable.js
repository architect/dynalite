exports.types = {
  TableName: {
    type: 'String',
    required: true,
    tableName: true,
    regex: '[a-zA-Z0-9_.-]+',
  },
  ProvisionedThroughput: {
    type: 'Structure',
    notNull: true,
    children: {
      WriteCapacityUnits: {
        type: 'Long',
        notNull: true,
        greaterThanOrEqual: 1,
      },
      ReadCapacityUnits: {
        type: 'Long',
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

