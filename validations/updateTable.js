exports.types = {
  TableName: {
    type: 'String',
    required: true,
    tableName: true,
    regex: '[a-zA-Z0-9_.-]+',
  },
  ProvisionedThroughput: {
    type: 'Structure',
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
  GlobalSecondaryIndexUpdates: {
    type: 'List',
    children: {
      type: 'Structure',
      children: {
        Update: {
          type: 'Structure',
          notNull: true,
          children: {
            IndexName: {
              type: 'String',
              notNull: true,
              regex: '[a-zA-Z0-9_.-]+',
              lengthGreaterThanOrEqual: 3,
              lengthLessThanOrEqual: 255,
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
                }
              },
            },
          }
        },
      },
    },
  },
}

exports.custom = function(data) {

  if (!data.ProvisionedThroughput && (!data.GlobalSecondaryIndexUpdates || !data.GlobalSecondaryIndexUpdates.length))
    return 'At least one of ProvisionedThroughput or GlobalSecondaryIndexUpdates is required'

  if (data.ProvisionedThroughput) {
    if (data.ProvisionedThroughput.ReadCapacityUnits > 1000000000000)
      return 'Given value ' + data.ProvisionedThroughput.ReadCapacityUnits + ' for ReadCapacityUnits is out of bounds'
    if (data.ProvisionedThroughput.WriteCapacityUnits > 1000000000000)
      return 'Given value ' + data.ProvisionedThroughput.WriteCapacityUnits + ' for WriteCapacityUnits is out of bounds'
  }

  if (data.GlobalSecondaryIndexUpdates) {
    for (var i = 0; i < data.GlobalSecondaryIndexUpdates.length; i++) {
      if (data.GlobalSecondaryIndexUpdates[i].Update.ProvisionedThroughput.ReadCapacityUnits > 1000000000000)
        return 'Given value ' + data.GlobalSecondaryIndexUpdates[i].Update.ProvisionedThroughput.ReadCapacityUnits + ' for ReadCapacityUnits is out of bounds for index ' + data.GlobalSecondaryIndexUpdates[i].Update.IndexName
      if (data.GlobalSecondaryIndexUpdates[i].Update.ProvisionedThroughput.WriteCapacityUnits > 1000000000000)
        return 'Given value ' + data.GlobalSecondaryIndexUpdates[i].Update.ProvisionedThroughput.WriteCapacityUnits + ' for WriteCapacityUnits is out of bounds for index ' + data.GlobalSecondaryIndexUpdates[i].Update.IndexName
    }
  }
}

