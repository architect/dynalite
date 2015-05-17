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
                },
              },
            },
          },
        },
        Create: {
          type: 'Structure',
          children: {
            Projection: {
              type: 'Structure',
              notNull: true,
              children: {
                ProjectionType: {
                  type: 'String',
                  enum: ['ALL', 'INCLUDE', 'KEYS_ONLY'],
                },
                NonKeyAttributes: {
                  type: 'List',
                  lengthGreaterThanOrEqual: 1,
                  children: 'String',
                },
              },
            },
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
                },
              },
            },
            KeySchema: {
              type: 'List',
              notNull: true,
              lengthGreaterThanOrEqual: 1,
              lengthLessThanOrEqual: 2,
              children: {
                type: 'Structure',
                children: {
                  AttributeName: {
                    type: 'String',
                    notNull: true,
                  },
                  KeyType: {
                    type: 'String',
                    notNull: true,
                  },
                },
              },
            },
          },
        },
        Delete: {
          type: 'Structure',
          children: {
            IndexName: {
              type: 'String',
              notNull: true,
              regex: '[a-zA-Z0-9_.-]+',
              lengthGreaterThanOrEqual: 3,
              lengthLessThanOrEqual: 255,
            },
          },
        },
      },
    },
  },
}

exports.custom = function(data) {

  if (!data.ProvisionedThroughput && !data.UpdateStreamEnabled &&
      (!data.GlobalSecondaryIndexUpdates || !data.GlobalSecondaryIndexUpdates.length)) {
    return 'At least one of ProvisionedThroughput, UpdateStreamEnabled or GlobalSecondaryIndexUpdates is required'
  }

  if (data.ProvisionedThroughput) {
    if (data.ProvisionedThroughput.ReadCapacityUnits > 1000000000000)
      return 'Given value ' + data.ProvisionedThroughput.ReadCapacityUnits + ' for ReadCapacityUnits is out of bounds'
    if (data.ProvisionedThroughput.WriteCapacityUnits > 1000000000000)
      return 'Given value ' + data.ProvisionedThroughput.WriteCapacityUnits + ' for WriteCapacityUnits is out of bounds'
  }

  if (data.GlobalSecondaryIndexUpdates) {
    var indexNames = Object.create(null)
    for (var i = 0; i < data.GlobalSecondaryIndexUpdates.length; i++) {
      var update = data.GlobalSecondaryIndexUpdates[i]
      if (!update.Update && !update.Create && !update.Delete) {
        return 'One or more parameter values were invalid: ' +
          'One of GlobalSecondaryIndexUpdate.Update, GlobalSecondaryIndexUpdate.Create, ' +
          'GlobalSecondaryIndexUpdate.Delete must not be null'
      }
      var indexName = (update.Update && update.Update.IndexName) ||
        (update.Create && update.Create.IndexName) ||
        (update.Delete && update.Delete.IndexName)
      if (indexNames[indexName]) {
        return 'One or more parameter values were invalid: ' +
          'Only one global secondary index update per index is allowed simultaneously. Index: ' + indexName
      }
      indexNames[indexName] = true
      if (update.Update) {
        if (update.Update.ProvisionedThroughput.ReadCapacityUnits > 1000000000000)
          return 'Given value ' + data.GlobalSecondaryIndexUpdates[i].Update.ProvisionedThroughput.ReadCapacityUnits + ' for ReadCapacityUnits is out of bounds for index ' + data.GlobalSecondaryIndexUpdates[i].Update.IndexName
        if (update.Update.ProvisionedThroughput.WriteCapacityUnits > 1000000000000)
          return 'Given value ' + data.GlobalSecondaryIndexUpdates[i].Update.ProvisionedThroughput.WriteCapacityUnits + ' for WriteCapacityUnits is out of bounds for index ' + data.GlobalSecondaryIndexUpdates[i].Update.IndexName
      }
    }
  }
}

