exports.types = {
  TableName: {
    type: 'String',
    required: true,
    tableName: true,
    regex: '[a-zA-Z0-9_.-]+',
  },
  ProvisionedThroughput: {
    type: 'FieldStruct<ProvisionedThroughput>',
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
  BillingMode: {
    type: 'String',
    enum: ['PROVISIONED', 'PAY_PER_REQUEST'],
  },
  GlobalSecondaryIndexUpdates: {
    type: 'List',
    children: {
      type: 'ValueStruct<GlobalSecondaryIndexUpdate>',
      children: {
        Update: {
          type: 'FieldStruct<UpdateGlobalSecondaryIndexAction>',
          children: {
            IndexName: {
              type: 'String',
              notNull: true,
              regex: '[a-zA-Z0-9_.-]+',
              lengthGreaterThanOrEqual: 3,
              lengthLessThanOrEqual: 255,
            },
            ProvisionedThroughput: {
              type: 'FieldStruct<ProvisionedThroughput>',
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
          type: 'FieldStruct<CreateGlobalSecondaryIndexAction>',
          children: {
            Projection: {
              type: 'FieldStruct<Projection>',
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
              type: 'FieldStruct<ProvisionedThroughput>',
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
                type: 'ValueStruct<KeySchemaElement>',
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
          type: 'FieldStruct<DeleteGlobalSecondaryIndexAction>',
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

  if (!data.ProvisionedThroughput && !data.BillingMode && !data.UpdateStreamEnabled &&
      (!data.GlobalSecondaryIndexUpdates || !data.GlobalSecondaryIndexUpdates.length) && !data.SSESpecification) {
    return 'At least one of ProvisionedThroughput, BillingMode, UpdateStreamEnabled, GlobalSecondaryIndexUpdates or SSESpecification or ReplicaUpdates is required'
  }

  if (data.BillingMode == 'PROVISIONED' && !data.ProvisionedThroughput) {
    return 'One or more parameter values were invalid: ' +
      'ProvisionedThroughput must be specified when BillingMode is PROVISIONED'
  }

  if (data.ProvisionedThroughput) {
    if (data.ProvisionedThroughput.ReadCapacityUnits > 1000000000000)
      return 'Given value ' + data.ProvisionedThroughput.ReadCapacityUnits + ' for ReadCapacityUnits is out of bounds'
    if (data.ProvisionedThroughput.WriteCapacityUnits > 1000000000000)
      return 'Given value ' + data.ProvisionedThroughput.WriteCapacityUnits + ' for WriteCapacityUnits is out of bounds'
  }

  if (data.GlobalSecondaryIndexUpdates) {
    var length = data.GlobalSecondaryIndexUpdates.length
    var indexNames = Object.create(null)
    for (var i = 0; i < length; i++) {
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
    }
  }
}
