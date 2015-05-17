exports.types = {
  AttributeDefinitions: {
    type: 'List',
    notNull: true,
    children: {
      type: 'Structure',
      children: {
        AttributeName: {
          type: 'String',
          notNull: true,
        },
        AttributeType: {
          type: 'String',
          notNull: true,
          enum: ['B', 'N', 'S'],
        },
      },
    },
  },
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
          enum: ['HASH', 'RANGE'],
        },
      },
    },
  },
  LocalSecondaryIndexes: {
    type: 'List',
    children: {
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
  },
  GlobalSecondaryIndexes: {
    type: 'List',
    children: {
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
  },
}

exports.custom = function(data) {

  if (data.ProvisionedThroughput.ReadCapacityUnits > 1000000000000)
    return 'Given value ' + data.ProvisionedThroughput.ReadCapacityUnits + ' for ReadCapacityUnits is out of bounds'
  if (data.ProvisionedThroughput.WriteCapacityUnits > 1000000000000)
    return 'Given value ' + data.ProvisionedThroughput.WriteCapacityUnits + ' for WriteCapacityUnits is out of bounds'

  var defns = data.AttributeDefinitions.map(function(key) { return key.AttributeName })
  var keys = data.KeySchema.map(function(key) { return key.AttributeName })

  if (keys.length == 2) {
    if (keys.some(function(key) { return !~defns.indexOf(key) }) ||
        // bizarre case - not sure what the general form of it is
        keys[0] == keys[1] && defns.length == 1)
      return 'Invalid KeySchema: Some index key attribute have no definition'
  }

  if (data.KeySchema.length == 1) {
    if (keys.some(function(key) { return !~defns.indexOf(key) }))
      return 'One or more parameter values were invalid: Some index key attributes are not defined in ' +
        'AttributeDefinitions. Keys: [' + keys.join(', ') + '], AttributeDefinitions: [' + defns.join(', ') + ']'
  }

  if (keys[0] == keys[1])
    return 'Both the Hash Key and the Range Key element in the KeySchema have the same name'

  if (data.KeySchema[0].KeyType != 'HASH')
    return 'Invalid KeySchema: The first KeySchemaElement is not a HASH key type'

  if (data.KeySchema[1] && data.KeySchema[1].KeyType != 'RANGE')
    return 'Invalid KeySchema: The second KeySchemaElement is not a RANGE key type'

  // TODO: Clean this up!
  if (!data.LocalSecondaryIndexes && !data.GlobalSecondaryIndexes && data.KeySchema.length != data.AttributeDefinitions.length)
    return 'One or more parameter values were invalid: Number of attributes in KeySchema does not ' +
      'exactly match number of attributes defined in AttributeDefinitions'

  var indexNames = Object.create(null), indexKeys, tableHash, i, indexName, indexHash, projectionType

  if (data.LocalSecondaryIndexes) {
    tableHash = data.KeySchema[0].AttributeName

    if (!data.LocalSecondaryIndexes.length)
      return 'One or more parameter values were invalid: List of LocalSecondaryIndexes is empty'

    if (data.KeySchema.length != 2)
      return 'One or more parameter values were invalid: Table KeySchema does not have a range key, ' +
        'which is required when specifying a LocalSecondaryIndex'

    for (i = 0; i < data.LocalSecondaryIndexes.length; i++) {
      indexName = data.LocalSecondaryIndexes[i].IndexName
      indexKeys = data.LocalSecondaryIndexes[i].KeySchema.map(function(key) { return key.AttributeName }) // eslint-disable-line no-loop-func
      if (indexKeys.some(function(key) { return !~defns.indexOf(key) })) // eslint-disable-line no-loop-func
        return 'One or more parameter values were invalid: ' +
          'Some index key attributes are not defined in AttributeDefinitions. ' +
          'Keys: [' + indexKeys.join(', ') + '], AttributeDefinitions: [' + defns.join(', ') + ']'

      if (data.LocalSecondaryIndexes[i].KeySchema[1] &&
          data.LocalSecondaryIndexes[i].KeySchema[0].AttributeName ==
          data.LocalSecondaryIndexes[i].KeySchema[1].AttributeName)
        return 'Both the Hash Key and the Range Key element in the KeySchema have the same name'

      if (data.LocalSecondaryIndexes[i].KeySchema[0].KeyType != 'HASH')
        return 'Invalid KeySchema: The first KeySchemaElement is not a HASH key type'
      if (data.LocalSecondaryIndexes[i].KeySchema[1] &&
          data.LocalSecondaryIndexes[i].KeySchema[1].KeyType != 'RANGE')
        return 'Invalid KeySchema: The second KeySchemaElement is not a RANGE key type'

      if (data.LocalSecondaryIndexes[i].KeySchema.length != 2)
        return 'One or more parameter values were invalid: Index KeySchema does not have a range key for index: ' +
          data.LocalSecondaryIndexes[i].IndexName

      indexHash = data.LocalSecondaryIndexes[i].KeySchema[0].AttributeName
      if (indexHash != tableHash)
        return 'One or more parameter values were invalid: ' +
          'Index KeySchema does not have the same leading hash key as table KeySchema for index: ' +
          data.LocalSecondaryIndexes[i].IndexName + '. index hash key: ' + indexHash +
          ', table hash key: ' + tableHash

      if (data.LocalSecondaryIndexes[i].Projection.ProjectionType == null)
        return 'One or more parameter values were invalid: Unknown ProjectionType: null'

      projectionType = data.LocalSecondaryIndexes[i].Projection.ProjectionType
      if (data.LocalSecondaryIndexes[i].Projection.NonKeyAttributes && projectionType != 'INCLUDE')
        return 'One or more parameter values were invalid: ' +
          'ProjectionType is ' + projectionType + ', but NonKeyAttributes is specified'

      if (indexNames[indexName])
        return 'One or more parameter values were invalid: Duplicate index name: ' + indexName
      indexNames[indexName] = true
    }

    if (data.LocalSecondaryIndexes.length > 5)
      return 'One or more parameter values were invalid: LocalSecondaryIndex count exceeds the per-table limit of 5'
  }

  if (data.GlobalSecondaryIndexes) {
    if (!data.GlobalSecondaryIndexes.length)
      return 'One or more parameter values were invalid: List of GlobalSecondaryIndexes is empty'

    for (i = 0; i < data.GlobalSecondaryIndexes.length; i++) {
      indexName = data.GlobalSecondaryIndexes[i].IndexName
      indexKeys = data.GlobalSecondaryIndexes[i].KeySchema.map(function(key) { return key.AttributeName }) // eslint-disable-line no-loop-func
      if (indexKeys.some(function(key) { return !~defns.indexOf(key) })) // eslint-disable-line no-loop-func
        return 'One or more parameter values were invalid: ' +
          'Some index key attributes are not defined in AttributeDefinitions. ' +
          'Keys: [' + indexKeys.join(', ') + '], AttributeDefinitions: [' + defns.join(', ') + ']'

      if (data.GlobalSecondaryIndexes[i].KeySchema[1] &&
          data.GlobalSecondaryIndexes[i].KeySchema[0].AttributeName ==
          data.GlobalSecondaryIndexes[i].KeySchema[1].AttributeName)
        return 'Both the Hash Key and the Range Key element in the KeySchema have the same name'

      if (data.GlobalSecondaryIndexes[i].KeySchema[0].KeyType != 'HASH')
        return 'Invalid KeySchema: The first KeySchemaElement is not a HASH key type'
      if (data.GlobalSecondaryIndexes[i].KeySchema[1] &&
          data.GlobalSecondaryIndexes[i].KeySchema[1].KeyType != 'RANGE')
        return 'Invalid KeySchema: The second KeySchemaElement is not a RANGE key type'

      if (data.GlobalSecondaryIndexes[i].Projection.ProjectionType == null)
        return 'One or more parameter values were invalid: Unknown ProjectionType: null'

      projectionType = data.GlobalSecondaryIndexes[i].Projection.ProjectionType
      if (data.GlobalSecondaryIndexes[i].Projection.NonKeyAttributes && projectionType != 'INCLUDE')
        return 'One or more parameter values were invalid: ' +
          'ProjectionType is ' + projectionType + ', but NonKeyAttributes is specified'

      if (indexNames[indexName])
        return 'One or more parameter values were invalid: Duplicate index name: ' + indexName
      indexNames[indexName] = true
    }

    if (data.GlobalSecondaryIndexes.length > 5)
      return 'One or more parameter values were invalid: GlobalSecondaryIndex count exceeds the per-table limit of 5'
  }
}

