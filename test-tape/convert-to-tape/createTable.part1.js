const test = require('tape')
// const should = require('should'); // Likely unused
const helpers = require('./helpers')

const target = 'CreateTable'
// Bind helper functions - anticipating unused ones
// const request = helpers.request; // Unused in part1
// const randomName = helpers.randomName; // Unused in part1
// const opts = helpers.opts.bind(null, target); // Unused in part1
const assertType = helpers.assertType.bind(null, target)
// const assertValidation = helpers.assertValidation.bind(null, target); // Unused in part1

test('createTable', (t) => {
  t.test('serializations', (st) => {

    st.test('should return SerializationException when TableName is not a string', (sst) => {
      assertType('TableName', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when AttributeDefinitions is not a list', (sst) => {
      assertType('AttributeDefinitions', 'List', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when KeySchema is not a list', (sst) => {
      assertType('KeySchema', 'List', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when LocalSecondaryIndexes is not a list', (sst) => {
      assertType('LocalSecondaryIndexes', 'List', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexes is not a list', (sst) => {
      assertType('GlobalSecondaryIndexes', 'List', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when ProvisionedThroughput is not a struct', (sst) => {
      assertType('ProvisionedThroughput', 'FieldStruct<ProvisionedThroughput>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when ProvisionedThroughput.WriteCapacityUnits is not a long', (sst) => {
      assertType('ProvisionedThroughput.WriteCapacityUnits', 'Long', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when ProvisionedThroughput.ReadCapacityUnits is not a long', (sst) => {
      assertType('ProvisionedThroughput.ReadCapacityUnits', 'Long', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when KeySchema.0 is not a struct', (sst) => {
      assertType('KeySchema.0', 'ValueStruct<KeySchemaElement>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when KeySchema.0.KeyType is not a string', (sst) => {
      assertType('KeySchema.0.KeyType', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when KeySchema.0.AttributeName is not a string', (sst) => {
      assertType('KeySchema.0.AttributeName', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when AttributeDefinitions.0 is not a struct', (sst) => {
      assertType('AttributeDefinitions.0', 'ValueStruct<AttributeDefinition>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when AttributeDefinitions.0.AttributeName is not a string', (sst) => {
      assertType('AttributeDefinitions.0.AttributeName', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when AttributeDefinitions.0.AttributeType is not a string', (sst) => {
      assertType('AttributeDefinitions.0.AttributeType', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when LocalSecondaryIndexes.0 is not a struct', (sst) => {
      assertType('LocalSecondaryIndexes.0', 'ValueStruct<LocalSecondaryIndex>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when LocalSecondaryIndexes.0.IndexName is not a string', (sst) => {
      assertType('LocalSecondaryIndexes.0.IndexName', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when LocalSecondaryIndexes.0.KeySchema is not a list', (sst) => {
      assertType('LocalSecondaryIndexes.0.KeySchema', 'List', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when LocalSecondaryIndexes.0.Projection is not a struct', (sst) => {
      assertType('LocalSecondaryIndexes.0.Projection', 'FieldStruct<Projection>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when LocalSecondaryIndexes.0.KeySchema.0 is not a struct', (sst) => {
      assertType('LocalSecondaryIndexes.0.KeySchema.0', 'ValueStruct<KeySchemaElement>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when LocalSecondaryIndexes.0.KeySchema.0.AttributeName is not a string', (sst) => {
      assertType('LocalSecondaryIndexes.0.KeySchema.0.AttributeName', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when LocalSecondaryIndexes.0.KeySchema.0.KeyType is not a string', (sst) => {
      assertType('LocalSecondaryIndexes.0.KeySchema.0.KeyType', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when LocalSecondaryIndexes.0.Projection.NonKeyAttributes is not a list', (sst) => {
      assertType('LocalSecondaryIndexes.0.Projection.NonKeyAttributes', 'List', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when LocalSecondaryIndexes.0.Projection.ProjectionType is not a string', (sst) => {
      assertType('LocalSecondaryIndexes.0.Projection.ProjectionType', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when LocalSecondaryIndexes.0.Projection.NonKeyAttributes.0 is not a string', (sst) => {
      assertType('LocalSecondaryIndexes.0.Projection.NonKeyAttributes.0', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexes.0 is not a struct', (sst) => {
      assertType('GlobalSecondaryIndexes.0', 'ValueStruct<GlobalSecondaryIndex>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexes.0.IndexName is not a string', (sst) => {
      assertType('GlobalSecondaryIndexes.0.IndexName', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexes.0.KeySchema is not a list', (sst) => {
      assertType('GlobalSecondaryIndexes.0.KeySchema', 'List', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexes.0.Projection is not a struct', (sst) => {
      assertType('GlobalSecondaryIndexes.0.Projection', 'FieldStruct<Projection>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexes.0.KeySchema.0 is not a struct', (sst) => {
      assertType('GlobalSecondaryIndexes.0.KeySchema.0', 'ValueStruct<KeySchemaElement>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexes.0.KeySchema.0.AttributeName is not a string', (sst) => {
      assertType('GlobalSecondaryIndexes.0.KeySchema.0.AttributeName', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexes.0.KeySchema.0.KeyType is not a string', (sst) => {
      assertType('GlobalSecondaryIndexes.0.KeySchema.0.KeyType', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexes.0.Projection.NonKeyAttributes is not a list', (sst) => {
      assertType('GlobalSecondaryIndexes.0.Projection.NonKeyAttributes', 'List', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexes.0.Projection.ProjectionType is not a string', (sst) => {
      assertType('GlobalSecondaryIndexes.0.Projection.ProjectionType', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexes.0.Projection.NonKeyAttributes.0 is not a string', (sst) => {
      assertType('GlobalSecondaryIndexes.0.Projection.NonKeyAttributes.0', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexes.0.ProvisionedThroughput is not a struct', (sst) => {
      assertType('GlobalSecondaryIndexes.0.ProvisionedThroughput', 'FieldStruct<ProvisionedThroughput>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexes.0.ProvisionedThroughput.WriteCapacityUnits is not a long', (sst) => {
      assertType('GlobalSecondaryIndexes.0.ProvisionedThroughput.WriteCapacityUnits', 'Long', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexes.0.ProvisionedThroughput.ReadCapacityUnits is not a long', (sst) => {
      assertType('GlobalSecondaryIndexes.0.ProvisionedThroughput.ReadCapacityUnits', 'Long', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when BillingMode is not a string', (sst) => {
      assertType('BillingMode', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.end() // End serializations tests
  })
  // Note: The original file only contained the 'serializations' describe block.
  // The 'validations' and 'functionality' blocks seem to be in createTable.part2.js etc.
  t.end() // End createTable tests
})
