const test = require('tape')
const helpers = require('./helpers')

const target = 'UpdateTable'
// Bind helper functions - anticipating unused ones
// const request = helpers.request; // Unused in part1
// const opts = helpers.opts.bind(null, target); // Unused in part1
const assertType = helpers.assertType.bind(null, target)
// const assertValidation = helpers.assertValidation.bind(null, target); // Unused in part1
// const assertNotFound = helpers.assertNotFound.bind(null, target); // Unused in part1

test('updateTable', (t) => {
  t.test('serializations', (st) => {

    st.test('should return SerializationException when TableName is not a string', (sst) => {
      assertType('TableName', 'String', (err) => {
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

    st.test('should return SerializationException when GlobalSecondaryIndexUpdates is not a list', (sst) => {
      assertType('GlobalSecondaryIndexUpdates', 'List', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexUpdates.0 is not a struct', (sst) => {
      assertType('GlobalSecondaryIndexUpdates.0', 'ValueStruct<GlobalSecondaryIndexUpdate>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexUpdates.0.Update is not a struct', (sst) => {
      assertType('GlobalSecondaryIndexUpdates.0.Update', 'FieldStruct<UpdateGlobalSecondaryIndexAction>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexUpdates.0.Update.IndexName is not a string', (sst) => {
      assertType('GlobalSecondaryIndexUpdates.0.Update.IndexName', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexUpdates.0.Update.ProvisionedThroughput is not a struct', (sst) => {
      assertType('GlobalSecondaryIndexUpdates.0.Update.ProvisionedThroughput', 'FieldStruct<ProvisionedThroughput>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexUpdates.0.Update.ProvisionedThroughput.WriteCapacityUnits is not a long', (sst) => {
      assertType('GlobalSecondaryIndexUpdates.0.Update.ProvisionedThroughput.WriteCapacityUnits', 'Long', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexUpdates.0.Update.ProvisionedThroughput.ReadCapacityUnits is not a long', (sst) => {
      assertType('GlobalSecondaryIndexUpdates.0.Update.ProvisionedThroughput.ReadCapacityUnits', 'Long', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create is not a struct', (sst) => {
      assertType('GlobalSecondaryIndexUpdates.0.Create', 'FieldStruct<CreateGlobalSecondaryIndexAction>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.IndexName is not a string', (sst) => {
      assertType('GlobalSecondaryIndexUpdates.0.Create.IndexName', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.ProvisionedThroughput is not a struct', (sst) => {
      assertType('GlobalSecondaryIndexUpdates.0.Create.ProvisionedThroughput', 'FieldStruct<ProvisionedThroughput>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.ProvisionedThroughput.WriteCapacityUnits is not a long', (sst) => {
      assertType('GlobalSecondaryIndexUpdates.0.Create.ProvisionedThroughput.WriteCapacityUnits', 'Long', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.ProvisionedThroughput.ReadCapacityUnits is not a long', (sst) => {
      assertType('GlobalSecondaryIndexUpdates.0.Create.ProvisionedThroughput.ReadCapacityUnits', 'Long', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.KeySchema is not a list', (sst) => {
      assertType('GlobalSecondaryIndexUpdates.0.Create.KeySchema', 'List', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.KeySchema.0 is not a struct', (sst) => {
      assertType('GlobalSecondaryIndexUpdates.0.Create.KeySchema.0', 'ValueStruct<KeySchemaElement>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.KeySchema.0.AttributeName is not a string', (sst) => {
      assertType('GlobalSecondaryIndexUpdates.0.Create.KeySchema.0.AttributeName', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.KeySchema.0.KeyType is not a string', (sst) => {
      assertType('GlobalSecondaryIndexUpdates.0.Create.KeySchema.0.KeyType', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.Projection is not a struct', (sst) => {
      assertType('GlobalSecondaryIndexUpdates.0.Create.Projection', 'FieldStruct<Projection>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.Projection.NonKeyAttributes is not a list', (sst) => {
      assertType('GlobalSecondaryIndexUpdates.0.Create.Projection.NonKeyAttributes', 'List', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.Projection.ProjectionType is not a string', (sst) => {
      assertType('GlobalSecondaryIndexUpdates.0.Create.Projection.ProjectionType', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.Projection.NonKeyAttributes.0 is not a string', (sst) => {
      assertType('GlobalSecondaryIndexUpdates.0.Create.Projection.NonKeyAttributes.0', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexUpdates.0.Delete is not a struct', (sst) => {
      assertType('GlobalSecondaryIndexUpdates.0.Delete', 'FieldStruct<DeleteGlobalSecondaryIndexAction>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when GlobalSecondaryIndexUpdates.0.Delete.IndexName is not a string', (sst) => {
      assertType('GlobalSecondaryIndexUpdates.0.Delete.IndexName', 'String', (err) => {
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
  // The 'validations' and 'functionality' blocks seem to be in updateTable.part2.js etc.
  t.end() // End updateTable tests
})
