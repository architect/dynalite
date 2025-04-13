const test = require('tape')
// const async = require('async'); // Likely not needed directly in tests
const helpers = require('./helpers')

const target = 'DeleteItem'
// Bind helper functions
// const request = helpers.request; // Marked as unused by linter
// const opts = helpers.opts.bind(null, target); // Marked as unused by linter
const assertType = helpers.assertType.bind(null, target)
// const assertValidation = helpers.assertValidation.bind(null, target); // Marked as unused by linter
// const assertConditional = helpers.assertConditional.bind(null, target); // Marked as unused by linter

test('deleteItem', (t) => {
  t.test('serializations', (st) => {

    st.test('should return SerializationException when TableName is not a string', (sst) => {
      assertType('TableName', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when Key is not a map', (sst) => {
      assertType('Key', 'Map<AttributeValue>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when Key.Attr is not an attr struct', (sst) => {
      // Timeout removed
      assertType('Key.Attr', 'AttrStruct<ValueStruct>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when Expected is not a map', (sst) => {
      assertType('Expected', 'Map<ExpectedAttributeValue>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when Expected.Attr is not a struct', (sst) => {
      assertType('Expected.Attr', 'ValueStruct<ExpectedAttributeValue>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when Expected.Attr.Exists is not a boolean', (sst) => {
      assertType('Expected.Attr.Exists', 'Boolean', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when Expected.Attr.Value is not an attr struct', (sst) => {
      // Timeout removed
      assertType('Expected.Attr.Value', 'AttrStruct<FieldStruct>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when ReturnConsumedCapacity is not a string', (sst) => {
      assertType('ReturnConsumedCapacity', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when ReturnItemCollectionMetrics is not a string', (sst) => {
      assertType('ReturnItemCollectionMetrics', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when ReturnValues is not a string', (sst) => {
      assertType('ReturnValues', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when ConditionExpression is not a string', (sst) => {
      assertType('ConditionExpression', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when ExpressionAttributeValues is not a map', (sst) => {
      assertType('ExpressionAttributeValues', 'Map<AttributeValue>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when ExpressionAttributeValues.Attr is not an attr struct', (sst) => {
      // Timeout removed
      assertType('ExpressionAttributeValues.Attr', 'AttrStruct<ValueStruct>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when ExpressionAttributeNames is not a map', (sst) => {
      assertType('ExpressionAttributeNames', 'Map<java.lang.String>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when ExpressionAttributeNames.Attr is not a string', (sst) => {
      assertType('ExpressionAttributeNames.Attr', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.end() // End serializations tests
  })
  // Note: The original file only contained the 'serializations' describe block.
  // The 'validations' and 'functionality' blocks seem to be in deleteItem.part2.js etc.
  t.end() // End deleteItem tests
})
