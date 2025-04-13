const test = require('tape')
// const async = require('async') // Keep async if helpers potentially use it - Removed as unused
const helpers = require('./helpers')

const target = 'BatchGetItem'
// Bind helper functions for convenience
const assertType = helpers.assertType.bind(null, target)

test('batchGetItem', (t) => {

  t.test('serializations', (st) => {

    st.test('should return SerializationException when RequestItems is not a map', (sst) => {
      assertType('RequestItems', 'Map<KeysAndAttributes>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when RequestItems.Attr is not a struct', (sst) => {
      assertType('RequestItems.Attr', 'ValueStruct<KeysAndAttributes>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when RequestItems.Attr.Keys is not a list', (sst) => {
      assertType('RequestItems.Attr.Keys', 'List', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when RequestItems.Attr.Keys.0 is not a map', (sst) => {
      assertType('RequestItems.Attr.Keys.0', 'ParameterizedMap', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when RequestItems.Attr.Keys.0.Attr is not an attr struct', (sst) => {
      // Timeout removed
      assertType('RequestItems.Attr.Keys.0.Attr', 'AttrStruct<ValueStruct>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when RequestItems.Attr.AttributesToGet is not a list', (sst) => {
      assertType('RequestItems.Attr.AttributesToGet', 'List', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when RequestItems.Attr.ConsistentRead is not a boolean', (sst) => {
      assertType('RequestItems.Attr.ConsistentRead', 'Boolean', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when RequestItems.Attr.ExpressionAttributeNames is not a map', (sst) => {
      assertType('RequestItems.Attr.ExpressionAttributeNames', 'Map<java.lang.String>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when RequestItems.Attr.ExpressionAttributeNames.Attr is not a string', (sst) => {
      assertType('RequestItems.Attr.ExpressionAttributeNames.Attr', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when RequestItems.Attr.ProjectionExpression is not a string', (sst) => {
      assertType('RequestItems.Attr.ProjectionExpression', 'String', (err) => {
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

    st.end() // End serializations tests
  })

  t.end() // End batchGetItem tests
})
