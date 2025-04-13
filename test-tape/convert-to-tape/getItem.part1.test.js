const test = require('tape')
// const async = require('async') // Keep async if helpers potentially use it - Removed as unused
const helpers = require('./helpers')

const target = 'GetItem'
// Bind helper functions for convenience
const assertType = helpers.assertType.bind(null, target)

test('getItem', (t) => {

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
      // Timeout removed - Tape does not handle it the same way.
      assertType('Key.Attr', 'AttrStruct<ValueStruct>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when AttributesToGet is not a list', (sst) => {
      assertType('AttributesToGet', 'List', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when ConsistentRead is not a boolean', (sst) => {
      assertType('ConsistentRead', 'Boolean', (err) => {
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

    st.test('should return SerializationException when ProjectionExpression is not a string', (sst) => {
      assertType('ProjectionExpression', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.end() // End of 'serializations' tests
  })

  t.end() // End of 'getItem' tests
})
