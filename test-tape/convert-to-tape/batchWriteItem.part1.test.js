const test = require('tape')
// const async = require('async') // Keep async for now, might be needed by helpers
const helpers = require('./helpers')
// const db = require('../../db'); // Original require, likely not needed directly in tests

const target = 'BatchWriteItem'
// Bind helper functions for convenience
const assertType = helpers.assertType.bind(null, target)

test('batchWriteItem', (t) => {

  t.test('serializations', (st) => {

    st.test('should return SerializationException when RequestItems is not a map', (sst) => {
      assertType('RequestItems', 'Map<java.util.List<com.amazonaws.dynamodb.v20120810.WriteRequest>>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when RequestItems.Attr is not a list', (sst) => {
      assertType('RequestItems.Attr', 'ParameterizedList', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when RequestItems.Attr.0 is not a struct', (sst) => {
      assertType('RequestItems.Attr.0', 'ValueStruct<WriteRequest>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when RequestItems.Attr.0.DeleteRequest is not a struct', (sst) => {
      assertType('RequestItems.Attr.0.DeleteRequest', 'FieldStruct<DeleteRequest>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when RequestItems.Attr.0.DeleteRequest.Key is not a map', (sst) => {
      assertType('RequestItems.Attr.0.DeleteRequest.Key', 'Map<AttributeValue>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when RequestItems.Attr.0.DeleteRequest.Key.Attr is not an attr struct', (sst) => {
      // Timeout removed
      assertType('RequestItems.Attr.0.DeleteRequest.Key.Attr', 'AttrStruct<ValueStruct>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when RequestItems.Attr.0.PutRequest is not a struct', (sst) => {
      assertType('RequestItems.Attr.0.PutRequest', 'FieldStruct<PutRequest>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when RequestItems.Attr.0.PutRequest.Item is not a map', (sst) => {
      assertType('RequestItems.Attr.0.PutRequest.Item', 'Map<AttributeValue>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when RequestItems.Attr.0.PutRequest.Item.Attr is not an attr struct', (sst) => {
      // Timeout removed
      assertType('RequestItems.Attr.0.PutRequest.Item.Attr', 'AttrStruct<ValueStruct>', (err) => {
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

    st.end() // End serializations tests
  })

  t.end() // End batchWriteItem tests
})
