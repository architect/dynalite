const test = require('tape')
// const should = require('should'); // Unused in part1
// const async = require('async'); // Likely unused
const helpers = require('./helpers')

const target = 'Scan'
// Bind helper functions - anticipating unused ones
// const request = helpers.request; // Unused in part1
// const opts = helpers.opts.bind(null, target); // Unused in part1
const assertType = helpers.assertType.bind(null, target)
// const assertValidation = helpers.assertValidation.bind(null, target); // Unused in part1
// const assertNotFound = helpers.assertNotFound.bind(null, target); // Unused in part1
// const runSlowTests = helpers.runSlowTests; // Unused in part1

test('scan', (t) => {
  t.test('serializations', (st) => {

    st.test('should return SerializationException when TableName is not a string', (sst) => {
      assertType('TableName', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when ExclusiveStartKey is not a map', (sst) => {
      assertType('ExclusiveStartKey', 'Map<AttributeValue>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when ExclusiveStartKey.Attr is not an attr struct', (sst) => {
      // Timeout removed
      assertType('ExclusiveStartKey.Attr', 'AttrStruct<ValueStruct>', (err) => {
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

    st.test('should return SerializationException when ReturnConsumedCapacity is not a string', (sst) => {
      assertType('ReturnConsumedCapacity', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when Select is not a string', (sst) => {
      assertType('Select', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when Limit is not an integer', (sst) => {
      assertType('Limit', 'Integer', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when Segment is not an integer', (sst) => {
      assertType('Segment', 'Integer', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when ConditionalOperator is not a string', (sst) => {
      assertType('ConditionalOperator', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when TotalSegments is not an integer', (sst) => {
      assertType('TotalSegments', 'Integer', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when ScanFilter is not a map', (sst) => {
      assertType('ScanFilter', 'Map<Condition>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when ScanFilter.Attr is not a struct', (sst) => {
      assertType('ScanFilter.Attr', 'ValueStruct<Condition>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when ScanFilter.Attr.ComparisonOperator is not a string', (sst) => {
      assertType('ScanFilter.Attr.ComparisonOperator', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when ScanFilter.Attr.AttributeValueList is not a list', (sst) => {
      assertType('ScanFilter.Attr.AttributeValueList', 'List', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when ScanFilter.Attr.AttributeValueList.0 is not an attr struct', (sst) => {
      // Timeout removed
      assertType('ScanFilter.Attr.AttributeValueList.0', 'AttrStruct<ValueStruct>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when FilterExpression is not a string', (sst) => {
      assertType('FilterExpression', 'String', (err) => {
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

    st.test('should return SerializationException when ProjectionExpression is not a string', (sst) => {
      assertType('ProjectionExpression', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when IndexName is not a string', (sst) => {
      assertType('IndexName', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.end() // End serializations tests
  })
  // Note: The original file only contained the 'serializations' describe block.
  // The 'validations' and 'functionality' blocks seem to be in scan.part2.js etc.
  t.end() // End scan tests
})
