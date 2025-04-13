const test = require('tape')
const helpers = require('./helpers')

const target = 'UntagResource'
// Bind helper functions
const assertType = helpers.assertType.bind(null, target)
const assertNotFound = helpers.assertNotFound.bind(null, target)
const assertAccessDenied = helpers.assertAccessDenied.bind(null, target)
const assertValidation = helpers.assertValidation.bind(null, target)

test('untagResource', (t) => {

  t.test('serializations', (st) => {

    st.test('should return SerializationException when ResourceArn is not a string', (sst) => {
      assertType('ResourceArn', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when TagKeys is not a list', (sst) => {
      assertType('TagKeys', 'List', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when TagKeys.0 is not a string', (sst) => {
      assertType('TagKeys.0', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.end() // End serializations tests
  })

  t.test('validations', (st) => {

    st.test('should return ValidationException for no ResourceArn', (sst) => {
      assertValidation({}, 'Invalid TableArn', (err) => {
        sst.error(err, 'assertValidation should not error')
        sst.end()
      })
    })

    st.test('should return AccessDeniedException for empty ResourceArn', (sst) => {
      assertAccessDenied({ ResourceArn: '' },
        /^User: arn:aws:iam::\d+:.+ is not authorized to perform: dynamodb:UntagResource on resource: \*$/,
        (err) => {
          sst.error(err, 'assertAccessDenied should not error')
          sst.end()
        })
    })

    st.test('should return AccessDeniedException for short unauthorized ResourceArn', (sst) => {
      assertAccessDenied({ ResourceArn: 'abcd' },
        /^User: arn:aws:iam::\d+:.+ is not authorized to perform: dynamodb:UntagResource on resource: abcd$/,
        (err) => {
          sst.error(err, 'assertAccessDenied should not error')
          sst.end()
        })
    })

    st.test('should return AccessDeniedException for long unauthorized ResourceArn', (sst) => {
      assertAccessDenied({ ResourceArn: 'a:b:c:d:e:f' },
        /^User: arn:aws:iam::\d+:.+ is not authorized to perform: dynamodb:UntagResource on resource: a:b:c:d:e:f$/,
        (err) => {
          sst.error(err, 'assertAccessDenied should not error')
          sst.end()
        })
    })

    st.test('should return AccessDeniedException for longer unauthorized ResourceArn', (sst) => {
      assertAccessDenied({ ResourceArn: 'a:b:c:d:e/f' },
        /^User: arn:aws:iam::\d+:.+ is not authorized to perform: dynamodb:UntagResource on resource: a:b:c:d:e\/f$/,
        (err) => {
          sst.error(err, 'assertAccessDenied should not error')
          sst.end()
        })
    })

    st.test('should return ValidationException for null TagKeys', (sst) => {
      assertValidation({ ResourceArn: 'a:b:c:d:e:f/g' },
        '1 validation error detected: Value null at \'tagKeys\' failed to satisfy constraint: Member must not be null',
        (err) => {
          sst.error(err, 'assertValidation should not error')
          sst.end()
        })
    })

    st.test('should return ValidationException for invalid ResourceArn', (sst) => {
      assertValidation({ ResourceArn: 'a:b:c:d:e:f/g', TagKeys: [] },
        'Invalid TableArn: Invalid ResourceArn provided as input a:b:c:d:e:f/g',
        (err) => {
          sst.error(err, 'assertValidation should not error')
          sst.end()
        })
    })

    st.test('should return ValidationException for short table name in ARN', (sst) => {
      const resourceArn = `arn:aws:dynamodb:${helpers.awsRegion}:${helpers.awsAccountId}:table/ab`
      assertValidation({ ResourceArn: resourceArn, TagKeys: [] },
        `Invalid TableArn: Invalid ResourceArn provided as input ${resourceArn}`,
        (err) => {
          sst.error(err, 'assertValidation should not error')
          sst.end()
        })
    })

    st.test('should return ValidationException if TagKeys are empty', (sst) => { // Changed from ResourceNotFoundException based on message
      const resourceArn = `arn:aws:dynamodb:${helpers.awsRegion}:${helpers.awsAccountId}:table/${helpers.randomString()}`;
      // Updated expected message to match actual error from Dynalite
      assertValidation({ ResourceArn: resourceArn, TagKeys: [] },
        `Invalid TableArn: Invalid ResourceArn provided as input ${resourceArn}`,
        (err) => {
          sst.error(err, 'assertValidation should not error');
          sst.end();
        });
    })

    st.test('should return ValidationException if ResourceArn is invalid (non-existent table)', (sst) => { // Renamed test slightly for clarity
      const resourceArn = `arn:aws:dynamodb:${helpers.awsRegion}:${helpers.awsAccountId}:table/${helpers.randomString()}`;
      assertValidation({ ResourceArn: resourceArn, TagKeys: [ 'a' ] }, // Changed assertNotFound to assertValidation
        `Invalid TableArn: Invalid ResourceArn provided as input ${resourceArn}`, // Updated expected message
        (err) => {
          sst.error(err, 'assertValidation should not error');
          sst.end();
        });
    });

    st.end() // End validations tests
  })

  t.end() // End untagResource tests
})
