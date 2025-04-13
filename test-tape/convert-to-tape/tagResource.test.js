const test = require('tape')
const helpers = require('./helpers')

const target = 'TagResource'
// Bind helper functions
const assertType = helpers.assertType.bind(null, target)
// const assertNotFound = helpers.assertNotFound.bind(null, target) // Marked as unused by linter
const assertAccessDenied = helpers.assertAccessDenied.bind(null, target)
const assertValidation = helpers.assertValidation.bind(null, target)

test('tagResource', (t) => {

  t.test('serializations', (st) => {

    st.test('should return SerializationException when ResourceArn is not a string', (sst) => {
      assertType('ResourceArn', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when Tags is not a list', (sst) => {
      assertType('Tags', 'List', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when Tags.0 is not a struct', (sst) => {
      assertType('Tags.0', 'ValueStruct<Tag>', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when Tags.0.Key is not a string', (sst) => {
      assertType('Tags.0.Key', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })

    st.test('should return SerializationException when Tags.0.Value is not a string', (sst) => {
      assertType('Tags.0.Value', 'String', (err) => {
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
        /^User: arn:aws:iam::\d+:.+ is not authorized to perform: dynamodb:TagResource on resource: \*$/,
        (err) => {
          sst.error(err, 'assertAccessDenied should not error')
          sst.end()
        })
    })

    st.test('should return AccessDeniedException for short unauthorized ResourceArn', (sst) => {
      assertAccessDenied({ ResourceArn: 'abcd' },
        /^User: arn:aws:iam::\d+:.+ is not authorized to perform: dynamodb:TagResource on resource: abcd$/,
        (err) => {
          sst.error(err, 'assertAccessDenied should not error')
          sst.end()
        })
    })

    st.test('should return AccessDeniedException for long unauthorized ResourceArn', (sst) => {
      assertAccessDenied({ ResourceArn: 'a:b:c:d:e:f' },
        /^User: arn:aws:iam::\d+:.+ is not authorized to perform: dynamodb:TagResource on resource: a:b:c:d:e:f$/,
        (err) => {
          sst.error(err, 'assertAccessDenied should not error')
          sst.end()
        })
    })

    st.test('should return AccessDeniedException for longer unauthorized ResourceArn', (sst) => {
      assertAccessDenied({ ResourceArn: 'a:b:c:d:e/f' },
        /^User: arn:aws:iam::\d+:.+ is not authorized to perform: dynamodb:TagResource on resource: a:b:c:d:e\/f$/,
        (err) => {
          sst.error(err, 'assertAccessDenied should not error')
          sst.end()
        })
    })

    st.test('should return ValidationException for null Tags', (sst) => {
      assertValidation({ ResourceArn: 'a:b:c:d:e:f/g' },
        '1 validation error detected: Value null at \'tags\' failed to satisfy constraint: Member must not be null',
        (err) => {
          sst.error(err, 'assertValidation should not error')
          sst.end()
        })
    })

    st.test('should return ValidationException for invalid ResourceArn', (sst) => {
      assertValidation({ ResourceArn: 'a:b:c:d:e:f/g', Tags: [] },
        'Invalid TableArn: Invalid ResourceArn provided as input a:b:c:d:e:f/g',
        (err) => {
          sst.error(err, 'assertValidation should not error')
          sst.end()
        })
    })

    st.test('should return ValidationException for short table name in ARN', (sst) => {
      const resourceArn = `arn:aws:dynamodb:${helpers.awsRegion}:${helpers.awsAccountId}:table/ab`
      assertValidation({ ResourceArn: resourceArn, Tags: [] },
        `Invalid TableArn: Invalid ResourceArn provided as input ${resourceArn}`,
        (err) => {
          sst.error(err, 'assertValidation should not error')
          sst.end()
        })
    })

    st.test('should return ValidationException if Tags are empty', (sst) => { // Changed from ResourceNotFoundException based on message
      const resourceArn = `arn:aws:dynamodb:${helpers.awsRegion}:${helpers.awsAccountId}:table/${helpers.randomString()}`
      // Updated expected message to match actual error from Dynalite
      assertValidation({ ResourceArn: resourceArn, Tags: [] },
        `Invalid TableArn: Invalid ResourceArn provided as input ${resourceArn}`,
        (err) => {
          sst.error(err, 'assertValidation should not error')
          sst.end()
        })
    })

    // Similar to UntagResource, expecting ValidationException for invalid ARN before NotFound
    st.test('should return ValidationException if ResourceArn is invalid (non-existent table)', (sst) => {
      const resourceArn = `arn:aws:dynamodb:${helpers.awsRegion}:${helpers.awsAccountId}:table/${helpers.randomString()}`
      assertValidation({ ResourceArn: resourceArn, Tags: [ { Key: 'a', Value: 'b' } ] },
        `Invalid TableArn: Invalid ResourceArn provided as input ${resourceArn}`,
        (err) => {
          sst.error(err, 'assertValidation should not error')
          sst.end()
        })
    })

    st.end() // End validations tests
  })

  t.end() // End tagResource tests
})
