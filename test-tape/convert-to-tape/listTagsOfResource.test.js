const test = require('tape')
// const should = require('should') // Unused
const helpers = require('./helpers')

const target = 'ListTagsOfResource'
// Bind helper functions
const request = helpers.request
const opts = helpers.opts.bind(null, target)
const assertType = helpers.assertType.bind(null, target)
const assertAccessDenied = helpers.assertAccessDenied.bind(null, target)
const assertNotFound = helpers.assertNotFound.bind(null, target)
const assertValidation = helpers.assertValidation.bind(null, target)

test('listTagsOfResource', (t) => {

  t.test('serializations', (st) => {

    st.test('should return SerializationException when ResourceArn is not a string', (sst) => {
      assertType('ResourceArn', 'String', (err) => {
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
        /^User: arn:aws:iam::\d+:.+ is not authorized to perform: dynamodb:ListTagsOfResource on resource: \*$/,
        (err) => {
          sst.error(err, 'assertAccessDenied should not error')
          sst.end()
        })
    })

    st.test('should return AccessDeniedException for short unauthorized ResourceArn', (sst) => {
      assertAccessDenied({ ResourceArn: 'abcd' },
        /^User: arn:aws:iam::\d+:.+ is not authorized to perform: dynamodb:ListTagsOfResource on resource: abcd$/,
        (err) => {
          sst.error(err, 'assertAccessDenied should not error')
          sst.end()
        })
    })

    st.test('should return AccessDeniedException for long unauthorized ResourceArn', (sst) => {
      assertAccessDenied({ ResourceArn: 'a:b:c:d:e:f' },
        /^User: arn:aws:iam::\d+:.+ is not authorized to perform: dynamodb:ListTagsOfResource on resource: a:b:c:d:e:f$/,
        (err) => {
          sst.error(err, 'assertAccessDenied should not error')
          sst.end()
        })
    })

    st.test('should return AccessDeniedException for longer unauthorized ResourceArn', (sst) => {
      assertAccessDenied({ ResourceArn: 'a:b:c:d:e/f' },
        /^User: arn:aws:iam::\d+:.+ is not authorized to perform: dynamodb:ListTagsOfResource on resource: a:b:c:d:e\/f$/,
        (err) => {
          sst.error(err, 'assertAccessDenied should not error')
          sst.end()
        })
    })

    st.test('should return ValidationException for invalid ResourceArn (format)', (sst) => {
      assertValidation({ ResourceArn: 'a:b:c:d:e:f/g' },
        'Invalid TableArn: Invalid ResourceArn provided as input a:b:c:d:e:f/g',
        (err) => {
          sst.error(err, 'assertValidation should not error')
          sst.end()
        })
    })

    st.test('should return ValidationException for short table name in ARN', (sst) => {
      const resourceArn = `arn:aws:dynamodb:${helpers.awsRegion}:${helpers.getAwsAccountId()}:table/ab`
      assertValidation({ ResourceArn: resourceArn },
        `Invalid TableArn: Invalid ResourceArn provided as input ${resourceArn}`,
        (err) => {
          sst.error(err, 'assertValidation should not error')
          sst.end()
        })
    })

    // Changed back to assertNotFound as the ARN format is valid
    st.test('should return ResourceNotFoundException if ResourceArn does not exist', (sst) => {
      const resourceArn = `arn:aws:dynamodb:${helpers.awsRegion}:${helpers.getAwsAccountId()}:table/${helpers.randomString()}`
      assertNotFound({ ResourceArn: resourceArn }, // Using assertNotFound again
        `Requested resource not found: ResourcArn: ${resourceArn} not found`, // Expect specific message
        (err) => {
          sst.error(err, 'assertNotFound should not error')
          sst.end()
        })
    })

    st.end() // End validations tests
  })

  t.test('functionality', (st) => {
    const resourceArn = `arn:aws:dynamodb:${helpers.awsRegion}:${helpers.getAwsAccountId()}:table/${helpers.testHashTable}`

    st.test('should succeed if valid resource and has no tags initially', (sst) => {
      request(opts({ ResourceArn: resourceArn }), (err, res) => {
        sst.error(err, 'ListTagsOfResource request should not error')
        if (!res) return sst.end('No response from ListTagsOfResource')
        sst.equal(res.statusCode, 200, 'Status code should be 200')
        sst.deepEqual(res.body, { Tags: [] }, 'Response body should contain empty Tags array')
        sst.end()
      })
    })

    st.test('should list tags correctly after adding and removing them', (sst) => {
      const tags = [ { Key: 't1', Value: 'v1' }, { Key: 't2', Value: 'v2' } ]
      const tagKeys = tags.map(tag => tag.Key)

      // 1. Tag the resource
      request(helpers.opts('TagResource', { ResourceArn: resourceArn, Tags: tags }), (errTag, resTag) => {
        sst.error(errTag, 'TagResource request should not error')
        if (!resTag) return sst.end('No response from TagResource')
        sst.equal(resTag.statusCode, 200, 'TagResource status code should be 200')

        // 2. List the tags and verify
        request(opts({ ResourceArn: resourceArn }), (errList1, resList1) => {
          sst.error(errList1, 'ListTagsOfResource (after tag) request should not error')
          if (!resList1) return sst.end('No response from ListTagsOfResource (after tag)')
          sst.equal(resList1.statusCode, 200, 'ListTagsOfResource (after tag) status code should be 200')
          sst.ok(resList1.body.Tags, 'Tags array should exist')
          if (resList1.body.Tags) { // Guard against accessing length of null/undefined
            sst.equal(resList1.body.Tags.length, tags.length, 'Correct number of tags should be listed')
            // Simple deep equal check assumes order is preserved or doesn't matter for this test
            sst.deepEqual(resList1.body.Tags.sort((a, b) => a.Key.localeCompare(b.Key)),
              tags.sort((a, b) => a.Key.localeCompare(b.Key)),
              'Listed tags should match added tags')
          }

          // 3. Untag the resource
          request(helpers.opts('UntagResource', { ResourceArn: resourceArn, TagKeys: tagKeys }), (errUntag, resUntag) => {
            sst.error(errUntag, 'UntagResource request should not error')
            if (!resUntag) return sst.end('No response from UntagResource')
            sst.equal(resUntag.statusCode, 200, 'UntagResource status code should be 200')

            // 4. List tags again and verify they are gone
            request(opts({ ResourceArn: resourceArn }), (errList2, resList2) => {
              sst.error(errList2, 'ListTagsOfResource (after untag) request should not error')
              if (!resList2) return sst.end('No response from ListTagsOfResource (after untag)')
              sst.equal(resList2.statusCode, 200, 'ListTagsOfResource (after untag) status code should be 200')
              sst.deepEqual(resList2.body, { Tags: [] }, 'Response body should be empty Tags array after untagging')
              sst.end() // Final end for this test case
            })
          })
        })
      })
    })

    st.end() // End functionality tests
  })

  t.end() // End listTagsOfResource tests
})
