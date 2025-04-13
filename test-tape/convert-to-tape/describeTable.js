const test = require('tape')
const helpers = require('./helpers')

const target = 'DescribeTable'
const request = helpers.request
const assertType = helpers.assertType.bind(null, target)
const assertValidation = helpers.assertValidation.bind(null, target)
const assertNotFound = helpers.assertNotFound.bind(null, target)

test('describeTable', (t) => {

  t.test('serializations', (st) => {
    st.test('should return SerializationException when TableName is not a string', (sst) => {
      assertType('TableName', 'String', (err) => {
        sst.error(err, 'assertType should not error')
        sst.end()
      })
    })
    st.end() // End serializations tests
  })

  t.test('validations', (st) => {
    st.test('should return ValidationException for no TableName', (sst) => {
      assertValidation({},
        'The parameter \'TableName\' is required but was not present in the request',
        (err) => {
          sst.error(err, 'assertValidation should not error')
          sst.end()
        })
    })

    st.test('should return ValidationException for empty TableName', (sst) => {
      assertValidation({ TableName: '' },
        'TableName must be at least 3 characters long and at most 255 characters long',
        (err) => {
          sst.error(err, 'assertValidation should not error')
          sst.end()
        })
    })

    st.test('should return ValidationException for short TableName', (sst) => {
      assertValidation({ TableName: 'a;' },
        'TableName must be at least 3 characters long and at most 255 characters long',
        (err) => {
          sst.error(err, 'assertValidation should not error')
          sst.end()
        })
    })

    st.test('should return ValidationException for long TableName', (sst) => {
      assertValidation({ TableName: new Array(256 + 1).join('a') },
        'TableName must be at least 3 characters long and at most 255 characters long',
        (err) => {
          sst.error(err, 'assertValidation should not error')
          sst.end()
        })
    })

    st.test('should return ValidationException for null attributes', (sst) => {
      assertValidation({ TableName: 'abc;' },
        '1 validation error detected: ' +
        'Value \'abc;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        (err) => {
          sst.error(err, 'assertValidation should not error')
          sst.end()
        })
    })

    st.test('should return ResourceNotFoundException if table does not exist', (sst) => {
      const name = helpers.randomString()
      assertNotFound({ TableName: name }, 'Requested resource not found: Table: ' + name + ' not found',
        (err) => {
          sst.error(err, 'assertNotFound should not error')
          sst.end()
        })
    })

    st.end() // End validations tests
  })

  // Added functionality test
  t.test('functionality', (st) => {
    st.test('should describe the test hash table successfully', (sst) => {
      const tableName = helpers.testHashTable
      helpers.waitUntilActive(tableName, (waitErr) => { // Ensure table is active first
        sst.error(waitErr, `waitUntilActive for ${tableName} should not error`)

        request(helpers.opts('DescribeTable', { TableName: tableName }), (err, res) => {
          sst.error(err, 'DescribeTable request should not error')
          if (!res) return sst.end('No response from DescribeTable')

          sst.equal(res.statusCode, 200, 'DescribeTable status code should be 200')
          sst.ok(res.body.Table, 'Response body should contain Table description')
          if (res.body.Table) {
            sst.equal(res.body.Table.TableName, tableName, 'Table name should match')
            sst.ok(res.body.Table.TableArn, 'Table ARN should exist')
            sst.equal(res.body.Table.TableStatus, 'ACTIVE', 'Table status should be ACTIVE')
            // Basic check for ARN format - adjust regex if needed
            sst.ok(/^arn:aws:dynamodb:[^:]+:[^:]+:table\/.+$/.test(res.body.Table.TableArn), 'Table ARN should have expected format')
          }
          sst.end()
        })
      })
    })
    st.end() // End functionality tests
  })

  t.end() // End describeTable tests
})
