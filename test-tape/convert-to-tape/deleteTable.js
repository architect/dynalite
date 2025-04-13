const test = require('tape')
const should = require('should') // Keep should for now, specific assertions might need it
const helpers = require('./helpers')

const target = 'DeleteTable'
// Bind helper functions
const request = helpers.request
const randomName = helpers.randomName
const opts = helpers.opts.bind(null, target)
const assertType = helpers.assertType.bind(null, target)
const assertValidation = helpers.assertValidation.bind(null, target)
const assertNotFound = helpers.assertNotFound.bind(null, target)
const assertInUse = helpers.assertInUse.bind(null, target)

test('deleteTable', (t) => {

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

    st.test('should return ValidationException for invalid characters in TableName', (sst) => { // Renamed from 'null attributes'
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
      assertNotFound({ TableName: name }, 'Requested resource not found: Table: ' + name + ' not found', (err) => {
        sst.error(err, 'assertNotFound should not error')
        sst.end()
      })
    })

    st.end() // End validations tests
  })

  t.test('functionality', (st) => {

    st.test('should eventually delete a table with GSI', (sst) => {
      // Timeout removed, Tape doesn't auto-timeout
      const tableName = randomName()
      const table = {
        TableName: tableName,
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
        GlobalSecondaryIndexes: [ {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' } ],
          ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
          Projection: { ProjectionType: 'KEYS_ONLY' },
        } ],
      }

      request(helpers.opts('CreateTable', table), (err, res) => {
        sst.error(err, 'CreateTable request should not error')
        if (!res) return sst.end('No response from CreateTable') // Added guard
        sst.equal(res.statusCode, 200, 'CreateTable status code should be 200')

        assertInUse({ TableName: table.TableName }, 'Attempt to change a resource which is still in use: ' +
            'Table is being created: ' + table.TableName, (errInUse) => {
          sst.error(errInUse, 'assertInUse should succeed while table is creating')

          helpers.waitUntilActive(table.TableName, (errWaitActive) => {
            sst.error(errWaitActive, 'waitUntilActive should succeed')

            request(opts(table), (errDelete, resDelete) => {
              sst.error(errDelete, 'DeleteTable request should not error')
              if (!resDelete) return sst.end('No response from DeleteTable') // Added guard
              sst.equal(resDelete.statusCode, 200, 'DeleteTable status code should be 200')

              // Use should for deep property checks for now
              resDelete.body.TableDescription.TableStatus.should.equal('DELETING')
              should.not.exist(resDelete.body.TableDescription.GlobalSecondaryIndexes)

              helpers.waitUntilDeleted(table.TableName, (errWaitDeleted, resWaitDeleted) => {
                sst.error(errWaitDeleted, 'waitUntilDeleted should succeed')
                if (!resWaitDeleted) return sst.end('No response from DescribeTable after delete') // Added guard
                // Check for ResourceNotFoundException type
                sst.equal(resWaitDeleted.body.__type, 'com.amazonaws.dynamodb.v20120810#ResourceNotFoundException', 'Table should be ResourceNotFound after deletion')
                sst.end() // End of the entire test flow
              })
            })
          })
        })
      })
    })

    st.end() // End functionality tests
  })

  t.end() // End deleteTable tests
})
