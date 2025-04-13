const test = require('tape')
const helpers = require('./helpers')

const target = 'UpdateTable'
// request = helpers.request, // Assuming helpers.request is available
// opts = helpers.opts.bind(null, target), // Assuming helpers.opts is available
// assertType = helpers.assertType.bind(null, target), // Assuming helpers.assertType is available
const assertValidation = helpers.assertValidation.bind(null, target)
const assertNotFound = helpers.assertNotFound.bind(null, target)

test('updateTable - validations - should return ValidationException for no TableName', function (t) {
  assertValidation({},
    'The parameter \'TableName\' is required but was not present in the request', function (err) {
      t.error(err, 'should not error')
      t.end()
    })
})

test('updateTable - validations - should return ValidationException for empty TableName', function (t) {
  assertValidation({ TableName: '' },
    'TableName must be at least 3 characters long and at most 255 characters long', function (err) {
      t.error(err, 'should not error')
      t.end()
    })
})

test('updateTable - validations - should return ValidationException for short TableName', function (t) {
  assertValidation({ TableName: 'a;' },
    'TableName must be at least 3 characters long and at most 255 characters long', function (err) {
      t.error(err, 'should not error')
      t.end()
    })
})

test('updateTable - validations - should return ValidationException for long TableName', function (t) {
  const name = new Array(256 + 1).join('a')
  assertValidation({ TableName: name },
    'TableName must be at least 3 characters long and at most 255 characters long', function (err) {
      t.error(err, 'should not error')
      t.end()
    })
})

test('updateTable - validations - should return ValidationException for null attributes', function (t) {
  assertValidation({ TableName: 'abc;' },
    '1 validation error detected: ' +
    'Value \'abc;\' at \'tableName\' failed to satisfy constraint: ' +
    'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+', function (err) {
      t.error(err, 'should not error')
      t.end()
    })
})

test('updateTable - validations - should return ValidationException for empty ProvisionedThroughput', function (t) {
  assertValidation({ TableName: 'abc', ProvisionedThroughput: {} }, [
    'Value null at \'provisionedThroughput.writeCapacityUnits\' failed to satisfy constraint: ' +
    'Member must not be null',
    'Value null at \'provisionedThroughput.readCapacityUnits\' failed to satisfy constraint: ' +
    'Member must not be null',
  ], function (err) {
    t.error(err, 'should not error')
    t.end()
  })
})

test('updateTable - validations - should return ValidationException for low ProvisionedThroughput.WriteCapacityUnits', function (t) {
  assertValidation({ TableName: 'abc', ProvisionedThroughput: { ReadCapacityUnits: -1, WriteCapacityUnits: -1 } }, [
    'Value \'-1\' at \'provisionedThroughput.writeCapacityUnits\' failed to satisfy constraint: ' +
    'Member must have value greater than or equal to 1',
    'Value \'-1\' at \'provisionedThroughput.readCapacityUnits\' failed to satisfy constraint: ' +
    'Member must have value greater than or equal to 1',
  ], function (err) {
    t.error(err, 'should not error')
    t.end()
  })
})

test('updateTable - validations - should return ValidationException for high ProvisionedThroughput.ReadCapacityUnits and neg', function (t) {
  assertValidation({ TableName: 'abc',
    ProvisionedThroughput: { ReadCapacityUnits: 1000000000001, WriteCapacityUnits: -1 } },
  '1 validation error detected: ' +
    'Value \'-1\' at \'provisionedThroughput.writeCapacityUnits\' failed to satisfy constraint: ' +
    'Member must have value greater than or equal to 1', function (err) {
    t.error(err, 'should not error')
    t.end()
  })
})

test('updateTable - validations - should return ValidationException for high ProvisionedThroughput.ReadCapacityUnits', function (t) {
  assertValidation({ TableName: 'abc',
    ProvisionedThroughput: { ReadCapacityUnits: 1000000000001, WriteCapacityUnits: 1000000000001 } },
  'Given value 1000000000001 for ReadCapacityUnits is out of bounds', function (err) {
    t.error(err, 'should not error')
    t.end()
  })
})

test('updateTable - validations - should return ValidationException for high ProvisionedThroughput.ReadCapacityUnits second', function (t) {
  assertValidation({ TableName: 'abc',
    ProvisionedThroughput: { WriteCapacityUnits: 1000000000001, ReadCapacityUnits: 1000000000001 } },
  'Given value 1000000000001 for ReadCapacityUnits is out of bounds', function (err) {
    t.error(err, 'should not error')
    t.end()
  })
})

test('updateTable - validations - should return ValidationException for high ProvisionedThroughput.WriteCapacityUnits', function (t) {
  assertValidation({ TableName: 'abc',
    ProvisionedThroughput: { ReadCapacityUnits: 1000000000000, WriteCapacityUnits: 1000000000001 } },
  'Given value 1000000000001 for WriteCapacityUnits is out of bounds', function (err) {
    t.error(err, 'should not error')
    t.end()
  })
})

test('updateTable - validations - should return ValidationException for empty GlobalSecondaryIndexUpdates', function (t) {
  assertValidation({ TableName: 'abc', GlobalSecondaryIndexUpdates: [] },
    'At least one of ProvisionedThroughput, BillingMode, UpdateStreamEnabled, GlobalSecondaryIndexUpdates or SSESpecification or ReplicaUpdates is required', function (err) {
      t.error(err, 'should not error')
      t.end()
    })
})

test('updateTable - validations - should return ValidationException for empty Update', function (t) {
  assertValidation({ TableName: 'abc', GlobalSecondaryIndexUpdates: [ { Update: {} } ] }, [
    'Value null at \'globalSecondaryIndexUpdates.1.member.update.indexName\' failed to satisfy constraint: ' +
    'Member must not be null',
    'Value null at \'globalSecondaryIndexUpdates.1.member.update.provisionedThroughput\' failed to satisfy constraint: ' +
    'Member must not be null',
  ], function (err) {
    t.error(err, 'should not error')
    t.end()
  })
})

test('updateTable - validations - should return ValidationException for bad IndexName and ProvisionedThroughput', function (t) {
  assertValidation({ TableName: 'abc', GlobalSecondaryIndexUpdates: [
    { Update: { IndexName: 'a', ProvisionedThroughput: {} } },
    { Update: { IndexName: 'abc;', ProvisionedThroughput: { ReadCapacityUnits: 1000000000001, WriteCapacityUnits: 0 } } },
  ] }, [
    'Value \'a\' at \'globalSecondaryIndexUpdates.1.member.update.indexName\' failed to satisfy constraint: ' +
    'Member must have length greater than or equal to 3',
    'Value null at \'globalSecondaryIndexUpdates.1.member.update.provisionedThroughput.writeCapacityUnits\' failed to satisfy constraint: ' +
    'Member must not be null',
    'Value null at \'globalSecondaryIndexUpdates.1.member.update.provisionedThroughput.readCapacityUnits\' failed to satisfy constraint: ' +
    'Member must not be null',
    'Value \'abc;\' at \'globalSecondaryIndexUpdates.2.member.update.indexName\' failed to satisfy constraint: ' +
    'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
    'Value \'0\' at \'globalSecondaryIndexUpdates.2.member.update.provisionedThroughput.writeCapacityUnits\' failed to satisfy constraint: ' +
    'Member must have value greater than or equal to 1',
  ], function (err) {
    t.error(err, 'should not error')
    t.end()
  })
})

test('updateTable - validations - should return ValidationException for empty index struct', function (t) {
  assertValidation({ TableName: 'abc', GlobalSecondaryIndexUpdates: [ {} ] },
    'One or more parameter values were invalid: ' +
    'One of GlobalSecondaryIndexUpdate.Update, ' +
    'GlobalSecondaryIndexUpdate.Create, ' +
    'GlobalSecondaryIndexUpdate.Delete must not be null', function (err) {
      t.error(err, 'should not error')
      t.end()
    })
})

test('updateTable - validations - should return ValidationException for too many empty GlobalSecondaryIndexUpdates', function (t) {
  assertValidation({ TableName: 'abc', GlobalSecondaryIndexUpdates: [ {}, {}, {}, {}, {}, {} ] },
    'One or more parameter values were invalid: ' +
    'One of GlobalSecondaryIndexUpdate.Update, ' +
    'GlobalSecondaryIndexUpdate.Create, ' +
    'GlobalSecondaryIndexUpdate.Delete must not be null', function (err) {
      t.error(err, 'should not error')
      t.end()
    })
})

test('updateTable - validations - should return ValidationException for repeated GlobalSecondaryIndexUpdates', function (t) {
  assertValidation({ TableName: 'abc', GlobalSecondaryIndexUpdates: [ { Delete: { IndexName: 'abc' } }, { Delete: { IndexName: 'abc' } } ] },
    'One or more parameter values were invalid: ' +
    'Only one global secondary index update per index is allowed simultaneously. Index: abc', function (err) {
      t.error(err, 'should not error')
      t.end()
    })
})

test('updateTable - validations - should return ValidationException for ProvisionedThroughput update when PAY_PER_REQUEST', function (t) {
  assertValidation({ TableName: helpers.testHashNTable, ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
    'One or more parameter values were invalid: ' +
    'Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST', function (err) {
      t.error(err, 'should not error')
      t.end()
    })
})

test('updateTable - validations - should return ValidationException for PROVISIONED without ProvisionedThroughput', function (t) {
  assertValidation({ TableName: helpers.testHashNTable, BillingMode: 'PROVISIONED' },
    'One or more parameter values were invalid: ' +
    'ProvisionedThroughput must be specified when BillingMode is PROVISIONED', function (err) {
      t.error(err, 'should not error')
      t.end()
    })
})

test('updateTable - validations - should return ResourceNotFoundException if table does not exist', function (t) {
  const name = helpers.randomString()
  assertNotFound({ TableName: name, ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
    'Requested resource not found: Table: ' + name + ' not found', function (err) {
      t.error(err, 'should not error')
      t.end()
    })
})

test('updateTable - validations - should return NotFoundException for high index ReadCapacityUnits when table does not exist', function (t) {
  assertNotFound({ TableName: 'abc', GlobalSecondaryIndexUpdates: [
    { Update: { IndexName: 'abc', ProvisionedThroughput: { ReadCapacityUnits: 1000000000001, WriteCapacityUnits: 1000000000001 } } },
  ] }, 'Requested resource not found: Table: abc not found', function (err) {
    t.error(err, 'should not error')
    t.end()
  })
})

test('updateTable - validations - should return NotFoundException for high index WriteCapacityUnits when table does not exist', function (t) {
  assertNotFound({ TableName: 'abc', GlobalSecondaryIndexUpdates: [
    { Update: { IndexName: 'abc', ProvisionedThroughput: { ReadCapacityUnits: 1000000000000, WriteCapacityUnits: 1000000000001 } } },
  ] }, 'Requested resource not found: Table: abc not found', function (err) {
    t.error(err, 'should not error')
    t.end()
  })
})

test('updateTable - validations - should return ValidationException for high index ReadCapacityUnits when index does not exist', function (t) {
  assertValidation({ TableName: helpers.testHashTable, GlobalSecondaryIndexUpdates: [
    { Update: { IndexName: 'abc', ProvisionedThroughput: { ReadCapacityUnits: 1000000000001, WriteCapacityUnits: 1000000000001 } } },
  ] }, 'This operation cannot be performed with given input values. Please contact DynamoDB service team for more info: Action Blocked: IndexUpdate', function (err) {
    t.error(err, 'should not error')
    t.end()
  })
})

test('updateTable - validations - should return ValidationException for high index WriteCapacityUnits when index does not exist', function (t) {
  assertValidation({ TableName: helpers.testHashTable, GlobalSecondaryIndexUpdates: [
    { Update: { IndexName: 'abc', ProvisionedThroughput: { ReadCapacityUnits: 1000000000000, WriteCapacityUnits: 1000000000001 } } },
  ] }, 'This operation cannot be performed with given input values. Please contact DynamoDB service team for more info: Action Blocked: IndexUpdate', function (err) {
    t.error(err, 'should not error')
    t.end()
  })
})

test('updateTable - validations - should return ValidationException if read and write are same', function (t) {
  helpers.request(helpers.opts('DescribeTable', { TableName: helpers.testHashTable }), function (err, res) {
    if (err) {
      t.error(err, 'DescribeTable should not error')
      return t.end()
    }
    const readUnits = res.body.Table.ProvisionedThroughput.ReadCapacityUnits
    const writeUnits = res.body.Table.ProvisionedThroughput.WriteCapacityUnits
    assertValidation({ TableName: helpers.testHashTable,
      ProvisionedThroughput: { ReadCapacityUnits: readUnits, WriteCapacityUnits: writeUnits } },
    'The provisioned throughput for the table will not change. The requested value equals the current value. ' +
      'Current ReadCapacityUnits provisioned for the table: ' + readUnits + '. Requested ReadCapacityUnits: ' + readUnits + '. ' +
      'Current WriteCapacityUnits provisioned for the table: ' + writeUnits + '. Requested WriteCapacityUnits: ' + writeUnits + '. ' +
      'Refer to the Amazon DynamoDB Developer Guide for current limits and how to request higher limits.', function (err2) {
      t.error(err2, 'assertValidation should not error')
      t.end()
    })
  })
})

test('updateTable - validations - should return LimitExceededException for too many GlobalSecondaryIndexUpdates', function (t) {
  helpers.request(helpers.opts(target, { TableName: helpers.testHashTable, GlobalSecondaryIndexUpdates: [
    { Delete: { IndexName: 'abc' } },
    { Delete: { IndexName: 'abd' } },
    { Delete: { IndexName: 'abe' } },
    { Delete: { IndexName: 'abf' } },
    { Delete: { IndexName: 'abg' } },
    { Delete: { IndexName: 'abh' } },
  ] }),
  function (err, res) {
    if (err) {
      // Check if the error is the expected LimitExceededException
      t.equal(err.body.__type, 'com.amazonaws.dynamodb.v20120810#LimitExceededException', 'Error type should match')
      t.equal(err.body.message, 'Subscriber limit exceeded: Only 1 online index can be created or deleted simultaneously per table', 'Error message should match')
      t.equal(err.statusCode, 400, 'Status code should be 400')
      t.end()
    }
    else {
      t.fail('Expected LimitExceededException, but got success. Response: ' + JSON.stringify(res))
      t.end()
    }
    // Original Mocha assertions replaced by checks within the error block above
    // res.body.__type.should.equal('com.amazonaws.dynamodb.v20120810#LimitExceededException')
    // res.body.message.should.equal('Subscriber limit exceeded: Only 1 online index can be created or deleted simultaneously per table')
    // res.statusCode.should.equal(400)
    // done()
  })
})

// TODO: No more than four decreases in a single UTC calendar day (No test needed for migration)
