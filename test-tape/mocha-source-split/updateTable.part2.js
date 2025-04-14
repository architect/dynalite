var helpers = require('./helpers')

var target = 'UpdateTable',
  request = helpers.request,
  opts = helpers.opts.bind(null, target),
  assertType = helpers.assertType.bind(null, target),
  assertValidation = helpers.assertValidation.bind(null, target),
  assertNotFound = helpers.assertNotFound.bind(null, target)

describe('updateTable', function () {
  describe('validations', function () {

    it('should return ValidationException for no TableName', function (done) {
      assertValidation({},
        'The parameter \'TableName\' is required but was not present in the request', done)
    })

    it('should return ValidationException for empty TableName', function (done) {
      assertValidation({ TableName: '' },
        'TableName must be at least 3 characters long and at most 255 characters long', done)
    })

    it('should return ValidationException for short TableName', function (done) {
      assertValidation({ TableName: 'a;' },
        'TableName must be at least 3 characters long and at most 255 characters long', done)
    })

    it('should return ValidationException for long TableName', function (done) {
      var name = new Array(256 + 1).join('a')
      assertValidation({ TableName: name },
        'TableName must be at least 3 characters long and at most 255 characters long', done)
    })

    it('should return ValidationException for null attributes', function (done) {
      assertValidation({ TableName: 'abc;' },
        '1 validation error detected: ' +
        'Value \'abc;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+', done)
    })

    it('should return ValidationException for empty ProvisionedThroughput', function (done) {
      assertValidation({ TableName: 'abc', ProvisionedThroughput: {} }, [
        'Value null at \'provisionedThroughput.writeCapacityUnits\' failed to satisfy constraint: ' +
        'Member must not be null',
        'Value null at \'provisionedThroughput.readCapacityUnits\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for low ProvisionedThroughput.WriteCapacityUnits', function (done) {
      assertValidation({ TableName: 'abc', ProvisionedThroughput: { ReadCapacityUnits: -1, WriteCapacityUnits: -1 } }, [
        'Value \'-1\' at \'provisionedThroughput.writeCapacityUnits\' failed to satisfy constraint: ' +
        'Member must have value greater than or equal to 1',
        'Value \'-1\' at \'provisionedThroughput.readCapacityUnits\' failed to satisfy constraint: ' +
        'Member must have value greater than or equal to 1',
      ], done)
    })

    it('should return ValidationException for high ProvisionedThroughput.ReadCapacityUnits and neg', function (done) {
      assertValidation({ TableName: 'abc',
        ProvisionedThroughput: { ReadCapacityUnits: 1000000000001, WriteCapacityUnits: -1 } },
      '1 validation error detected: ' +
        'Value \'-1\' at \'provisionedThroughput.writeCapacityUnits\' failed to satisfy constraint: ' +
        'Member must have value greater than or equal to 1', done)
    })

    it('should return ValidationException for high ProvisionedThroughput.ReadCapacityUnits', function (done) {
      assertValidation({ TableName: 'abc',
        ProvisionedThroughput: { ReadCapacityUnits: 1000000000001, WriteCapacityUnits: 1000000000001 } },
      'Given value 1000000000001 for ReadCapacityUnits is out of bounds', done)
    })

    it('should return ValidationException for high ProvisionedThroughput.ReadCapacityUnits second', function (done) {
      assertValidation({ TableName: 'abc',
        ProvisionedThroughput: { WriteCapacityUnits: 1000000000001, ReadCapacityUnits: 1000000000001 } },
      'Given value 1000000000001 for ReadCapacityUnits is out of bounds', done)
    })

    it('should return ValidationException for high ProvisionedThroughput.WriteCapacityUnits', function (done) {
      assertValidation({ TableName: 'abc',
        ProvisionedThroughput: { ReadCapacityUnits: 1000000000000, WriteCapacityUnits: 1000000000001 } },
      'Given value 1000000000001 for WriteCapacityUnits is out of bounds', done)
    })

    it('should return ValidationException for empty GlobalSecondaryIndexUpdates', function (done) {
      assertValidation({ TableName: 'abc', GlobalSecondaryIndexUpdates: [] },
        'At least one of ProvisionedThroughput, BillingMode, UpdateStreamEnabled, GlobalSecondaryIndexUpdates or SSESpecification or ReplicaUpdates is required', done)
    })

    it('should return ValidationException for empty Update', function (done) {
      assertValidation({ TableName: 'abc', GlobalSecondaryIndexUpdates: [ { Update: {} } ] }, [
        'Value null at \'globalSecondaryIndexUpdates.1.member.update.indexName\' failed to satisfy constraint: ' +
        'Member must not be null',
        'Value null at \'globalSecondaryIndexUpdates.1.member.update.provisionedThroughput\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for bad IndexName and ProvisionedThroughput', function (done) {
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
      ], done)
    })

    it('should return ValidationException for empty index struct', function (done) {
      assertValidation({ TableName: 'abc', GlobalSecondaryIndexUpdates: [ {} ] },
        'One or more parameter values were invalid: ' +
        'One of GlobalSecondaryIndexUpdate.Update, ' +
        'GlobalSecondaryIndexUpdate.Create, ' +
        'GlobalSecondaryIndexUpdate.Delete must not be null', done)
    })

    it('should return ValidationException for too many empty GlobalSecondaryIndexUpdates', function (done) {
      assertValidation({ TableName: 'abc', GlobalSecondaryIndexUpdates: [ {}, {}, {}, {}, {}, {} ] },
        'One or more parameter values were invalid: ' +
        'One of GlobalSecondaryIndexUpdate.Update, ' +
        'GlobalSecondaryIndexUpdate.Create, ' +
        'GlobalSecondaryIndexUpdate.Delete must not be null', done)
    })

    it('should return ValidationException for repeated GlobalSecondaryIndexUpdates', function (done) {
      assertValidation({ TableName: 'abc', GlobalSecondaryIndexUpdates: [ { Delete: { IndexName: 'abc' } }, { Delete: { IndexName: 'abc' } } ] },
        'One or more parameter values were invalid: ' +
        'Only one global secondary index update per index is allowed simultaneously. Index: abc', done)
    })

    it('should return ValidationException for ProvisionedThroughput update when PAY_PER_REQUEST', function (done) {
      assertValidation({ TableName: helpers.testHashNTable, ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
        'One or more parameter values were invalid: ' +
        'Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST', done)
    })

    it('should return ValidationException for PROVISIONED without ProvisionedThroughput', function (done) {
      assertValidation({ TableName: helpers.testHashNTable, BillingMode: 'PROVISIONED' },
        'One or more parameter values were invalid: ' +
        'ProvisionedThroughput must be specified when BillingMode is PROVISIONED', done)
    })

    it('should return ResourceNotFoundException if table does not exist', function (done) {
      var name = helpers.randomString()
      assertNotFound({ TableName: name, ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
        'Requested resource not found: Table: ' + name + ' not found', done)
    })

    it('should return NotFoundException for high index ReadCapacityUnits when table does not exist', function (done) {
      assertNotFound({ TableName: 'abc', GlobalSecondaryIndexUpdates: [
        { Update: { IndexName: 'abc', ProvisionedThroughput: { ReadCapacityUnits: 1000000000001, WriteCapacityUnits: 1000000000001 } } },
      ] }, 'Requested resource not found: Table: abc not found', done)
    })

    it('should return NotFoundException for high index WriteCapacityUnits when table does not exist', function (done) {
      assertNotFound({ TableName: 'abc', GlobalSecondaryIndexUpdates: [
        { Update: { IndexName: 'abc', ProvisionedThroughput: { ReadCapacityUnits: 1000000000000, WriteCapacityUnits: 1000000000001 } } },
      ] }, 'Requested resource not found: Table: abc not found', done)
    })

    it('should return ValidationException for high index ReadCapacityUnits when index does not exist', function (done) {
      assertValidation({ TableName: helpers.testHashTable, GlobalSecondaryIndexUpdates: [
        { Update: { IndexName: 'abc', ProvisionedThroughput: { ReadCapacityUnits: 1000000000001, WriteCapacityUnits: 1000000000001 } } },
      ] }, 'This operation cannot be performed with given input values. Please contact DynamoDB service team for more info: Action Blocked: IndexUpdate', done)
    })

    it('should return ValidationException for high index WriteCapacityUnits when index does not exist', function (done) {
      assertValidation({ TableName: helpers.testHashTable, GlobalSecondaryIndexUpdates: [
        { Update: { IndexName: 'abc', ProvisionedThroughput: { ReadCapacityUnits: 1000000000000, WriteCapacityUnits: 1000000000001 } } },
      ] }, 'This operation cannot be performed with given input values. Please contact DynamoDB service team for more info: Action Blocked: IndexUpdate', done)
    })

    it('should return ValidationException if read and write are same', function (done) {
      request(helpers.opts('DescribeTable', { TableName: helpers.testHashTable }), function (err, res) {
        if (err) return err(done)
        var readUnits = res.body.Table.ProvisionedThroughput.ReadCapacityUnits
        var writeUnits = res.body.Table.ProvisionedThroughput.WriteCapacityUnits
        assertValidation({ TableName: helpers.testHashTable,
          ProvisionedThroughput: { ReadCapacityUnits: readUnits, WriteCapacityUnits: writeUnits } },
        'The provisioned throughput for the table will not change. The requested value equals the current value. ' +
          'Current ReadCapacityUnits provisioned for the table: ' + readUnits + '. Requested ReadCapacityUnits: ' + readUnits + '. ' +
          'Current WriteCapacityUnits provisioned for the table: ' + writeUnits + '. Requested WriteCapacityUnits: ' + writeUnits + '. ' +
          'Refer to the Amazon DynamoDB Developer Guide for current limits and how to request higher limits.', done)
      })
    })

    it('should return LimitExceededException for too many GlobalSecondaryIndexUpdates', function (done) {
      request(opts({ TableName: helpers.testHashTable, GlobalSecondaryIndexUpdates: [
        { Delete: { IndexName: 'abc' } },
        { Delete: { IndexName: 'abd' } },
        { Delete: { IndexName: 'abe' } },
        { Delete: { IndexName: 'abf' } },
        { Delete: { IndexName: 'abg' } },
        { Delete: { IndexName: 'abh' } },
      ] }), function (err, res) {
        if (err) return done(err)

        res.body.__type.should.equal('com.amazonaws.dynamodb.v20120810#LimitExceededException')
        res.body.message.should.equal('Subscriber limit exceeded: Only 1 online index can be created or deleted simultaneously per table')
        res.statusCode.should.equal(400)
        done()
      })
    })

    // TODO: No more than four decreases in a single UTC calendar day
  })
})