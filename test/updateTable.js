var helpers = require('./helpers')

var target = 'UpdateTable',
    request = helpers.request,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target)

describe('updateTable', function() {

  describe('serializations', function() {

    it('should return SerializationException when TableName is not a string', function(done) {
      assertType('TableName', 'String', done)
    })

    it('should return SerializationException when ProvisionedThroughput is not a struct', function(done) {
      assertType('ProvisionedThroughput', 'Structure', done)
    })

    it('should return SerializationException when ProvisionedThroughput.WriteCapacityUnits is not a long', function(done) {
      assertType('ProvisionedThroughput.WriteCapacityUnits', 'Long', done)
    })

    it('should return SerializationException when ProvisionedThroughput.ReadCapacityUnits is not a long', function(done) {
      assertType('ProvisionedThroughput.ReadCapacityUnits', 'Long', done)
    })

  })

  describe('validations', function() {

    it('should return ValidationException for no TableName', function(done) {
      assertValidation({},
        'The paramater \'tableName\' is required but was not present in the request', done)
    })

    it('should return ValidationException for empty TableName', function(done) {
      assertValidation({TableName: ''},
        'TableName must be at least 3 characters long and at most 255 characters long', done)
    })

    it('should return ValidationException for short TableName', function(done) {
      assertValidation({TableName: 'a;'},
        'TableName must be at least 3 characters long and at most 255 characters long', done)
    })

    it('should return ValidationException for long TableName', function(done) {
      var name = new Array(256 + 1).join('a')
      assertValidation({TableName: name},
        'TableName must be at least 3 characters long and at most 255 characters long', done)
    })

    it('should return ValidationException for null attributes', function(done) {
      assertValidation({TableName: 'abc;'},
        '1 validation error detected: ' +
        'Value \'abc;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+', done)
    })

    it('should return ValidationException for empty ProvisionedThroughput', function(done) {
      assertValidation({TableName: 'abc', ProvisionedThroughput: {}},
        '2 validation errors detected: ' +
        'Value null at \'provisionedThroughput.writeCapacityUnits\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'provisionedThroughput.readCapacityUnits\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for low ProvisionedThroughput.WriteCapacityUnits', function(done) {
      assertValidation({TableName: 'abc', ProvisionedThroughput: {ReadCapacityUnits: -1, WriteCapacityUnits: -1}},
        '2 validation errors detected: ' +
        'Value \'-1\' at \'provisionedThroughput.writeCapacityUnits\' failed to satisfy constraint: ' +
        'Member must have value greater than or equal to 1; ' +
        'Value \'-1\' at \'provisionedThroughput.readCapacityUnits\' failed to satisfy constraint: ' +
        'Member must have value greater than or equal to 1', done)
    })

    it('should return ValidationException for high ProvisionedThroughput.ReadCapacityUnits and neg', function(done) {
      assertValidation({TableName: 'abc',
        ProvisionedThroughput: {ReadCapacityUnits: 1000000000001, WriteCapacityUnits: -1}},
        '1 validation error detected: ' +
        'Value \'-1\' at \'provisionedThroughput.writeCapacityUnits\' failed to satisfy constraint: ' +
        'Member must have value greater than or equal to 1', done)
    })

    it('should return ValidationException for high ProvisionedThroughput.ReadCapacityUnits', function(done) {
      assertValidation({TableName: 'abc',
        ProvisionedThroughput: {ReadCapacityUnits: 1000000000001, WriteCapacityUnits: 1000000000001}},
        'Given value 1000000000001 for ReadCapacityUnits is out of bounds', done)
    })

    it('should return ValidationException for high ProvisionedThroughput.ReadCapacityUnits second', function(done) {
      assertValidation({TableName: 'abc',
        ProvisionedThroughput: {WriteCapacityUnits: 1000000000001, ReadCapacityUnits: 1000000000001}},
        'Given value 1000000000001 for ReadCapacityUnits is out of bounds', done)
    })

    it('should return ValidationException for high ProvisionedThroughput.WriteCapacityUnits', function(done) {
      assertValidation({TableName: 'abc',
        ProvisionedThroughput: {ReadCapacityUnits: 1000000000000, WriteCapacityUnits: 1000000000001}},
        'Given value 1000000000001 for WriteCapacityUnits is out of bounds', done)
    })

    it('should return ValidationException if read and write are same', function(done) {
      assertValidation({TableName: helpers.testHashTable,
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        'The provisioned throughput for the table will not change. The requested value equals the current value. ' +
        'Current ReadCapacityUnits provisioned for the table: 1. Requested ReadCapacityUnits: 1. ' +
        'Current WriteCapacityUnits provisioned for the table: 1. Requested WriteCapacityUnits: 1. ' +
        'Refer to the Amazon DynamoDB Developer Guide for current limits and how to request higher limits.', done)
    })

    // TODO: No idea why - this response never returns
    it.skip('should return ValidationException if read rate is more than double', function(done) {
      assertValidation({TableName: helpers.testHashTable,
        ProvisionedThroughput: {ReadCapacityUnits: 3, WriteCapacityUnits: 1}},
        '', done)
    })

    // TODO: No idea why - this response never returns
    it.skip('should return ValidationException if write rate is more than double', function(done) {
      assertValidation({TableName: helpers.testHashTable,
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 3}},
        '', done)
    })

    // TODO: No more than four decreases in a single UTC calendar day
  })

  describe('functionality', function() {

    it('should double rates and then reduce if requested', function(done) {
      this.timeout(200000)
      var throughput = {ReadCapacityUnits: 2, WriteCapacityUnits: 2}, increase = Date.now() / 1000
      request(opts({TableName: helpers.testHashTable, ProvisionedThroughput: throughput}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        var desc = res.body.TableDescription
        desc.AttributeDefinitions.should.eql([{AttributeName: 'a', AttributeType: 'S'}])
        desc.CreationDateTime.should.be.below(Date.now() / 1000)
        desc.ItemCount.should.be.above(-1)
        desc.KeySchema.should.eql([{AttributeName: 'a', KeyType: 'HASH'}])
        desc.ProvisionedThroughput.LastIncreaseDateTime.should.be.above(increase - 5)
        desc.ProvisionedThroughput.NumberOfDecreasesToday.should.equal(0)
        desc.ProvisionedThroughput.ReadCapacityUnits.should.equal(1)
        desc.ProvisionedThroughput.WriteCapacityUnits.should.equal(1)
        desc.TableName.should.equal(helpers.testHashTable)
        desc.TableSizeBytes.should.be.above(-1)
        desc.TableStatus.should.equal('UPDATING')

        increase = desc.ProvisionedThroughput.LastIncreaseDateTime

        helpers.waitUntilActive(helpers.testHashTable, function(err, res) {
          if (err) return done(err)

          var desc = res.body.Table, decrease = Date.now() / 1000
          desc.ProvisionedThroughput.ReadCapacityUnits.should.equal(2)
          desc.ProvisionedThroughput.WriteCapacityUnits.should.equal(2)
          desc.ProvisionedThroughput.LastIncreaseDateTime.should.equal(increase)

          throughput = {ReadCapacityUnits: 1, WriteCapacityUnits: 1}
          request(opts({TableName: helpers.testHashTable, ProvisionedThroughput: throughput}), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)

            var desc = res.body.TableDescription
            desc.ProvisionedThroughput.LastIncreaseDateTime.should.equal(increase)
            desc.ProvisionedThroughput.LastDecreaseDateTime.should.be.above(decrease - 5)
            desc.ProvisionedThroughput.NumberOfDecreasesToday.should.equal(0)
            desc.ProvisionedThroughput.ReadCapacityUnits.should.equal(2)
            desc.ProvisionedThroughput.WriteCapacityUnits.should.equal(2)
            desc.TableStatus.should.equal('UPDATING')

            decrease = desc.ProvisionedThroughput.LastDecreaseDateTime

            helpers.waitUntilActive(helpers.testHashTable, function(err, res) {
              if (err) return done(err)

              var desc = res.body.Table
              desc.ProvisionedThroughput.LastIncreaseDateTime.should.equal(increase)
              desc.ProvisionedThroughput.LastDecreaseDateTime.should.equal(decrease)
              desc.ProvisionedThroughput.NumberOfDecreasesToday.should.equal(1)
              desc.ProvisionedThroughput.ReadCapacityUnits.should.equal(1)
              desc.ProvisionedThroughput.WriteCapacityUnits.should.equal(1)

              done()
            })
          })
        })
      })
    })
  })
})


