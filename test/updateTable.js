var helpers = require('./helpers')

var target = 'UpdateTable',
  request = helpers.request,
  opts = helpers.opts.bind(null, target),
  assertType = helpers.assertType.bind(null, target),
  assertValidation = helpers.assertValidation.bind(null, target),
  assertNotFound = helpers.assertNotFound.bind(null, target)

describe('updateTable', function () {

  describe('serializations', function () {

    it('should return SerializationException when TableName is not a string', function (done) {
      assertType('TableName', 'String', done)
    })

    it('should return SerializationException when ProvisionedThroughput is not a struct', function (done) {
      assertType('ProvisionedThroughput', 'FieldStruct<ProvisionedThroughput>', done)
    })

    it('should return SerializationException when ProvisionedThroughput.WriteCapacityUnits is not a long', function (done) {
      assertType('ProvisionedThroughput.WriteCapacityUnits', 'Long', done)
    })

    it('should return SerializationException when ProvisionedThroughput.ReadCapacityUnits is not a long', function (done) {
      assertType('ProvisionedThroughput.ReadCapacityUnits', 'Long', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates is not a list', function (done) {
      assertType('GlobalSecondaryIndexUpdates', 'List', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0 is not a struct', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0', 'ValueStruct<GlobalSecondaryIndexUpdate>', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Update is not a struct', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Update', 'FieldStruct<UpdateGlobalSecondaryIndexAction>', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Update.IndexName is not a string', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Update.IndexName', 'String', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Update.ProvisionedThroughput is not a struct', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Update.ProvisionedThroughput', 'FieldStruct<ProvisionedThroughput>', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Update.ProvisionedThroughput.WriteCapacityUnits is not a long', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Update.ProvisionedThroughput.WriteCapacityUnits', 'Long', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Update.ProvisionedThroughput.ReadCapacityUnits is not a long', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Update.ProvisionedThroughput.ReadCapacityUnits', 'Long', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create is not a struct', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Create', 'FieldStruct<CreateGlobalSecondaryIndexAction>', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.IndexName is not a string', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Create.IndexName', 'String', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.ProvisionedThroughput is not a struct', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Create.ProvisionedThroughput', 'FieldStruct<ProvisionedThroughput>', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.ProvisionedThroughput.WriteCapacityUnits is not a long', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Create.ProvisionedThroughput.WriteCapacityUnits', 'Long', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.ProvisionedThroughput.ReadCapacityUnits is not a long', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Create.ProvisionedThroughput.ReadCapacityUnits', 'Long', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.KeySchema is not a list', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Create.KeySchema', 'List', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.KeySchema.0 is not a struct', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Create.KeySchema.0', 'ValueStruct<KeySchemaElement>', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.KeySchema.0.AttributeName is not a string', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Create.KeySchema.0.AttributeName', 'String', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.KeySchema.0.KeyType is not a string', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Create.KeySchema.0.KeyType', 'String', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.Projection is not a struct', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Create.Projection', 'FieldStruct<Projection>', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.Projection.NonKeyAttributes is not a list', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Create.Projection.NonKeyAttributes', 'List', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.Projection.ProjectionType is not a string', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Create.Projection.ProjectionType', 'String', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.Projection.NonKeyAttributes.0 is not a string', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Create.Projection.NonKeyAttributes.0', 'String', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Delete is not a struct', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Delete', 'FieldStruct<DeleteGlobalSecondaryIndexAction>', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Delete.IndexName is not a strin', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Delete.IndexName', 'String', done)
    })

    it('should return SerializationException when BillingMode is not a string', function (done) {
      assertType('BillingMode', 'String', done)
    })

  })

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

  describe('functionality', function () {

    it('should triple rates and then reduce if requested', function (done) {
      this.timeout(200000)
      var oldRead = helpers.readCapacity, oldWrite = helpers.writeCapacity,
        newRead = oldRead * 3, newWrite = oldWrite * 3, increase = Date.now() / 1000,
        throughput = { ReadCapacityUnits: newRead, WriteCapacityUnits: newWrite }
      request(opts({ TableName: helpers.testHashTable, ProvisionedThroughput: throughput }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        var desc = res.body.TableDescription
        desc.AttributeDefinitions.should.eql([ { AttributeName: 'a', AttributeType: 'S' } ])
        desc.CreationDateTime.should.be.below(Date.now() / 1000)
        desc.ItemCount.should.be.above(-1)
        desc.KeySchema.should.eql([ { AttributeName: 'a', KeyType: 'HASH' } ])
        desc.ProvisionedThroughput.LastIncreaseDateTime.should.be.above(increase - 5)
        desc.ProvisionedThroughput.NumberOfDecreasesToday.should.be.above(-1)
        desc.ProvisionedThroughput.ReadCapacityUnits.should.equal(oldRead)
        desc.ProvisionedThroughput.WriteCapacityUnits.should.equal(oldWrite)
        desc.TableName.should.equal(helpers.testHashTable)
        desc.TableSizeBytes.should.be.above(-1)
        desc.TableStatus.should.equal('UPDATING')

        var numDecreases = desc.ProvisionedThroughput.NumberOfDecreasesToday
        increase = desc.ProvisionedThroughput.LastIncreaseDateTime

        helpers.waitUntilActive(helpers.testHashTable, function (err, res) {
          if (err) return done(err)

          var decrease = Date.now() / 1000
          desc = res.body.Table
          desc.ProvisionedThroughput.ReadCapacityUnits.should.equal(newRead)
          desc.ProvisionedThroughput.WriteCapacityUnits.should.equal(newWrite)
          desc.ProvisionedThroughput.LastIncreaseDateTime.should.be.above(increase)

          increase = desc.ProvisionedThroughput.LastIncreaseDateTime

          throughput = { ReadCapacityUnits: oldRead, WriteCapacityUnits: oldWrite }
          request(opts({ TableName: helpers.testHashTable, ProvisionedThroughput: throughput }), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)

            desc = res.body.TableDescription
            desc.ProvisionedThroughput.LastIncreaseDateTime.should.equal(increase)
            desc.ProvisionedThroughput.LastDecreaseDateTime.should.be.above(decrease - 5)
            desc.ProvisionedThroughput.NumberOfDecreasesToday.should.equal(numDecreases)
            desc.ProvisionedThroughput.ReadCapacityUnits.should.equal(newRead)
            desc.ProvisionedThroughput.WriteCapacityUnits.should.equal(newWrite)
            desc.TableStatus.should.equal('UPDATING')

            decrease = desc.ProvisionedThroughput.LastDecreaseDateTime

            helpers.waitUntilActive(helpers.testHashTable, function (err, res) {
              if (err) return done(err)

              desc = res.body.Table
              desc.ProvisionedThroughput.LastIncreaseDateTime.should.equal(increase)
              desc.ProvisionedThroughput.LastDecreaseDateTime.should.be.above(decrease)
              desc.ProvisionedThroughput.NumberOfDecreasesToday.should.equal(numDecreases + 1)
              desc.ProvisionedThroughput.ReadCapacityUnits.should.equal(oldRead)
              desc.ProvisionedThroughput.WriteCapacityUnits.should.equal(oldWrite)

              done()
            })
          })
        })
      })
    })

    // XXX: this takes more than 20 mins to run
    it.skip('should allow table to be converted to PAY_PER_REQUEST and back again', function (done) {
      this.timeout(1500000)
      var read = helpers.readCapacity, write = helpers.writeCapacity,
        throughput = { ReadCapacityUnits: read, WriteCapacityUnits: write }, decrease = Date.now() / 1000
      request(opts({ TableName: helpers.testRangeTable, BillingMode: 'PAY_PER_REQUEST' }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        var desc = res.body.TableDescription
        desc.TableStatus.should.equal('UPDATING')
        desc.BillingModeSummary.should.eql({ BillingMode: 'PAY_PER_REQUEST' })
        desc.TableThroughputModeSummary.should.eql({ TableThroughputMode: 'PAY_PER_REQUEST' })
        desc.ProvisionedThroughput.LastDecreaseDateTime.should.be.above(decrease - 5)
        desc.ProvisionedThroughput.NumberOfDecreasesToday.should.be.above(-1)
        desc.ProvisionedThroughput.ReadCapacityUnits.should.equal(0)
        desc.ProvisionedThroughput.WriteCapacityUnits.should.equal(0)

        desc.GlobalSecondaryIndexes.forEach(function (index) {
          index.IndexStatus.should.equal('UPDATING')
          index.ProvisionedThroughput.should.eql({
            NumberOfDecreasesToday: 0,
            ReadCapacityUnits: 0,
            WriteCapacityUnits: 0,
          })
        })

        helpers.waitUntilActive(helpers.testRangeTable, function (err, res) {
          if (err) return done(err)

          var desc = res.body.Table
          desc.BillingModeSummary.BillingMode.should.equal('PAY_PER_REQUEST')
          desc.BillingModeSummary.LastUpdateToPayPerRequestDateTime.should.be.above(decrease - 5)
          desc.TableThroughputModeSummary.TableThroughputMode.should.equal('PAY_PER_REQUEST')
          desc.TableThroughputModeSummary.LastUpdateToPayPerRequestDateTime.should.be.above(decrease - 5)
          desc.ProvisionedThroughput.NumberOfDecreasesToday.should.be.above(-1)
          desc.ProvisionedThroughput.ReadCapacityUnits.should.equal(0)
          desc.ProvisionedThroughput.WriteCapacityUnits.should.equal(0)
          desc.GlobalSecondaryIndexes.forEach(function (index) {
            index.ProvisionedThroughput.LastDecreaseDateTime.should.be.above(decrease - 5)
            index.ProvisionedThroughput.NumberOfDecreasesToday.should.be.above(0)
            index.ProvisionedThroughput.ReadCapacityUnits.should.equal(0)
            index.ProvisionedThroughput.WriteCapacityUnits.should.equal(0)
          })

          assertValidation({ TableName: helpers.testRangeTable, BillingMode: 'PROVISIONED', ProvisionedThroughput: throughput },
            'One or more parameter values were invalid: ' +
              'ProvisionedThroughput must be specified for index: index3,index4', function (err) {
              if (err) return done(err)

              request(opts({
                TableName: helpers.testRangeTable,
                BillingMode: 'PROVISIONED',
                ProvisionedThroughput: throughput,
                GlobalSecondaryIndexUpdates: [ {
                  Update: {
                    IndexName: 'index3',
                    ProvisionedThroughput: throughput,
                  },
                }, {
                  Update: {
                    IndexName: 'index4',
                    ProvisionedThroughput: throughput,
                  },
                } ],
              }), function (err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)

                var desc = res.body.TableDescription
                desc.TableStatus.should.equal('UPDATING')
                desc.BillingModeSummary.BillingMode.should.equal('PROVISIONED')
                desc.BillingModeSummary.LastUpdateToPayPerRequestDateTime.should.be.above(decrease - 5)
                desc.TableThroughputModeSummary.TableThroughputMode.should.equal('PROVISIONED')
                desc.TableThroughputModeSummary.LastUpdateToPayPerRequestDateTime.should.be.above(decrease - 5)
                desc.ProvisionedThroughput.NumberOfDecreasesToday.should.be.above(-1)
                desc.ProvisionedThroughput.ReadCapacityUnits.should.equal(read)
                desc.ProvisionedThroughput.WriteCapacityUnits.should.equal(write)

                desc.GlobalSecondaryIndexes.forEach(function (index) {
                  index.IndexStatus.should.equal('UPDATING')
                  index.ProvisionedThroughput.LastDecreaseDateTime.should.be.above(decrease - 5)
                  index.ProvisionedThroughput.NumberOfDecreasesToday.should.be.above(0)
                  index.ProvisionedThroughput.ReadCapacityUnits.should.equal(read)
                  index.ProvisionedThroughput.WriteCapacityUnits.should.equal(write)
                })

                helpers.waitUntilActive(helpers.testRangeTable, function (err, res) {
                  if (err) return done(err)

                  var desc = res.body.Table
                  desc.BillingModeSummary.BillingMode.should.equal('PROVISIONED')
                  desc.BillingModeSummary.LastUpdateToPayPerRequestDateTime.should.be.above(decrease - 5)
                  desc.TableThroughputModeSummary.TableThroughputMode.should.equal('PROVISIONED')
                  desc.TableThroughputModeSummary.LastUpdateToPayPerRequestDateTime.should.be.above(decrease - 5)
                  desc.ProvisionedThroughput.NumberOfDecreasesToday.should.be.above(-1)
                  desc.ProvisionedThroughput.ReadCapacityUnits.should.equal(read)
                  desc.ProvisionedThroughput.WriteCapacityUnits.should.equal(write)

                  desc.GlobalSecondaryIndexes.forEach(function (index) {
                    index.ProvisionedThroughput.LastDecreaseDateTime.should.be.above(decrease - 5)
                    index.ProvisionedThroughput.NumberOfDecreasesToday.should.be.above(0)
                    index.ProvisionedThroughput.ReadCapacityUnits.should.equal(read)
                    index.ProvisionedThroughput.WriteCapacityUnits.should.equal(write)
                  })

                  done()
                })
              })
            })
        })
      })
    })

  })
})
