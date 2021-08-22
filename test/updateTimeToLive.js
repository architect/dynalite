var helpers = require('./helpers')

var target = 'UpdateTimeToLive',
    request = helpers.request,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target)

describe('updateTimeToLive', function() {

  describe('serializations', function() {

    it('should return SerializationException when TableName is not a string', function(done) {
      assertType('TableName', 'String', done)
    })

    it('should return SerializationException when TimeToLiveSpecification is not a struct', function(done) {
      assertType('TimeToLiveSpecification', 'FieldStruct<TimeToLiveSpecification>', done)
    })

    it('should return SerializationException when TimeToLiveSpecification.AttributeName is not a string', function(done) {
      assertType('TimeToLiveSpecification.AttributeName', 'String', done)
    })

    it('should return SerializationException when TimeToLiveSpecification.Enabled is not a boolean', function(done) {
      assertType('TimeToLiveSpecification.Enabled', 'Boolean', done)
    })

  })

  describe('validations', function() {

    it('should return ValidationException for no TableName', function(done) {
      assertValidation({},
        'The parameter \'TableName\' is required but was not present in the request', done)
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

    it('should return ValidationException for invalid chars', function(done) {
      assertValidation({TableName: 'abc;'},
        '1 validation error detected: ' +
        'Value \'abc;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+', done)
    })

    it('should return ValidationException for empty TimeToLiveSpecification', function(done) {
      assertValidation({TableName: 'abc', TimeToLiveSpecification: {}}, [
        'Value null at \'timeToLiveSpecification.enabled\' failed to satisfy constraint: ' +
        'Member must not be null',
        'Value null at \'timeToLiveSpecification.attributeName\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for null members in TimeToLiveSpecification', function(done) {
      assertValidation({TableName: 'abc', TimeToLiveSpecification: {AttributeName: null, Enabled: null}}, [
        'Value null at \'timeToLiveSpecification.attributeName\' failed to satisfy constraint: ' +
        'Member must not be null',
        'Value null at \'timeToLiveSpecification.enabled\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for empty TimeToLiveSpecification.AttributeName', function(done) {
      assertValidation({TableName: 'abc',
        TimeToLiveSpecification: {AttributeName: "", Enabled: true}},
        'TimeToLiveSpecification.AttributeName must be non empty', done)
    })

    it('should return ResourceNotFoundException if table does not exist', function(done) {
      var name = helpers.randomString()
      assertNotFound({TableName: name,
        TimeToLiveSpecification: {AttributeName: "id", Enabled: true}},
        'Requested resource not found: Table: ' + name + ' not found', done)
    })

    it('should return ValidationException for false TimeToLiveSpecification.Enabled when already disabled', function(done) {
      assertValidation({TableName: helpers.testHashTable,
        TimeToLiveSpecification: {AttributeName: "a", Enabled: false}},
        'TimeToLive is already disabled', done)
    })

    it('should return ValidationException for true TimeToLiveSpecification.Enabled when already enabled', function(done) {
      request(opts({TableName: helpers.testHashTable, TimeToLiveSpecification: {AttributeName: "a", Enabled: true}}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        assertValidation({TableName: helpers.testHashTable,
          TimeToLiveSpecification: {AttributeName: "a", Enabled: true}},
          'TimeToLive is already enabled', function(err){
            if (err) return done(err)
            // teardown
            request(opts({TableName: helpers.testHashTable, TimeToLiveSpecification: {AttributeName: "a", Enabled: false}}), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)
              done()
            })
          })
      })
    })
  })

  describe('functionality', function() {
    it('should enable when disabled', function(done) {
      request(opts({TableName: helpers.testHashTable, TimeToLiveSpecification: {AttributeName: "a", Enabled: true}}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({TimeToLiveSpecification: {AttributeName: "a", Enabled: true}})

        request(helpers.opts('DescribeTimeToLive', {TableName: helpers.testHashTable}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({TimeToLiveDescription: {TimeToLiveStatus: "ENABLED", AttributeName: "a"}})

          // teardown
          request(opts({TableName: helpers.testHashTable, TimeToLiveSpecification: {AttributeName: "a", Enabled: false}}), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            done()
          })
        })
      })
    })

    it('should disable when enabled', function(done) {
      request(opts({TableName: helpers.testHashTable, TimeToLiveSpecification: {AttributeName: "a", Enabled: true}}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({TimeToLiveSpecification: {AttributeName: "a", Enabled: true}})

        request(opts({TableName: helpers.testHashTable, TimeToLiveSpecification: {AttributeName: "a", Enabled: false}}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({TimeToLiveSpecification: {Enabled: false}})


          request(helpers.opts('DescribeTimeToLive', {TableName: helpers.testHashTable}), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({TimeToLiveDescription: {TimeToLiveStatus: "DISABLED"}})
            done()
          })
        })
      })
    })

    it('should delete the expired items from Tables and Indices when TTL is enabled', function(done) {
      request(opts({TableName: helpers.testRangeTable, TimeToLiveSpecification: {AttributeName: "TTL", Enabled: true}}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({TimeToLiveSpecification: {AttributeName: "TTL", Enabled: true}})

        var timestampNow = Math.round(Date.now() / 1000);
        var sharedPk = helpers.randomString()
        var sharedGsiPk = helpers.randomString()
        var expiredItem = {
          a: {S: sharedPk},
          b: {S: helpers.randomString()},
          c: {S: sharedGsiPk},
          d: {S: helpers.randomString()},
          TTL: {N: (timestampNow + 1).toString()},
        }
        var livingItem = {
          a: {S: sharedPk},
          b: {S: helpers.randomString()},
          c: {S: sharedGsiPk},
          d: {S: helpers.randomString()},
          TTL: {N: (timestampNow + 1000000).toString()},
        }
        var batchWriteItemInput = {
          RequestItems: {},
        }
        batchWriteItemInput.RequestItems[helpers.testRangeTable] = [
          { PutRequest: { Item: expiredItem }},
          { PutRequest: { Item: livingItem  }},
        ]
        request(helpers.opts('BatchWriteItem', batchWriteItemInput), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)

          setTimeout(function(){
            request(helpers.opts('Query', {
              TableName: helpers.testRangeTable,
              KeyConditionExpression: "a = :a",
              ExpressionAttributeValues: {
                ":a": expiredItem.a,
              },
            }), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)
              res.body.Items.should.eql([livingItem], 'Expired item should be deleted from table')

              request(helpers.opts('Query', {
                TableName: helpers.testRangeTable,
                IndexName: 'index1',
                KeyConditionExpression: "a = :a",
                ExpressionAttributeValues: {
                  ":a": expiredItem.a,
                },
              }), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)
                res.body.Items.should.eql([livingItem], "Expired Item should be deleted from LSI")

                request(helpers.opts('Query', {
                  TableName: helpers.testRangeTable,
                  IndexName: 'index3',
                  KeyConditionExpression: "c = :c",
                  ExpressionAttributeValues: {
                    ":c": expiredItem.c,
                  },
                }), function(err, res) {
                  if (err) return done(err)
                  res.statusCode.should.equal(200)
                  res.body.Items.should.eql([livingItem], "Expired Item should be deleted from GSI")

                  // teardown
                  request(opts({
                    TableName: helpers.testRangeTable,
                    TimeToLiveSpecification: {AttributeName: "TTL", Enabled: false}
                  }), function (err, res) {
                    if (err) return done(err)
                    res.statusCode.should.equal(200)
                    done()
                  })
                })
              })
            })
          }, 3000)
        })
      })
    })
  })
})
