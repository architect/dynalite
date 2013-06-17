var async = require('async'),
    helpers = require('./helpers'),
    dynalite = require('..')

var target = 'DynamoDB_20120810.ListTables',
    request = helpers.request,
    opts = helpers.opts.bind(null, target),
    assertSerialization = helpers.assertSerialization.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target)

describe('listTables', function() {

  before(function(done) {
    dynalite.listen(4567, done)
  })

  after(function(done) {
    dynalite.close(done)
  })

  describe('serializations', function() {

    it('should return 400 if no body', function(done) {
      request({headers: {'x-amz-target': target}}, function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(400)
        res.body.should.eql({__type: 'com.amazon.coral.service#SerializationException'})
        done()
      })
    })

    it('should return 200 if no params and application/json', function(done) {
      var requestOpts = opts({})
      requestOpts.headers['Content-Type'] = 'application/json'
      request(requestOpts, function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.TableNames.should.be.an.instanceOf(Array)
        res.headers['x-amzn-requestid'].length.should.equal(52)
        res.headers['x-amz-crc32'].should.not.be.empty
        res.headers['content-type'].should.equal('application/json')
        res.headers['content-length'].should.equal(String(JSON.stringify(res.body).length))
        done()
      })
    })

    it('should return 200 if no params and application/x-amz-json-1.0', function(done) {
      request(opts({}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.TableNames.should.be.an.instanceOf(Array)
        res.headers['x-amzn-requestid'].length.should.equal(52)
        res.headers['x-amz-crc32'].should.not.be.empty
        res.headers['content-type'].should.equal('application/x-amz-json-1.0')
        res.headers['content-length'].should.equal(String(JSON.stringify(res.body).length))
        done()
      })
    })

    it('should return 200 if random attributes are supplied', function(done) {
      request(opts({hi: 'yo', stuff: 'things'}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.TableNames.should.be.an.instanceOf(Array)
        done()
      })
    })

    it('should return 200 if null attributes are supplied', function(done) {
      request(opts({ExclusiveStartTableName: null, Limit: null}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.TableNames.should.be.an.instanceOf(Array)
        done()
      })
    })

    it('should return 200 if correct types are supplied', function(done) {
      request(opts({ExclusiveStartTableName: 'aaa', Limit: 100}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.TableNames.should.be.an.instanceOf(Array)
        done()
      })
    })

    // it should not include ExclusiveStartTableName value in output

    it('should return SerializationException when ExclusiveStartTableName is not a string', function(done) {
      assertType('ExclusiveStartTableName', 'String', done)
    })

    it('should return SerializationException when Limit is not an integer', function(done) {
      assertType('Limit', 'Integer', done)
    })

    it('should serialize ExclusiveStartTableName before Limit', function(done) {
      async.parallel([
        assertSerialization.bind(null, {ExclusiveStartTableName: true, Limit: true},
          'class java.lang.Boolean can not be converted to an String'),
        assertSerialization.bind(null, {Limit: true, ExclusiveStartTableName: true},
          'class java.lang.Boolean can not be converted to an String'),
      ], done)
    })
  })

  describe('validations', function() {

    it('should return ValidationExceptions in order', function(done) {
      assertValidation({ExclusiveStartTableName: 'a;', Limit: 500},
                       '3 validation errors detected: ' +
                       'Value \'500\' at \'limit\' failed to satisfy constraint: ' +
                       'Member must have value less than or equal to 100; ' +
                       'Value \'a;\' at \'exclusiveStartTableName\' failed to satisfy constraint: ' +
                       'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
                       'Value \'a;\' at \'exclusiveStartTableName\' failed to satisfy constraint: ' +
                       'Member must have length greater than or equal to 3', done)
    })

    it('should return ValidationException for empty ExclusiveStartTableName', function(done) {
      assertValidation({ExclusiveStartTableName: ''},
                       '2 validation errors detected: ' +
                       'Value \'\' at \'exclusiveStartTableName\' failed to satisfy constraint: ' +
                       'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
                       'Value \'\' at \'exclusiveStartTableName\' failed to satisfy constraint: ' +
                       'Member must have length greater than or equal to 3', done)
    })

    it('should return ValidationException for long ExclusiveStartTableName', function(done) {
      var name = '', i
      for (i = 0; i < 256; i++) name += 'a'
      assertValidation({ExclusiveStartTableName: name},
                       '1 validation error detected: ' +
                       'Value \'' + name + '\' at \'exclusiveStartTableName\' failed to satisfy constraint: ' +
                       'Member must have length less than or equal to 255', done)
    })

    it('should return ValidationException for low Limit', function(done) {
      assertValidation({Limit: 0},
                       '1 validation error detected: ' +
                       'Value \'0\' at \'limit\' failed to satisfy constraint: ' +
                       'Member must have value greater than or equal to 1', done)
    })

  })

})
