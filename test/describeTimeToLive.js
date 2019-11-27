var helpers = require('./helpers')

var target = 'DescribeTimeToLive',
    request = helpers.request,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target)

describe('describeTimeToLive', function() {

  describe('serializations', function() {

    it('should return SerializationException when TableName is not a string', function(done) {
      assertType('TableName', 'String', done)
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
      assertValidation({TableName: new Array(256 + 1).join('a')},
        'TableName must be at least 3 characters long and at most 255 characters long', done)
    })

    it('should return ValidationException for null attributes', function(done) {
      assertValidation({TableName: 'abc;'},
        '1 validation error detected: ' +
        'Value \'abc;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+', done)
    })

    it('should return ResourceNotFoundException if table does not exist', function(done) {
      var name = helpers.randomString()
      assertNotFound({TableName: name}, 'Requested resource not found: Table: ' + name + ' not found', done)
    })

  })

  describe('functionality', function() {

    it('should succeed if table exists', function(done) {
      request(opts({TableName: helpers.testHashTable}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({TimeToLiveDescription: {TimeToLiveStatus: 'DISABLED'}})
        done()
      })
    })

  })

})


