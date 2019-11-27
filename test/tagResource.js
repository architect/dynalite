var helpers = require('./helpers')

var target = 'TagResource',
    assertType = helpers.assertType.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target),
    assertAccessDenied = helpers.assertAccessDenied.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target)

describe('tagResource', function() {

  describe('serializations', function() {

    it('should return SerializationException when ResourceArn is not a string', function(done) {
      assertType('ResourceArn', 'String', done)
    })

    it('should return SerializationException when Tags is not a list', function(done) {
      assertType('Tags', 'List', done)
    })

    it('should return SerializationException when Tags.0 is not a struct', function(done) {
      assertType('Tags.0', 'ValueStruct<Tag>', done)
    })

    it('should return SerializationException when Tags.0.Key is not a string', function(done) {
      assertType('Tags.0.Key', 'String', done)
    })

    it('should return SerializationException when Tags.0.Value is not a string', function(done) {
      assertType('Tags.0.Value', 'String', done)
    })

  })

  describe('validations', function() {

    it('should return ValidationException for no ResourceArn', function(done) {
      assertValidation({}, 'Invalid TableArn', done)
    })

    it('should return AccessDeniedException for empty ResourceArn', function(done) {
      assertAccessDenied({ResourceArn: ''},
        /^User: arn:aws:iam::\d+:.+ is not authorized to perform: dynamodb:TagResource on resource: \*$/,
        done)
    })

    it('should return AccessDeniedException for short unauthorized ResourceArn', function(done) {
      assertAccessDenied({ResourceArn: 'abcd'},
        /^User: arn:aws:iam::\d+:.+ is not authorized to perform: dynamodb:TagResource on resource: abcd$/,
        done)
    })

    it('should return AccessDeniedException for long unauthorized ResourceArn', function(done) {
      assertAccessDenied({ResourceArn: 'a:b:c:d:e:f'},
        /^User: arn:aws:iam::\d+:.+ is not authorized to perform: dynamodb:TagResource on resource: a:b:c:d:e:f$/,
        done)
    })

    it('should return AccessDeniedException for longer unauthorized ResourceArn', function(done) {
      assertAccessDenied({ResourceArn: 'a:b:c:d:e/f'},
        /^User: arn:aws:iam::\d+:.+ is not authorized to perform: dynamodb:TagResource on resource: a:b:c:d:e\/f$/,
        done)
    })

    it('should return ValidationException for null Tags', function(done) {
      assertValidation({ResourceArn: 'a:b:c:d:e:f/g'},
        '1 validation error detected: Value null at \'tags\' failed to satisfy constraint: Member must not be null', done)
    })

    it('should return ValidationException for invalid ResourceArn', function(done) {
      assertValidation({ResourceArn: 'a:b:c:d:e:f/g', Tags: []},
        'Invalid TableArn: Invalid ResourceArn provided as input a:b:c:d:e:f/g', done)
    })

    it('should return ValidationException for short table name', function(done) {
      var resourceArn = 'arn:aws:dynamodb:' + helpers.awsRegion + ':' + helpers.awsAccountId + ':table/ab'
      assertValidation({ResourceArn: resourceArn, Tags: []},
        'Invalid TableArn: Invalid ResourceArn provided as input ' + resourceArn, done)
    })

    it('should return ResourceNotFoundException if Tags are empty', function(done) {
      var resourceArn = 'arn:aws:dynamodb:' + helpers.awsRegion + ':' + helpers.awsAccountId + ':table/' + helpers.randomString()
      assertValidation({ResourceArn: resourceArn, Tags: []},
        'Atleast one Tag needs to be provided as Input.', done)
    })

    it('should return ResourceNotFoundException if ResourceArn does not exist', function(done) {
      var resourceArn = 'arn:aws:dynamodb:' + helpers.awsRegion + ':' + helpers.awsAccountId + ':table/' + helpers.randomString()
      assertNotFound({ResourceArn: resourceArn, Tags: [{Key: 'a', Value: 'b'}]},
        'Requested resource not found: ResourcArn: ' + resourceArn + ' not found', done)
    })

  })

})
