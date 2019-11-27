var helpers = require('./helpers')

var target = 'UntagResource',
    assertType = helpers.assertType.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target),
    assertAccessDenied = helpers.assertAccessDenied.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target)

describe('untagResource', function() {

  describe('serializations', function() {

    it('should return SerializationException when ResourceArn is not a string', function(done) {
      assertType('ResourceArn', 'String', done)
    })

    it('should return SerializationException when TagKeys is not a list', function(done) {
      assertType('TagKeys', 'List', done)
    })

    it('should return SerializationException when TagKeys.0 is not a string', function(done) {
      assertType('TagKeys.0', 'String', done)
    })

  })

  describe('validations', function() {

    it('should return ValidationException for no ResourceArn', function(done) {
      assertValidation({}, 'Invalid TableArn', done)
    })

    it('should return AccessDeniedException for empty ResourceArn', function(done) {
      assertAccessDenied({ResourceArn: ''},
        /^User: arn:aws:iam::\d+:.+ is not authorized to perform: dynamodb:UntagResource on resource: \*$/,
        done)
    })

    it('should return AccessDeniedException for short unauthorized ResourceArn', function(done) {
      assertAccessDenied({ResourceArn: 'abcd'},
        /^User: arn:aws:iam::\d+:.+ is not authorized to perform: dynamodb:UntagResource on resource: abcd$/,
        done)
    })

    it('should return AccessDeniedException for long unauthorized ResourceArn', function(done) {
      assertAccessDenied({ResourceArn: 'a:b:c:d:e:f'},
        /^User: arn:aws:iam::\d+:.+ is not authorized to perform: dynamodb:UntagResource on resource: a:b:c:d:e:f$/,
        done)
    })

    it('should return AccessDeniedException for longer unauthorized ResourceArn', function(done) {
      assertAccessDenied({ResourceArn: 'a:b:c:d:e/f'},
        /^User: arn:aws:iam::\d+:.+ is not authorized to perform: dynamodb:UntagResource on resource: a:b:c:d:e\/f$/,
        done)
    })

    it('should return ValidationException for null TagKeys', function(done) {
      assertValidation({ResourceArn: 'a:b:c:d:e:f/g'},
        '1 validation error detected: Value null at \'tagKeys\' failed to satisfy constraint: Member must not be null', done)
    })

    it('should return ValidationException for invalid ResourceArn', function(done) {
      assertValidation({ResourceArn: 'a:b:c:d:e:f/g', TagKeys: []},
        'Invalid TableArn: Invalid ResourceArn provided as input a:b:c:d:e:f/g', done)
    })

    it('should return ValidationException for short table name', function(done) {
      var resourceArn = 'arn:aws:dynamodb:' + helpers.awsRegion + ':' + helpers.awsAccountId + ':table/ab'
      assertValidation({ResourceArn: resourceArn, TagKeys: []},
        'Invalid TableArn: Invalid ResourceArn provided as input ' + resourceArn, done)
    })

    it('should return ResourceNotFoundException if TagKeys are empty', function(done) {
      var resourceArn = 'arn:aws:dynamodb:' + helpers.awsRegion + ':' + helpers.awsAccountId + ':table/' + helpers.randomString()
      assertValidation({ResourceArn: resourceArn, TagKeys: []},
        'Atleast one Tag Key needs to be provided as Input.', done)
    })

    it('should return ResourceNotFoundException if ResourceArn does not exist', function(done) {
      var resourceArn = 'arn:aws:dynamodb:' + helpers.awsRegion + ':' + helpers.awsAccountId + ':table/' + helpers.randomString()
      assertNotFound({ResourceArn: resourceArn, TagKeys: ['a']},
        'Requested resource not found', done)
    })

  })

})
