var helpers = require('./helpers')

var target = 'TagResource',
    assertType = helpers.assertType.bind(null, target),
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

  })

})
