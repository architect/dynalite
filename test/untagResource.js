var helpers = require('./helpers')

var target = 'UntagResource',
    assertType = helpers.assertType.bind(null, target),
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

  })

})
