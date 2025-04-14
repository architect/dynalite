var async = require('async'),
  helpers = require('./helpers')

var target = 'GetItem',
  request = helpers.request,
  randomName = helpers.randomName,
  opts = helpers.opts.bind(null, target),
  assertType = helpers.assertType.bind(null, target),
  assertValidation = helpers.assertValidation.bind(null, target),
  assertNotFound = helpers.assertNotFound.bind(null, target)

describe('getItem', function () {
  describe('serializations', function () {

    it('should return SerializationException when TableName is not a string', function (done) {
      assertType('TableName', 'String', done)
    })

    it('should return SerializationException when Key is not a map', function (done) {
      assertType('Key', 'Map<AttributeValue>', done)
    })

    it('should return SerializationException when Key.Attr is not an attr struct', function (done) {
      this.timeout(60000)
      assertType('Key.Attr', 'AttrStruct<ValueStruct>', done)
    })

    it('should return SerializationException when AttributesToGet is not a list', function (done) {
      assertType('AttributesToGet', 'List', done)
    })

    it('should return SerializationException when ConsistentRead is not a boolean', function (done) {
      assertType('ConsistentRead', 'Boolean', done)
    })

    it('should return SerializationException when ReturnConsumedCapacity is not a string', function (done) {
      assertType('ReturnConsumedCapacity', 'String', done)
    })

    it('should return SerializationException when ExpressionAttributeNames is not a map', function (done) {
      assertType('ExpressionAttributeNames', 'Map<java.lang.String>', done)
    })

    it('should return SerializationException when ExpressionAttributeNames.Attr is not a string', function (done) {
      assertType('ExpressionAttributeNames.Attr', 'String', done)
    })

    it('should return SerializationException when ProjectionExpression is not a string', function (done) {
      assertType('ProjectionExpression', 'String', done)
    })

  })
})