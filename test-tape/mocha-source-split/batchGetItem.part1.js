var async = require('async'),
  helpers = require('./helpers')

var target = 'BatchGetItem',
  request = helpers.request,
  randomName = helpers.randomName,
  opts = helpers.opts.bind(null, target),
  assertType = helpers.assertType.bind(null, target),
  assertValidation = helpers.assertValidation.bind(null, target),
  assertNotFound = helpers.assertNotFound.bind(null, target),
  runSlowTests = helpers.runSlowTests

describe('batchGetItem', function () {
  describe('serializations', function () {

    it('should return SerializationException when RequestItems is not a map', function (done) {
      assertType('RequestItems', 'Map<KeysAndAttributes>', done)
    })

    it('should return SerializationException when RequestItems.Attr is not a struct', function (done) {
      assertType('RequestItems.Attr', 'ValueStruct<KeysAndAttributes>', done)
    })

    it('should return SerializationException when RequestItems.Attr.Keys is not a list', function (done) {
      assertType('RequestItems.Attr.Keys', 'List', done)
    })

    it('should return SerializationException when RequestItems.Attr.Keys.0 is not a map', function (done) {
      assertType('RequestItems.Attr.Keys.0', 'ParameterizedMap', done)
    })

    it('should return SerializationException when RequestItems.Attr.Keys.0.Attr is not an attr struct', function (done) {
      this.timeout(60000)
      assertType('RequestItems.Attr.Keys.0.Attr', 'AttrStruct<ValueStruct>', done)
    })

    it('should return SerializationException when RequestItems.Attr.AttributesToGet is not a list', function (done) {
      assertType('RequestItems.Attr.AttributesToGet', 'List', done)
    })

    it('should return SerializationException when RequestItems.Attr.ConsistentRead is not a boolean', function (done) {
      assertType('RequestItems.Attr.ConsistentRead', 'Boolean', done)
    })

    it('should return SerializationException when RequestItems.Attr.ExpressionAttributeNames is not a map', function (done) {
      assertType('RequestItems.Attr.ExpressionAttributeNames', 'Map<java.lang.String>', done)
    })

    it('should return SerializationException when RequestItems.Attr.ExpressionAttributeNames.Attr is not a string', function (done) {
      assertType('RequestItems.Attr.ExpressionAttributeNames.Attr', 'String', done)
    })

    it('should return SerializationException when RequestItems.Attr.ProjectionExpression is not a string', function (done) {
      assertType('RequestItems.Attr.ProjectionExpression', 'String', done)
    })

    it('should return SerializationException when ReturnConsumedCapacity is not a string', function (done) {
      assertType('ReturnConsumedCapacity', 'String', done)
    })

  })
})