var async = require('async'),
  helpers = require('./helpers')

var target = 'PutItem',
  request = helpers.request,
  randomName = helpers.randomName,
  opts = helpers.opts.bind(null, target),
  assertType = helpers.assertType.bind(null, target),
  assertValidation = helpers.assertValidation.bind(null, target),
  assertNotFound = helpers.assertNotFound.bind(null, target),
  assertConditional = helpers.assertConditional.bind(null, target)

describe('putItem', function () {
  describe('serializations', function () {

    it('should return SerializationException when TableName is not a string', function (done) {
      assertType('TableName', 'String', done)
    })

    it('should return SerializationException when Item is not a map', function (done) {
      assertType('Item', 'Map<AttributeValue>', done)
    })

    it('should return SerializationException when Item.Attr is not an attr struct', function (done) {
      this.timeout(60000)
      assertType('Item.Attr', 'AttrStruct<ValueStruct>', done)
    })

    it('should return SerializationException when Expected is not a map', function (done) {
      assertType('Expected', 'Map<ExpectedAttributeValue>', done)
    })

    it('should return SerializationException when Expected.Attr is not a struct', function (done) {
      assertType('Expected.Attr', 'ValueStruct<ExpectedAttributeValue>', done)
    })

    it('should return SerializationException when Expected.Attr.Exists is not a boolean', function (done) {
      assertType('Expected.Attr.Exists', 'Boolean', done)
    })

    it('should return SerializationException when Expected.Attr.Value is not an attr struct', function (done) {
      this.timeout(60000)
      assertType('Expected.Attr.Value', 'AttrStruct<FieldStruct>', done)
    })

    it('should return SerializationException when ReturnConsumedCapacity is not a string', function (done) {
      assertType('ReturnConsumedCapacity', 'String', done)
    })

    it('should return SerializationException when ReturnItemCollectionMetrics is not a string', function (done) {
      assertType('ReturnItemCollectionMetrics', 'String', done)
    })

    it('should return SerializationException when ReturnValues is not a string', function (done) {
      assertType('ReturnValues', 'String', done)
    })

    it('should return SerializationException when ConditionExpression is not a string', function (done) {
      assertType('ConditionExpression', 'String', done)
    })

    it('should return SerializationException when ExpressionAttributeValues is not a map', function (done) {
      assertType('ExpressionAttributeValues', 'Map<AttributeValue>', done)
    })

    it('should return SerializationException when ExpressionAttributeValues.Attr is not an attr struct', function (done) {
      this.timeout(60000)
      assertType('ExpressionAttributeValues.Attr', 'AttrStruct<ValueStruct>', done)
    })

    it('should return SerializationException when ExpressionAttributeNames is not a map', function (done) {
      assertType('ExpressionAttributeNames', 'Map<java.lang.String>', done)
    })

    it('should return SerializationException when ExpressionAttributeNames.Attr is not a string', function (done) {
      assertType('ExpressionAttributeNames.Attr', 'String', done)
    })

  })
})