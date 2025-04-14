var async = require('async'),
  helpers = require('./helpers'),
  db = require('../../db')

var target = 'BatchWriteItem',
  request = helpers.request,
  randomName = helpers.randomName,
  opts = helpers.opts.bind(null, target),
  assertType = helpers.assertType.bind(null, target),
  assertValidation = helpers.assertValidation.bind(null, target),
  assertNotFound = helpers.assertNotFound.bind(null, target)

describe('batchWriteItem', function () {
  describe('serializations', function () {

    it('should return SerializationException when RequestItems is not a map', function (done) {
      assertType('RequestItems', 'Map<java.util.List<com.amazonaws.dynamodb.v20120810.WriteRequest>>', done)
    })

    it('should return SerializationException when RequestItems.Attr is not a list', function (done) {
      assertType('RequestItems.Attr', 'ParameterizedList', done)
    })

    it('should return SerializationException when RequestItems.Attr.0 is not a struct', function (done) {
      assertType('RequestItems.Attr.0', 'ValueStruct<WriteRequest>', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.DeleteRequest is not a struct', function (done) {
      assertType('RequestItems.Attr.0.DeleteRequest', 'FieldStruct<DeleteRequest>', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.DeleteRequest.Key is not a map', function (done) {
      assertType('RequestItems.Attr.0.DeleteRequest.Key', 'Map<AttributeValue>', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.DeleteRequest.Key.Attr is not an attr struct', function (done) {
      this.timeout(60000)
      assertType('RequestItems.Attr.0.DeleteRequest.Key.Attr', 'AttrStruct<ValueStruct>', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.PutRequest is not a struct', function (done) {
      assertType('RequestItems.Attr.0.PutRequest', 'FieldStruct<PutRequest>', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.PutRequest.Item is not a map', function (done) {
      assertType('RequestItems.Attr.0.PutRequest.Item', 'Map<AttributeValue>', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.PutRequest.Item.Attr is not an attr struct', function (done) {
      this.timeout(60000)
      assertType('RequestItems.Attr.0.PutRequest.Item.Attr', 'AttrStruct<ValueStruct>', done)
    })

    it('should return SerializationException when ReturnConsumedCapacity is not a string', function (done) {
      assertType('ReturnConsumedCapacity', 'String', done)
    })

    it('should return SerializationException when ReturnItemCollectionMetrics is not a string', function (done) {
      assertType('ReturnItemCollectionMetrics', 'String', done)
    })

  })
})