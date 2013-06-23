var async = require('async'),
    helpers = require('./helpers'),
    should = require('should'),
    dynalite = require('..')

var target = 'DynamoDB_20120810.BatchWriteItem',
    request = helpers.request,
    opts = helpers.opts.bind(null, target),
    assertSerialization = helpers.assertSerialization.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target)

describe('batchWriteItem', function() {

  beforeEach(function(done) {
    dynalite.listen(4567, done)
  })

  afterEach(function(done) {
    dynalite.close(done)
  })

  describe('serializations', function() {

    it('should return SerializationException when RequestItems is not a map', function(done) {
      assertType('RequestItems', 'Map', done)
    })

    it('should return SerializationException when RequestItems.Attr is not a list', function(done) {
      assertType('RequestItems.Attr', 'List', done)
    })

    it('should return SerializationException when RequestItems.Attr.0 is not a struct', function(done) {
      assertType('RequestItems.Attr.0', 'Structure', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.DeleteRequest is not a struct', function(done) {
      assertType('RequestItems.Attr.0.DeleteRequest', 'Structure', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.DeleteRequest.Key is not a map', function(done) {
      assertType('RequestItems.Attr.0.DeleteRequest.Key', 'Map', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.DeleteRequest.Key.Attr is not a struct', function(done) {
      assertType('RequestItems.Attr.0.DeleteRequest.Key.Attr', 'Structure', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.DeleteRequest.Key.Attr.S is not a string', function(done) {
      assertType('RequestItems.Attr.0.DeleteRequest.Key.Attr.S', 'String', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.DeleteRequest.Key.Attr.B is not a blob', function(done) {
      assertType('RequestItems.Attr.0.DeleteRequest.Key.Attr.B', 'Blob', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.DeleteRequest.Key.Attr.N is not a string', function(done) {
      assertType('RequestItems.Attr.0.DeleteRequest.Key.Attr.N', 'String', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.PutRequest is not a struct', function(done) {
      assertType('RequestItems.Attr.0.PutRequest', 'Structure', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.PutRequest.Item is not a map', function(done) {
      assertType('RequestItems.Attr.0.PutRequest.Item', 'Map', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.PutRequest.Item.Attr is not a struct', function(done) {
      assertType('RequestItems.Attr.0.PutRequest.Item.Attr', 'Structure', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.PutRequest.Item.Attr.S is not a string', function(done) {
      assertType('RequestItems.Attr.0.PutRequest.Item.Attr.S', 'String', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.PutRequest.Item.Attr.B is not a blob', function(done) {
      assertType('RequestItems.Attr.0.PutRequest.Item.Attr.B', 'Blob', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.PutRequest.Item.Attr.N is not a string', function(done) {
      assertType('RequestItems.Attr.0.PutRequest.Item.Attr.N', 'String', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.PutRequest.Item.Attr.SS is not a list', function(done) {
      assertType('RequestItems.Attr.0.PutRequest.Item.Attr.SS', 'List', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.PutRequest.Item.Attr.SS.0 is not a string', function(done) {
      assertType('RequestItems.Attr.0.PutRequest.Item.Attr.SS.0', 'String', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.PutRequest.Item.Attr.NS is not a list', function(done) {
      assertType('RequestItems.Attr.0.PutRequest.Item.Attr.NS', 'List', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.PutRequest.Item.Attr.NS.0 is not a string', function(done) {
      assertType('RequestItems.Attr.0.PutRequest.Item.Attr.NS.0', 'String', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.PutRequest.Item.Attr.BS is not a list', function(done) {
      assertType('RequestItems.Attr.0.PutRequest.Item.Attr.BS', 'List', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.PutRequest.Item.Attr.BS.0 is not a blob', function(done) {
      assertType('RequestItems.Attr.0.PutRequest.Item.Attr.BS.0', 'Blob', done)
    })

    it('should return SerializationException when ReturnConsumedCapacity is not a string', function(done) {
      assertType('ReturnConsumedCapacity', 'String', done)
    })

    it('should return SerializationException when ReturnItemCollectionMetrics is not a string', function(done) {
      assertType('ReturnItemCollectionMetrics', 'String', done)
    })

  })

  describe('validations', function() {

    it('should return ValidationException for missing RequestItems', function(done) {
      assertValidation({ReturnConsumedCapacity: 'hi', ReturnItemCollectionMetrics: 'hi'},
        'The requestItems parameter is required for BatchWriteItem', done)
    })

    it('should return ValidationException for empty RequestItems', function(done) {
      assertValidation({RequestItems: {}},
        'The requestItems parameter is required for BatchWriteItem', done)
    })

    it('should return ValidationException for short table name', function(done) {
      assertValidation({RequestItems: {a:[]}, ReturnConsumedCapacity: 'hi', ReturnItemCollectionMetrics: 'hi'},
        'TableName must be at least 3 characters long and at most 255 characters long', done)
    })

    it('should return ValidationException for incorrect attributes', function(done) {
      assertValidation({RequestItems: {'aa;': [{PutRequest: {}, DeleteRequest: {}}]},
        ReturnConsumedCapacity: 'hi', ReturnItemCollectionMetrics: 'hi'},
        '4 validation errors detected: ' +
        'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [TOTAL, NONE]; ' +
        'Value \'hi\' at \'returnItemCollectionMetrics\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [SIZE, NONE]; ' +
        'Value null at \'requestItems.aa;.member.1.member.deleteRequest.key\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'requestItems.aa;.member.1.member.putRequest.item\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

  })

})


