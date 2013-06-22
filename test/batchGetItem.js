var async = require('async'),
    helpers = require('./helpers'),
    should = require('should'),
    dynalite = require('..')

var target = 'DynamoDB_20120810.BatchGetItem',
    request = helpers.request,
    opts = helpers.opts.bind(null, target),
    assertSerialization = helpers.assertSerialization.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target)

describe('batchGetItem', function() {

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

    it('should return SerializationException when RequestItems.Attr is not a struct', function(done) {
      assertType('RequestItems.Attr', 'Structure', done)
    })

    it('should return SerializationException when RequestItems.Attr.Keys is not a list', function(done) {
      assertType('RequestItems.Attr.Keys', 'List', done)
    })

    it('should return SerializationException when RequestItems.Attr.Keys.0 is not a map', function(done) {
      assertType('RequestItems.Attr.Keys.0', 'Map', done)
    })

    it('should return SerializationException when RequestItems.Attr.Keys.0.Attr is not a struct', function(done) {
      assertType('RequestItems.Attr.Keys.0.Attr', 'Structure', done)
    })

    it('should return SerializationException when RequestItems.Attr.Keys.0.Attr.S is not a string', function(done) {
      assertType('RequestItems.Attr.Keys.0.Attr.S', 'String', done)
    })

    it('should return SerializationException when RequestItems.Attr.Keys.0.Attr.B is not a blob', function(done) {
      assertType('RequestItems.Attr.Keys.0.Attr.B', 'Blob', done)
    })

    it('should return SerializationException when RequestItems.Attr.Keys.0.Attr.N is not a string', function(done) {
      assertType('RequestItems.Attr.Keys.0.Attr.N', 'String', done)
    })

    it('should return SerializationException when RequestItems.Attr.AttributesToGet is not a list', function(done) {
      assertType('RequestItems.Attr.AttributesToGet', 'List', done)
    })

    it('should return SerializationException when RequestItems.Attr.ConsistentRead is not a boolean', function(done) {
      assertType('RequestItems.Attr.ConsistentRead', 'Boolean', done)
    })

    it('should return SerializationException when ReturnConsumedCapacity is not a string', function(done) {
      assertType('ReturnConsumedCapacity', 'String', done)
    })

  })

  describe('validations', function() {

  })

})


