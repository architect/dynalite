var async = require('async'),
    helpers = require('./helpers'),
    should = require('should'),
    dynalite = require('..')

var target = 'DynamoDB_20120810.Query',
    request = helpers.request,
    opts = helpers.opts.bind(null, target),
    assertSerialization = helpers.assertSerialization.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target)

describe('query', function() {

  beforeEach(function(done) {
    dynalite.listen(4567, done)
  })

  afterEach(function(done) {
    dynalite.close(done)
  })

  describe('serializations', function() {

    it('should return SerializationException when TableName is not a string', function(done) {
      assertType('TableName', 'String', done)
    })

    it('should return SerializationException when ExclusiveStartKey is not a map', function(done) {
      assertType('ExclusiveStartKey', 'Map', done)
    })

    it('should return SerializationException when ExclusiveStartKey.Attr is not a struct', function(done) {
      assertType('ExclusiveStartKey.Attr', 'Structure', done)
    })

    it('should return SerializationException when ExclusiveStartKey.Attr.S is not a string', function(done) {
      assertType('ExclusiveStartKey.Attr.S', 'String', done)
    })

    it('should return SerializationException when ExclusiveStartKey.Attr.B is not a blob', function(done) {
      assertType('ExclusiveStartKey.Attr.B', 'Blob', done)
    })

    it('should return SerializationException when ExclusiveStartKey.Attr.N is not a string', function(done) {
      assertType('ExclusiveStartKey.Attr.N', 'String', done)
    })

    it('should return SerializationException when AttributesToGet is not a list', function(done) {
      assertType('AttributesToGet', 'List', done)
    })

    it('should return SerializationException when ConsistentRead is not a boolean', function(done) {
      assertType('ConsistentRead', 'Boolean', done)
    })

    it('should return SerializationException when ReturnConsumedCapacity is not a string', function(done) {
      assertType('ReturnConsumedCapacity', 'String', done)
    })

    it('should return SerializationException when IndexName is not a string', function(done) {
      assertType('IndexName', 'String', done)
    })

    it('should return SerializationException when ScanIndexForward is not a boolean', function(done) {
      assertType('ScanIndexForward', 'Boolean', done)
    })

    it('should return SerializationException when Select is not a string', function(done) {
      assertType('Select', 'String', done)
    })

    it('should return SerializationException when Limit is not an integer', function(done) {
      assertType('Limit', 'Integer', done)
    })

    it('should return SerializationException when KeyConditions is not a map', function(done) {
      assertType('KeyConditions', 'Map', done)
    })

    it('should return SerializationException when KeyConditions.Attr is not a struct', function(done) {
      assertType('KeyConditions.Attr', 'Structure', done)
    })

    it('should return SerializationException when KeyConditions.Attr.ComparisonOperator is not a string', function(done) {
      assertType('KeyConditions.Attr.ComparisonOperator', 'String', done)
    })

    it('should return SerializationException when KeyConditions.Attr.AttributeValueList is not a list', function(done) {
      assertType('KeyConditions.Attr.AttributeValueList', 'List', done)
    })

    it('should return SerializationException when KeyConditions.Attr.AttributeValueList.0 is not a struct', function(done) {
      assertType('KeyConditions.Attr.AttributeValueList.0', 'Structure', done)
    })

    it('should return SerializationException when KeyConditions.Attr.AttributeValueList.0.S is not a string', function(done) {
      assertType('KeyConditions.Attr.AttributeValueList.0.S', 'String', done)
    })

    it('should return SerializationException when KeyConditions.Attr.AttributeValueList.0.B is not a blob', function(done) {
      assertType('KeyConditions.Attr.AttributeValueList.0.B', 'Blob', done)
    })

    it('should return SerializationException when KeyConditions.Attr.AttributeValueList.0.N is not a string', function(done) {
      assertType('KeyConditions.Attr.AttributeValueList.0.N', 'String', done)
    })

    it('should return SerializationException when KeyConditions.Attr.AttributeValueList.0.SS is not a list', function(done) {
      assertType('KeyConditions.Attr.AttributeValueList.0.SS', 'List', done)
    })

    it('should return SerializationException when KeyConditions.Attr.AttributeValueList.0.SS.0 is not a string', function(done) {
      assertType('KeyConditions.Attr.AttributeValueList.0.SS.0', 'String', done)
    })

    it('should return SerializationException when KeyConditions.Attr.AttributeValueList.0.NS is not a list', function(done) {
      assertType('KeyConditions.Attr.AttributeValueList.0.NS', 'List', done)
    })

    it('should return SerializationException when KeyConditions.Attr.AttributeValueList.0.NS.0 is not a string', function(done) {
      assertType('KeyConditions.Attr.AttributeValueList.0.NS.0', 'String', done)
    })

    it('should return SerializationException when KeyConditions.Attr.AttributeValueList.0.BS is not a list', function(done) {
      assertType('KeyConditions.Attr.AttributeValueList.0.BS', 'List', done)
    })

    it('should return SerializationException when KeyConditions.Attr.AttributeValueList.0.BS.0 is not a blob', function(done) {
      assertType('KeyConditions.Attr.AttributeValueList.0.BS.0', 'Blob', done)
    })

  })

  describe('validations', function() {

  })

})


