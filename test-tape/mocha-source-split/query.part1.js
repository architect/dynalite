var helpers = require('./helpers'),
  should = require('should'),
  async = require('async')

var target = 'Query',
  request = helpers.request,
  opts = helpers.opts.bind(null, target),
  assertType = helpers.assertType.bind(null, target),
  assertValidation = helpers.assertValidation.bind(null, target),
  assertNotFound = helpers.assertNotFound.bind(null, target),
  runSlowTests = helpers.runSlowTests

describe('query', function () {
  describe('serializations', function () {

    it('should return SerializationException when TableName is not a string', function (done) {
      assertType('TableName', 'String', done)
    })

    it('should return SerializationException when ExclusiveStartKey is not a map', function (done) {
      assertType('ExclusiveStartKey', 'Map<AttributeValue>', done)
    })

    it('should return SerializationException when ExclusiveStartKey.Attr is not an attr struct', function (done) {
      this.timeout(60000)
      assertType('ExclusiveStartKey.Attr', 'AttrStruct<ValueStruct>', done)
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

    it('should return SerializationException when QueryFilter is not a map', function (done) {
      assertType('QueryFilter', 'Map<Condition>', done)
    })

    it('should return SerializationException when QueryFilter.Attr is not a struct', function (done) {
      assertType('QueryFilter.Attr', 'ValueStruct<Condition>', done)
    })

    it('should return SerializationException when QueryFilter.Attr.ComparisonOperator is not a string', function (done) {
      assertType('QueryFilter.Attr.ComparisonOperator', 'String', done)
    })

    it('should return SerializationException when QueryFilter.Attr.AttributeValueList is not a list', function (done) {
      assertType('QueryFilter.Attr.AttributeValueList', 'List', done)
    })

    it('should return SerializationException when QueryFilter.Attr.AttributeValueList.0 is not an attr struct', function (done) {
      this.timeout(60000)
      assertType('QueryFilter.Attr.AttributeValueList.0', 'AttrStruct<ValueStruct>', done)
    })

    it('should return SerializationException when IndexName is not a string', function (done) {
      assertType('IndexName', 'String', done)
    })

    it('should return SerializationException when ScanIndexForward is not a boolean', function (done) {
      assertType('ScanIndexForward', 'Boolean', done)
    })

    it('should return SerializationException when Select is not a string', function (done) {
      assertType('Select', 'String', done)
    })

    it('should return SerializationException when Limit is not an integer', function (done) {
      assertType('Limit', 'Integer', done)
    })

    it('should return SerializationException when ConditionalOperator is not a string', function (done) {
      assertType('ConditionalOperator', 'String', done)
    })

    it('should return SerializationException when KeyConditions is not a map', function (done) {
      assertType('KeyConditions', 'Map<Condition>', done)
    })

    it('should return SerializationException when KeyConditions.Attr is not a struct', function (done) {
      assertType('KeyConditions.Attr', 'ValueStruct<Condition>', done)
    })

    it('should return SerializationException when KeyConditions.Attr.ComparisonOperator is not a string', function (done) {
      assertType('KeyConditions.Attr.ComparisonOperator', 'String', done)
    })

    it('should return SerializationException when KeyConditions.Attr.AttributeValueList is not a list', function (done) {
      assertType('KeyConditions.Attr.AttributeValueList', 'List', done)
    })

    it('should return SerializationException when KeyConditions.Attr.AttributeValueList.0 is not an attr struct', function (done) {
      this.timeout(60000)
      assertType('KeyConditions.Attr.AttributeValueList.0', 'AttrStruct<ValueStruct>', done)
    })

    it('should return SerializationException when KeyConditionExpression is not a string', function (done) {
      assertType('KeyConditionExpression', 'String', done)
    })

    it('should return SerializationException when FilterExpression is not a string', function (done) {
      assertType('FilterExpression', 'String', done)
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

    it('should return SerializationException when ProjectionExpression is not a string', function (done) {
      assertType('ProjectionExpression', 'String', done)
    })

  })
})