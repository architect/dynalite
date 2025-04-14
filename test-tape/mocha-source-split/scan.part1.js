var helpers = require('./helpers'),
  should = require('should'),
  async = require('async')

var target = 'Scan',
  request = helpers.request,
  opts = helpers.opts.bind(null, target),
  assertType = helpers.assertType.bind(null, target),
  assertValidation = helpers.assertValidation.bind(null, target),
  assertNotFound = helpers.assertNotFound.bind(null, target),
  runSlowTests = helpers.runSlowTests

describe('scan', function () {
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

    it('should return SerializationException when ReturnConsumedCapacity is not a string', function (done) {
      assertType('ReturnConsumedCapacity', 'String', done)
    })

    it('should return SerializationException when Select is not a string', function (done) {
      assertType('Select', 'String', done)
    })

    it('should return SerializationException when Limit is not an integer', function (done) {
      assertType('Limit', 'Integer', done)
    })

    it('should return SerializationException when Segment is not an integer', function (done) {
      assertType('Segment', 'Integer', done)
    })

    it('should return SerializationException when ConditionalOperator is not a string', function (done) {
      assertType('ConditionalOperator', 'String', done)
    })

    it('should return SerializationException when TotalSegments is not an integer', function (done) {
      assertType('TotalSegments', 'Integer', done)
    })

    it('should return SerializationException when ScanFilter is not a map', function (done) {
      assertType('ScanFilter', 'Map<Condition>', done)
    })

    it('should return SerializationException when ScanFilter.Attr is not a struct', function (done) {
      assertType('ScanFilter.Attr', 'ValueStruct<Condition>', done)
    })

    it('should return SerializationException when ScanFilter.Attr.ComparisonOperator is not a string', function (done) {
      assertType('ScanFilter.Attr.ComparisonOperator', 'String', done)
    })

    it('should return SerializationException when ScanFilter.Attr.AttributeValueList is not a list', function (done) {
      assertType('ScanFilter.Attr.AttributeValueList', 'List', done)
    })

    it('should return SerializationException when ScanFilter.Attr.AttributeValueList.0 is not an attr struct', function (done) {
      this.timeout(60000)
      assertType('ScanFilter.Attr.AttributeValueList.0', 'AttrStruct<ValueStruct>', done)
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

    it('should return SerializationException when IndexName is not a string', function (done) {
      assertType('IndexName', 'String', done)
    })

  })
})