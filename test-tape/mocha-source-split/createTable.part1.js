var helpers = require('./helpers'),
  should = require('should')

var target = 'CreateTable',
  request = helpers.request,
  randomName = helpers.randomName,
  opts = helpers.opts.bind(null, target),
  assertType = helpers.assertType.bind(null, target),
  assertValidation = helpers.assertValidation.bind(null, target)

describe('createTable', function () {
  describe('serializations', function () {

    it('should return SerializationException when TableName is not a string', function (done) {
      assertType('TableName', 'String', done)
    })

    it('should return SerializationException when AttributeDefinitions is not a list', function (done) {
      assertType('AttributeDefinitions', 'List', done)
    })

    it('should return SerializationException when KeySchema is not a list', function (done) {
      assertType('KeySchema', 'List', done)
    })

    it('should return SerializationException when LocalSecondaryIndexes is not a list', function (done) {
      assertType('LocalSecondaryIndexes', 'List', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexes is not a list', function (done) {
      assertType('GlobalSecondaryIndexes', 'List', done)
    })

    it('should return SerializationException when ProvisionedThroughput is not a struct', function (done) {
      assertType('ProvisionedThroughput', 'FieldStruct<ProvisionedThroughput>', done)
    })

    it('should return SerializationException when ProvisionedThroughput.WriteCapacityUnits is not a long', function (done) {
      assertType('ProvisionedThroughput.WriteCapacityUnits', 'Long', done)
    })

    it('should return SerializationException when ProvisionedThroughput.ReadCapacityUnits is not a long', function (done) {
      assertType('ProvisionedThroughput.ReadCapacityUnits', 'Long', done)
    })

    it('should return SerializationException when KeySchema.0 is not a struct', function (done) {
      assertType('KeySchema.0', 'ValueStruct<KeySchemaElement>', done)
    })

    it('should return SerializationException when KeySchema.0.KeyType is not a string', function (done) {
      assertType('KeySchema.0.KeyType', 'String', done)
    })

    it('should return SerializationException when KeySchema.0.AttributeName is not a string', function (done) {
      assertType('KeySchema.0.AttributeName', 'String', done)
    })

    it('should return SerializationException when AttributeDefinitions.0 is not a struct', function (done) {
      assertType('AttributeDefinitions.0', 'ValueStruct<AttributeDefinition>', done)
    })

    it('should return SerializationException when AttributeDefinitions.0.AttributeName is not a string', function (done) {
      assertType('AttributeDefinitions.0.AttributeName', 'String', done)
    })

    it('should return SerializationException when AttributeDefinitions.0.AttributeType is not a string', function (done) {
      assertType('AttributeDefinitions.0.AttributeType', 'String', done)
    })

    it('should return SerializationException when LocalSecondaryIndexes.0 is not a struct', function (done) {
      assertType('LocalSecondaryIndexes.0', 'ValueStruct<LocalSecondaryIndex>', done)
    })

    it('should return SerializationException when LocalSecondaryIndexes.0.IndexName is not a string', function (done) {
      assertType('LocalSecondaryIndexes.0.IndexName', 'String', done)
    })

    it('should return SerializationException when LocalSecondaryIndexes.0.KeySchema is not a list', function (done) {
      assertType('LocalSecondaryIndexes.0.KeySchema', 'List', done)
    })

    it('should return SerializationException when LocalSecondaryIndexes.0.Projection is not a struct', function (done) {
      assertType('LocalSecondaryIndexes.0.Projection', 'FieldStruct<Projection>', done)
    })

    it('should return SerializationException when LocalSecondaryIndexes.0.KeySchema.0 is not a struct', function (done) {
      assertType('LocalSecondaryIndexes.0.KeySchema.0', 'ValueStruct<KeySchemaElement>', done)
    })

    it('should return SerializationException when LocalSecondaryIndexes.0.KeySchema.0.AttributeName is not a string', function (done) {
      assertType('LocalSecondaryIndexes.0.KeySchema.0.AttributeName', 'String', done)
    })

    it('should return SerializationException when LocalSecondaryIndexes.0.KeySchema.0.KeyType is not a string', function (done) {
      assertType('LocalSecondaryIndexes.0.KeySchema.0.KeyType', 'String', done)
    })

    it('should return SerializationException when LocalSecondaryIndexes.0.Projection.NonKeyAttributes is not a list', function (done) {
      assertType('LocalSecondaryIndexes.0.Projection.NonKeyAttributes', 'List', done)
    })

    it('should return SerializationException when LocalSecondaryIndexes.0.Projection.ProjectionType is not a string', function (done) {
      assertType('LocalSecondaryIndexes.0.Projection.ProjectionType', 'String', done)
    })

    it('should return SerializationException when LocalSecondaryIndexes.0.Projection.NonKeyAttributes.0 is not a string', function (done) {
      assertType('LocalSecondaryIndexes.0.Projection.NonKeyAttributes.0', 'String', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexes.0 is not a struct', function (done) {
      assertType('GlobalSecondaryIndexes.0', 'ValueStruct<GlobalSecondaryIndex>', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexes.0.IndexName is not a string', function (done) {
      assertType('GlobalSecondaryIndexes.0.IndexName', 'String', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexes.0.KeySchema is not a list', function (done) {
      assertType('GlobalSecondaryIndexes.0.KeySchema', 'List', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexes.0.Projection is not a struct', function (done) {
      assertType('GlobalSecondaryIndexes.0.Projection', 'FieldStruct<Projection>', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexes.0.KeySchema.0 is not a struct', function (done) {
      assertType('GlobalSecondaryIndexes.0.KeySchema.0', 'ValueStruct<KeySchemaElement>', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexes.0.KeySchema.0.AttributeName is not a string', function (done) {
      assertType('GlobalSecondaryIndexes.0.KeySchema.0.AttributeName', 'String', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexes.0.KeySchema.0.KeyType is not a string', function (done) {
      assertType('GlobalSecondaryIndexes.0.KeySchema.0.KeyType', 'String', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexes.0.Projection.NonKeyAttributes is not a list', function (done) {
      assertType('GlobalSecondaryIndexes.0.Projection.NonKeyAttributes', 'List', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexes.0.Projection.ProjectionType is not a string', function (done) {
      assertType('GlobalSecondaryIndexes.0.Projection.ProjectionType', 'String', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexes.0.Projection.NonKeyAttributes.0 is not a string', function (done) {
      assertType('GlobalSecondaryIndexes.0.Projection.NonKeyAttributes.0', 'String', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexes.0.ProvisionedThroughput is not a struct', function (done) {
      assertType('GlobalSecondaryIndexes.0.ProvisionedThroughput', 'FieldStruct<ProvisionedThroughput>', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexes.0.ProvisionedThroughput.WriteCapacityUnits is not a long', function (done) {
      assertType('GlobalSecondaryIndexes.0.ProvisionedThroughput.WriteCapacityUnits', 'Long', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexes.0.ProvisionedThroughput.ReadCapacityUnits is not a long', function (done) {
      assertType('GlobalSecondaryIndexes.0.ProvisionedThroughput.ReadCapacityUnits', 'Long', done)
    })

    it('should return SerializationException when BillingMode is not a string', function (done) {
      assertType('BillingMode', 'String', done)
    })

  })
})