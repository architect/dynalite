var helpers = require('./helpers')

var target = 'UpdateTable',
  request = helpers.request,
  opts = helpers.opts.bind(null, target),
  assertType = helpers.assertType.bind(null, target),
  assertValidation = helpers.assertValidation.bind(null, target),
  assertNotFound = helpers.assertNotFound.bind(null, target)

describe('updateTable', function () {
  describe('serializations', function () {

    it('should return SerializationException when TableName is not a string', function (done) {
      assertType('TableName', 'String', done)
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

    it('should return SerializationException when GlobalSecondaryIndexUpdates is not a list', function (done) {
      assertType('GlobalSecondaryIndexUpdates', 'List', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0 is not a struct', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0', 'ValueStruct<GlobalSecondaryIndexUpdate>', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Update is not a struct', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Update', 'FieldStruct<UpdateGlobalSecondaryIndexAction>', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Update.IndexName is not a string', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Update.IndexName', 'String', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Update.ProvisionedThroughput is not a struct', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Update.ProvisionedThroughput', 'FieldStruct<ProvisionedThroughput>', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Update.ProvisionedThroughput.WriteCapacityUnits is not a long', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Update.ProvisionedThroughput.WriteCapacityUnits', 'Long', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Update.ProvisionedThroughput.ReadCapacityUnits is not a long', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Update.ProvisionedThroughput.ReadCapacityUnits', 'Long', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create is not a struct', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Create', 'FieldStruct<CreateGlobalSecondaryIndexAction>', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.IndexName is not a string', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Create.IndexName', 'String', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.ProvisionedThroughput is not a struct', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Create.ProvisionedThroughput', 'FieldStruct<ProvisionedThroughput>', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.ProvisionedThroughput.WriteCapacityUnits is not a long', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Create.ProvisionedThroughput.WriteCapacityUnits', 'Long', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.ProvisionedThroughput.ReadCapacityUnits is not a long', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Create.ProvisionedThroughput.ReadCapacityUnits', 'Long', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.KeySchema is not a list', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Create.KeySchema', 'List', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.KeySchema.0 is not a struct', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Create.KeySchema.0', 'ValueStruct<KeySchemaElement>', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.KeySchema.0.AttributeName is not a string', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Create.KeySchema.0.AttributeName', 'String', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.KeySchema.0.KeyType is not a string', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Create.KeySchema.0.KeyType', 'String', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.Projection is not a struct', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Create.Projection', 'FieldStruct<Projection>', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.Projection.NonKeyAttributes is not a list', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Create.Projection.NonKeyAttributes', 'List', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.Projection.ProjectionType is not a string', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Create.Projection.ProjectionType', 'String', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Create.Projection.NonKeyAttributes.0 is not a string', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Create.Projection.NonKeyAttributes.0', 'String', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Delete is not a struct', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Delete', 'FieldStruct<DeleteGlobalSecondaryIndexAction>', done)
    })

    it('should return SerializationException when GlobalSecondaryIndexUpdates.0.Delete.IndexName is not a strin', function (done) {
      assertType('GlobalSecondaryIndexUpdates.0.Delete.IndexName', 'String', done)
    })

    it('should return SerializationException when BillingMode is not a string', function (done) {
      assertType('BillingMode', 'String', done)
    })

  })
})