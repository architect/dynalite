var async = require('async'),
    helpers = require('./helpers'),
    should = require('should'),
    dynalite = require('..')

var target = 'UpdateTable',
    request = helpers.request,
    opts = helpers.opts.bind(null, target),
    assertSerialization = helpers.assertSerialization.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target)

describe('updateTable', function() {

  describe('serializations', function() {

    it('should return SerializationException when TableName is not a string', function(done) {
      assertType('TableName', 'String', done)
    })

    it('should return SerializationException when ProvisionedThroughput is not a struct', function(done) {
      assertType('ProvisionedThroughput', 'Structure', done)
    })

    it('should return SerializationException when ProvisionedThroughput.WriteCapacityUnits is not a long', function(done) {
      assertType('ProvisionedThroughput.WriteCapacityUnits', 'Long', done)
    })

    it('should return SerializationException when ProvisionedThroughput.ReadCapacityUnits is not a long', function(done) {
      assertType('ProvisionedThroughput.ReadCapacityUnits', 'Long', done)
    })

  })

  describe('validations', function() {

    it('should return ValidationException for no TableName', function(done) {
      assertValidation({},
        'The paramater \'tableName\' is required but was not present in the request', done)
    })

    it('should return ValidationException for empty TableName', function(done) {
      assertValidation({TableName: ''},
        'TableName must be at least 3 characters long and at most 255 characters long', done)
    })

    it('should return ValidationException for short TableName', function(done) {
      assertValidation({TableName: 'a;'},
        'TableName must be at least 3 characters long and at most 255 characters long', done)
    })

    it('should return ValidationException for long TableName', function(done) {
      var name = '', i
      for (i = 0; i < 256; i++) name += 'a'
      assertValidation({TableName: name},
        'TableName must be at least 3 characters long and at most 255 characters long', done)
    })

    it('should return ValidationException for null attributes', function(done) {
      assertValidation({TableName: 'abc;'},
        '2 validation errors detected: ' +
        'Value \'abc;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value null at \'provisionedThroughput\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for empty ProvisionedThroughput', function(done) {
      assertValidation({TableName: 'abc', AttributeDefinitions: [], ProvisionedThroughput: {}},
        '2 validation errors detected: ' +
        'Value null at \'provisionedThroughput.writeCapacityUnits\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'provisionedThroughput.readCapacityUnits\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for low ProvisionedThroughput.WriteCapacityUnits', function(done) {
      assertValidation({TableName: 'abc', AttributeDefinitions: [], KeySchema: [],
        ProvisionedThroughput: {ReadCapacityUnits: -1, WriteCapacityUnits: -1}},
        '2 validation errors detected: ' +
        'Value \'-1\' at \'provisionedThroughput.writeCapacityUnits\' failed to satisfy constraint: ' +
        'Member must have value greater than or equal to 1; ' +
        'Value \'-1\' at \'provisionedThroughput.readCapacityUnits\' failed to satisfy constraint: ' +
        'Member must have value greater than or equal to 1', done)
    })

    it('should return ValidationException for high ProvisionedThroughput.ReadCapacityUnits and neg', function(done) {
      assertValidation({TableName: 'abc', AttributeDefinitions: [], KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}],
        ProvisionedThroughput: {ReadCapacityUnits: 1000000000001, WriteCapacityUnits: -1}},
        '1 validation error detected: ' +
        'Value \'-1\' at \'provisionedThroughput.writeCapacityUnits\' failed to satisfy constraint: ' +
        'Member must have value greater than or equal to 1', done)
    })

    it('should return ValidationException for high ProvisionedThroughput.ReadCapacityUnits', function(done) {
      assertValidation({TableName: 'abc', AttributeDefinitions: [], KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}],
        ProvisionedThroughput: {ReadCapacityUnits: 1000000000001, WriteCapacityUnits: 1000000000001}},
        'Given value 1000000000001 for ReadCapacityUnits is out of bounds', done)
    })

    it('should return ValidationException for high ProvisionedThroughput.ReadCapacityUnits second', function(done) {
      assertValidation({TableName: 'abc', AttributeDefinitions: [], KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}],
        ProvisionedThroughput: {WriteCapacityUnits: 1000000000001, ReadCapacityUnits: 1000000000001}},
        'Given value 1000000000001 for ReadCapacityUnits is out of bounds', done)
    })

    it('should return ValidationException for high ProvisionedThroughput.WriteCapacityUnits', function(done) {
      assertValidation({TableName: 'abc', AttributeDefinitions: [], KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}],
        ProvisionedThroughput: {ReadCapacityUnits: 1000000000000, WriteCapacityUnits: 1000000000001}},
        'Given value 1000000000001 for WriteCapacityUnits is out of bounds', done)
    })

  })

})


