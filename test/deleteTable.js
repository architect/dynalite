var async = require('async'),
    helpers = require('./helpers'),
    should = require('should'),
    dynalite = require('..')

var target = 'DynamoDB_20120810.DeleteTable',
    request = helpers.request,
    opts = helpers.opts.bind(null, target),
    assertSerialization = helpers.assertSerialization.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target)

describe('deleteTable', function() {

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
        '1 validation error detected: ' +
        'Value \'abc;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+', done)
    })

    it('should return ResourceNotFoundException if table does not exist', function(done) {
      var name = String(Math.random() * 0x100000000)
      assertNotFound({TableName: name}, 'Requested resource not found: Table: ' + name + ' not found', done)
    })

    it.skip('should succeed for basic', function(done) {
      var name = 'abc' + Math.random() * 0x100000000, table = {
        TableName: name,
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1},
      }
      request(helpers.opts('DynamoDB_20120810.CreateTable', table), function(err, res) {
        if (err) return done(err)
        request(opts({TableName: name}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          should.exist(res.body.Table)
          var desc = res.body.Table
          desc.CreationDateTime.should.be.above(1371362773)
          ;delete desc.CreationDateTime
          table.ItemCount = 0
          table.ProvisionedThroughput.NumberOfDecreasesToday = 0
          table.TableSizeBytes = 0
          table.TableStatus = 'CREATING'
          desc.should.eql(table)
          done()
        })
      })
    })

  })

})


