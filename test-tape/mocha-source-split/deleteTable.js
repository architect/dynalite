var should = require('should'),
  helpers = require('./helpers')

var target = 'DeleteTable',
  request = helpers.request,
  randomName = helpers.randomName,
  opts = helpers.opts.bind(null, target),
  assertType = helpers.assertType.bind(null, target),
  assertValidation = helpers.assertValidation.bind(null, target),
  assertNotFound = helpers.assertNotFound.bind(null, target),
  assertInUse = helpers.assertInUse.bind(null, target)

describe('deleteTable', function () {

  describe('serializations', function () {

    it('should return SerializationException when TableName is not a string', function (done) {
      assertType('TableName', 'String', done)
    })

  })

  describe('validations', function () {

    it('should return ValidationException for no TableName', function (done) {
      assertValidation({},
        'The parameter \'TableName\' is required but was not present in the request', done)
    })

    it('should return ValidationException for empty TableName', function (done) {
      assertValidation({ TableName: '' },
        'TableName must be at least 3 characters long and at most 255 characters long', done)
    })

    it('should return ValidationException for short TableName', function (done) {
      assertValidation({ TableName: 'a;' },
        'TableName must be at least 3 characters long and at most 255 characters long', done)
    })

    it('should return ValidationException for long TableName', function (done) {
      assertValidation({ TableName: new Array(256 + 1).join('a') },
        'TableName must be at least 3 characters long and at most 255 characters long', done)
    })

    it('should return ValidationException for null attributes', function (done) {
      assertValidation({ TableName: 'abc;' },
        '1 validation error detected: ' +
        'Value \'abc;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+', done)
    })

    it('should return ResourceNotFoundException if table does not exist', function (done) {
      var name = helpers.randomString()
      assertNotFound({ TableName: name }, 'Requested resource not found: Table: ' + name + ' not found', done)
    })

  })

  describe('functionality', function () {

    it('should eventually delete', function (done) {
      this.timeout(100000)
      var table = {
        TableName: randomName(),
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
        GlobalSecondaryIndexes: [ {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' } ],
          ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
          Projection: { ProjectionType: 'KEYS_ONLY' },
        } ],
      }
      request(helpers.opts('CreateTable', table), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        assertInUse({ TableName: table.TableName }, 'Attempt to change a resource which is still in use: ' +
            'Table is being created: ' + table.TableName, function (err) {
          if (err) return done(err)

          helpers.waitUntilActive(table.TableName, function (err) {
            if (err) return done(err)

            request(opts(table), function (err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)

              res.body.TableDescription.TableStatus.should.equal('DELETING')
              should.not.exist(res.body.TableDescription.GlobalSecondaryIndexes)

              helpers.waitUntilDeleted(table.TableName, function (err, res) {
                if (err) return done(err)
                res.body.__type.should.equal('com.amazonaws.dynamodb.v20120810#ResourceNotFoundException')
                done()
              })
            })
          })
        })
      })
    })

  })

})
