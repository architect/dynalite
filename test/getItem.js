var async = require('async'),
    helpers = require('./helpers'),
    should = require('should'),
    dynalite = require('..')

var target = 'GetItem',
    request = helpers.request,
    randomName = helpers.randomName,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target)

describe('getItem', function() {

  describe('serializations', function() {

    it('should return SerializationException when TableName is not a string', function(done) {
      assertType('TableName', 'String', done)
    })

    it('should return SerializationException when Key is not a map', function(done) {
      assertType('Key', 'Map', done)
    })

    it('should return SerializationException when Key.Attr is not a struct', function(done) {
      assertType('Key.Attr', 'Structure', done)
    })

    it('should return SerializationException when Key.Attr.S is not a string', function(done) {
      assertType('Key.Attr.S', 'String', done)
    })

    it('should return SerializationException when Key.Attr.B is not a blob', function(done) {
      assertType('Key.Attr.B', 'Blob', done)
    })

    it('should return SerializationException when Key.Attr.N is not a string', function(done) {
      assertType('Key.Attr.N', 'String', done)
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

    it('should return ValidationException for incorrect attributes', function(done) {
      assertValidation({TableName: 'abc;', ReturnConsumedCapacity: 'hi', AttributesToGet: []},
        '4 validation errors detected: ' +
        'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [TOTAL, NONE]; ' +
        'Value \'[]\' at \'attributesToGet\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1; ' +
        'Value \'abc;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value null at \'key\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for empty key type', function(done) {
      assertValidation({TableName: 'abc', Key: {a: {}}},
        'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException for bad key type', function(done) {
      assertValidation({TableName: 'abc', Key: {a: {a: ''}}},
        'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException for empty string', function(done) {
      assertValidation({TableName: 'abc', Key: {a: {S: ''}}},
        'One or more parameter values were invalid: An AttributeValue may not contain an empty string.', done)
    })

    it('should return ValidationException for empty binary', function(done) {
      assertValidation({TableName: 'abc', Key: {a: {B: ''}}},
        'One or more parameter values were invalid: An AttributeValue may not contain an empty binary type.', done)
    })

    // Somehow allows set types for keys
    it('should return ValidationException for empty set key', function(done) {
      assertValidation({TableName: 'abc', Key: {a: {SS: []}}},
        'One or more parameter values were invalid: An AttributeValue may not contain an empty set.', done)
    })

    it('should return ValidationException for empty string in set', function(done) {
      assertValidation({TableName: 'abc', Key: {a: {SS: ['a', '']}}},
        'One or more parameter values were invalid: An AttributeValue may not contain an empty string.', done)
    })

    it('should return ValidationException for empty binary in set', function(done) {
      assertValidation({TableName: 'abc', Key: {a: {BS: ['aaaa', '']}}},
        'One or more parameter values were invalid: Binary sets may not contain null or empty values', done)
    })

    it('should return ValidationException if key has empty numeric in set', function(done) {
      assertValidation({TableName: 'abc', Key: {a: {NS: ['1', '']}}},
        'The parameter cannot be converted to a numeric value', done)
    })

    it('should return ValidationException for duplicate string in set', function(done) {
      assertValidation({TableName: 'abc', Key: {a: {SS: ['a', 'a']}}},
        'One or more parameter values were invalid: Input collection [a, a] contains duplicates.', done)
    })

    it('should return ValidationException for duplicate number in set', function(done) {
      assertValidation({TableName: 'abc', Key: {a: {NS: ['1', '1']}}},
        'Input collection contains duplicates', done)
    })

    it('should return ValidationException for duplicate binary in set', function(done) {
      assertValidation({TableName: 'abc', Key: {a: {BS: ['Yg==', 'Yg==']}}},
        'One or more parameter values were invalid: Input collection [Yg==, Yg==]of type BS contains duplicates.', done)
    })

    it('should return ValidationException for multiple types', function(done) {
      assertValidation({TableName: 'abc', Key: {a: {S: 'a', N: '1'}}},
        'Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException if key has empty numeric type', function(done) {
      assertValidation({TableName: 'abc', Key: {a: {N: ''}}},
        'The parameter cannot be converted to a numeric value', done)
    })

    it('should return ValidationException if key has incorrect numeric type', function(done) {
      assertValidation({TableName: 'abc', Key: {a: {N: 'b'}}},
        'The parameter cannot be converted to a numeric value: b', done)
    })

    it('should return ValidationException if key has incorrect numeric type in set', function(done) {
      assertValidation({TableName: 'abc', Key: {a: {NS: ['1', 'b', 'a']}}},
        'The parameter cannot be converted to a numeric value: b', done)
    })

    it('should return ResourceNotFoundException if key is empty and table does not exist', function(done) {
      assertNotFound({TableName: helpers.randomString(), Key: {}},
        'Requested resource not found', done)
    })

    it('should return ValidationException if key is empty and table does exist', function(done) {
      assertValidation({TableName: helpers.testHashTable, Key: {}},
        'The provided key element does not match the schema', done)
    })

    it('should return ValidationException if key has incorrect attributes', function(done) {
      assertValidation({TableName: helpers.testHashTable, Key: {b: {S: 'a'}}},
        'The provided key element does not match the schema', done)
    })

    it('should return ValidationException if key has extra attributes', function(done) {
      assertValidation({TableName: helpers.testHashTable, Key: {a: {S: 'a'}, b: {S: 'a'}}},
        'The provided key element does not match the schema', done)
    })

    it('should return ValidationException if key is incorrect binary type', function(done) {
      assertValidation({TableName: helpers.testHashTable, Key: {a: {B: 'abcd'}}},
        'The provided key element does not match the schema', done)
    })

    it('should return ValidationException if key is incorrect numeric type', function(done) {
      assertValidation({TableName: helpers.testHashTable, Key: {a: {N: '1'}}},
        'The provided key element does not match the schema', done)
    })

    it('should return ValidationException if key is incorrect string set type', function(done) {
      assertValidation({TableName: helpers.testHashTable, Key: {a: {SS: ['a']}}},
        'The provided key element does not match the schema', done)
    })

    it('should return ValidationException if key is incorrect numeric set type', function(done) {
      assertValidation({TableName: helpers.testHashTable, Key: {a: {NS: ['1']}}},
        'The provided key element does not match the schema', done)
    })

    it('should return ValidationException if key is incorrect binary set type', function(done) {
      assertValidation({TableName: helpers.testHashTable, Key: {a: {BS: ['aaaa']}}},
        'The provided key element does not match the schema', done)
    })

    it('should return ValidationException if missing range key', function(done) {
      assertValidation({TableName: helpers.testRangeTable, Key: {a: {S: 'a'}}},
        'The provided key element does not match the schema', done)
    })

    it('should return ResourceNotFoundException if table is being created', function(done) {
      var table = {
        TableName: randomName(),
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1},
      }
      request(helpers.opts('CreateTable', table), function(err) {
        if (err) return done(err)
        assertNotFound({TableName: table.TableName, Key: {a: {S: 'a'}}},
          'Requested resource not found', done)
      })
    })

  })

  describe('functionality', function() {

    var hashItem = {a: {S: helpers.randomString()}, b: {S: 'a'}, c: {N: '23'}},
        rangeItem = {a: {S: helpers.randomString()}, b: {S: helpers.randomString()}, c: {N: '23'}}

    before(function(done) {
      var putItems = [
        {TableName: helpers.testHashTable, Item: hashItem},
        {TableName: helpers.testRangeTable, Item: rangeItem},
      ]
      async.forEach(putItems, function(putItem, cb) { request(helpers.opts('PutItem', putItem), cb) }, done)
    })

    it('should return empty response if key does not exist', function(done) {
      request(opts({TableName: helpers.testHashTable, Key: {a: {S: helpers.randomString()}}}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({})
        done()
      })
    })

    it('should return ConsumedCapacity if specified', function(done) {
      request(opts({TableName: helpers.testHashTable, Key: {a: {S: 'a'}}, ReturnConsumedCapacity: 'TOTAL'}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({ConsumedCapacity: {CapacityUnits: 0.5, TableName: helpers.testHashTable}})
        done()
      })
    })

    it('should return ConsumedCapacity if specified and consistent read is double', function(done) {
      request(opts({TableName: helpers.testHashTable, Key: {a: {S: 'a'}}, ReturnConsumedCapacity: 'TOTAL', ConsistentRead: 0.5}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({ConsumedCapacity: {CapacityUnits: 0.5, TableName: helpers.testHashTable}})
        done()
      })
    })

    it('should return full ConsumedCapacity if specified', function(done) {
      request(opts({TableName: helpers.testHashTable, Key: {a: {S: 'a'}}, ReturnConsumedCapacity: 'TOTAL', ConsistentRead: true}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({ConsumedCapacity: {CapacityUnits: 1, TableName: helpers.testHashTable}})
        done()
      })
    })

    it('should return full ConsumedCapacity if specified and double', function(done) {
      request(opts({TableName: helpers.testHashTable, Key: {a: {S: 'a'}}, ReturnConsumedCapacity: 'TOTAL', ConsistentRead: -1.1}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({ConsumedCapacity: {CapacityUnits: 1, TableName: helpers.testHashTable}})
        done()
      })
    })

    it('should return object by hash key', function(done) {
      request(opts({TableName: helpers.testHashTable, Key: {a: hashItem.a}, ConsistentRead: true}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({Item: hashItem})
        done()
      })
    })

    it('should return object by range key', function(done) {
      request(opts({TableName: helpers.testRangeTable, Key: {a: rangeItem.a, b: rangeItem.b}, ConsistentRead: true}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({Item: rangeItem})
        done()
      })
    })

    it('should only return requested attributes', function(done) {
      request(opts({TableName: helpers.testHashTable, Key: {a: hashItem.a}, AttributesToGet: ['b', 'c'], ConsistentRead: true}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({Item: {b: hashItem.b, c: hashItem.c}})
        done()
      })
    })

  })

})


