var async = require('async'),
    helpers = require('./helpers')

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

    it('should return SerializationException when Key.Attr is not an attr struct', function(done) {
      this.timeout(60000)
      assertType('Key.Attr', 'AttrStructure', done)
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
        '2 validation errors detected: ' +
        'Value null at \'tableName\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'key\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for empty TableName', function(done) {
      assertValidation({TableName: ''},
        '3 validation errors detected: ' +
        'Value \'\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 3; ' +
        'Value null at \'key\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for short TableName', function(done) {
      assertValidation({TableName: 'a;'},
        '3 validation errors detected: ' +
        'Value \'a;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'a;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 3; ' +
        'Value null at \'key\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for long TableName', function(done) {
      var name = new Array(256 + 1).join('a')
      assertValidation({TableName: name},
        '2 validation errors detected: ' +
        'Value \'' + name + '\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 255; ' +
        'Value null at \'key\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for incorrect attributes', function(done) {
      assertValidation({TableName: 'abc;', ReturnConsumedCapacity: 'hi', AttributesToGet: []},
        '4 validation errors detected: ' +
        'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [INDEXES, TOTAL, NONE]; ' +
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
        'One or more parameter values were invalid: An AttributeValue may not contain an empty string', done)
    })

    it('should return ValidationException for empty binary', function(done) {
      assertValidation({TableName: 'abc', Key: {a: {B: ''}}},
        'One or more parameter values were invalid: An AttributeValue may not contain a null or empty binary type.', done)
    })

    it('should return ValidationException for false null', function(done) {
      assertValidation({TableName: 'abc', Key: {a: {NULL: false}}},
        'One or more parameter values were invalid: Null attribute value types must have the value of true', done)
    })

    // Somehow allows set types for keys
    it('should return ValidationException for empty set key', function(done) {
      assertValidation({TableName: 'abc', Key: {a: {SS: []}}},
        'One or more parameter values were invalid: An string set  may not be empty', done)
    })

    it('should return ValidationException for empty string in set', function(done) {
      assertValidation({TableName: 'abc', Key: {a: {SS: ['a', '']}}},
        'One or more parameter values were invalid: An string set may not have a empty string as a member', done)
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

    it('should return ValidationException if key has large numeric type', function(done) {
      assertValidation({TableName: 'abc', Key: {a: {N: '123456789012345678901234567890123456789'}}},
        'Attempting to store more than 38 significant digits in a Number', done)
    })

    it('should return ValidationException if key has long digited number', function(done) {
      assertValidation({TableName: 'aaa', Key: {a: {N: '-1.23456789012345678901234567890123456789'}}},
        'Attempting to store more than 38 significant digits in a Number', done)
    })

    it('should return ValidationException if key has huge positive number', function(done) {
      assertValidation({TableName: 'aaa', Key: {a: {N: '1e126'}}},
        'Number overflow. Attempting to store a number with magnitude larger than supported range', done)
    })

    it('should return ValidationException if key has huge negative number', function(done) {
      assertValidation({TableName: 'aaa', Key: {a: {N: '-1e126'}}},
        'Number overflow. Attempting to store a number with magnitude larger than supported range', done)
    })

    it('should return ValidationException if key has tiny positive number', function(done) {
      assertValidation({TableName: 'aaa', Key: {a: {N: '1e-131'}}},
        'Number underflow. Attempting to store a number with magnitude smaller than supported range', done)
    })

    it('should return ValidationException if key has tiny negative number', function(done) {
      assertValidation({TableName: 'aaa', Key: {a: {N: '-1e-131'}}},
        'Number underflow. Attempting to store a number with magnitude smaller than supported range', done)
    })

    it('should return ValidationException if key has incorrect numeric type in set', function(done) {
      assertValidation({TableName: 'abc', Key: {a: {NS: ['1', 'b', 'a']}}},
        'The parameter cannot be converted to a numeric value: b', done)
    })

    it('should return ValidationException duplicate values in AttributesToGet', function(done) {
      assertValidation({TableName: 'abc', Key: {}, AttributesToGet: ['a', 'a']},
        'One or more parameter values were invalid: Duplicate value in attribute name: a', done)
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

    it('should return ValidationException if hash key is too big', function(done) {
      var keyStr = (helpers.randomString() + new Array(2048).join('a')).slice(0, 2049)
      assertValidation({TableName: helpers.testHashTable, Key: {a: {S: keyStr}}},
        'One or more parameter values were invalid: ' +
        'Size of hashkey has exceeded the maximum size limit of2048 bytes', done)
    })

    it('should return ValidationException if range key is too big', function(done) {
      var keyStr = (helpers.randomString() + new Array(1024).join('a')).slice(0, 1025)
      assertValidation({TableName: helpers.testRangeTable, Key: {a: {S: 'a'}, b: {S: keyStr}}},
        'One or more parameter values were invalid: ' +
        'Aggregated size of all range keys has exceeded the size limit of 1024 bytes', done)
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
        helpers.deleteWhenActive(table.TableName)
      })
    })

  })

  describe('functionality', function() {

    var hashItem = {a: {S: helpers.randomString()}, b: {S: 'a'}, g: {N: '23'}},
        rangeItem = {a: {S: helpers.randomString()}, b: {S: helpers.randomString()}, g: {N: '23'}}

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
      var req = {TableName: helpers.testHashTable, Key: {a: {S: helpers.randomString()}}, ReturnConsumedCapacity: 'TOTAL'}
      request(opts(req), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({ConsumedCapacity: {CapacityUnits: 0.5, TableName: helpers.testHashTable}})
        req.ReturnConsumedCapacity = 'INDEXES'
        request(opts(req), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ConsumedCapacity: {CapacityUnits: 0.5, Table: {CapacityUnits: 0.5}, TableName: helpers.testHashTable}})
          done()
        })
      })
    })

    it('should return full ConsumedCapacity if specified', function(done) {
      var req = {TableName: helpers.testHashTable, Key: {a: {S: helpers.randomString()}}, ReturnConsumedCapacity: 'TOTAL', ConsistentRead: true}
      request(opts(req), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({ConsumedCapacity: {CapacityUnits: 1, TableName: helpers.testHashTable}})
        req.ReturnConsumedCapacity = 'INDEXES'
        request(opts(req), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ConsumedCapacity: {CapacityUnits: 1, Table: {CapacityUnits: 1}, TableName: helpers.testHashTable}})
          done()
        })
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
      request(opts({TableName: helpers.testHashTable, Key: {a: hashItem.a}, AttributesToGet: ['b', 'g'], ConsistentRead: true}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({Item: {b: hashItem.b, g: hashItem.g}})
        done()
      })
    })

    it('should return ConsumedCapacity for small item with no ConsistentRead', function(done) {
      var a = helpers.randomString(), b = new Array(4082 - a.length).join('b'),
        item = {a: {S: a}, b: {S: b}, c: {N: '12.3456'}, d: {B: 'AQI='}, e: {BS: ['AQI=', 'Ag==', 'AQ==']}}
      request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({TableName: helpers.testHashTable, Key: {a: item.a}, ReturnConsumedCapacity: 'TOTAL'}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.ConsumedCapacity.should.eql({CapacityUnits: 0.5, TableName: helpers.testHashTable})
          done()
        })
      })
    })

    it('should return ConsumedCapacity for larger item with no ConsistentRead', function(done) {
      var a = helpers.randomString(), b = new Array(4084 - a.length).join('b'),
        item = {a: {S: a}, b: {S: b}, c: {N: '12.3456'}, d: {B: 'AQI='}, e: {BS: ['AQI=', 'Ag==']}}
      request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({TableName: helpers.testHashTable, Key: {a: item.a}, ReturnConsumedCapacity: 'TOTAL'}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.ConsumedCapacity.should.eql({CapacityUnits: 1, TableName: helpers.testHashTable})
          done()
        })
      })
    })

    it('should return ConsumedCapacity for small item with ConsistentRead', function(done) {
      var a = helpers.randomString(), b = new Array(4082 - a.length).join('b'),
        item = {a: {S: a}, b: {S: b}, c: {N: '12.3456'}, d: {B: 'AQI='}, e: {BS: ['AQI=', 'Ag==', 'AQ==']}}
      request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({TableName: helpers.testHashTable, Key: {a: item.a}, ReturnConsumedCapacity: 'TOTAL', ConsistentRead: true}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.ConsumedCapacity.should.eql({CapacityUnits: 1, TableName: helpers.testHashTable})
          done()
        })
      })
    })

    it('should return ConsumedCapacity for larger item with ConsistentRead', function(done) {
      var a = helpers.randomString(), b = new Array(4084 - a.length).join('b'),
        item = {a: {S: a}, b: {S: b}, c: {N: '12.3456'}, d: {B: 'AQI='}, e: {BS: ['AQI=', 'Ag==']}}
      request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({TableName: helpers.testHashTable, Key: {a: item.a}, ReturnConsumedCapacity: 'TOTAL', ConsistentRead: true}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.ConsumedCapacity.should.eql({CapacityUnits: 2, TableName: helpers.testHashTable})
          done()
        })
      })
    })

  })

})


