var helpers = require('./helpers')

var target = 'UpdateItem',
    request = helpers.request,
    randomName = helpers.randomName,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target),
    assertConditional = helpers.assertConditional.bind(null, target)

describe('updateItem', function() {

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

    it('should return SerializationException when Expected is not a map', function(done) {
      assertType('Expected', 'Map', done)
    })

    it('should return SerializationException when Expected.Attr is not a struct', function(done) {
      assertType('Expected.Attr', 'Structure', done)
    })

    it('should return SerializationException when Expected.Attr.Exists is not a boolean', function(done) {
      assertType('Expected.Attr.Exists', 'Boolean', done)
    })

    it('should return SerializationException when Expected.Attr.Value is not a struct', function(done) {
      assertType('Expected.Attr.Value', 'Structure', done)
    })

    it('should return SerializationException when Expected.Attr.Value.S is not a string', function(done) {
      assertType('Expected.Attr.Value.S', 'String', done)
    })

    it('should return SerializationException when Expected.Attr.Value.B is not a blob', function(done) {
      assertType('Expected.Attr.Value.B', 'Blob', done)
    })

    it('should return SerializationException when Expected.Attr.Value.N is not a string', function(done) {
      assertType('Expected.Attr.Value.N', 'String', done)
    })

    it('should return SerializationException when Expected.Attr.Value.SS is not a list', function(done) {
      assertType('Expected.Attr.Value.SS', 'List', done)
    })

    it('should return SerializationException when Expected.Attr.Value.SS.0 is not a string', function(done) {
      assertType('Expected.Attr.Value.SS.0', 'String', done)
    })

    it('should return SerializationException when Expected.Attr.Value.NS is not a list', function(done) {
      assertType('Expected.Attr.Value.NS', 'List', done)
    })

    it('should return SerializationException when Expected.Attr.Value.NS.0 is not a string', function(done) {
      assertType('Expected.Attr.Value.NS.0', 'String', done)
    })

    it('should return SerializationException when Expected.Attr.Value.BS is not a list', function(done) {
      assertType('Expected.Attr.Value.BS', 'List', done)
    })

    it('should return SerializationException when Expected.Attr.Value.BS.0 is not a blob', function(done) {
      assertType('Expected.Attr.Value.BS.0', 'Blob', done)
    })

    it('should return SerializationException when AttributeUpdates is not a map', function(done) {
      assertType('AttributeUpdates', 'Map', done)
    })

    it('should return SerializationException when AttributeUpdates.Attr is not a struct', function(done) {
      assertType('AttributeUpdates.Attr', 'Structure', done)
    })

    it('should return SerializationException when AttributeUpdates.Attr.Action is not a string', function(done) {
      assertType('AttributeUpdates.Attr.Action', 'String', done)
    })

    it('should return SerializationException when AttributeUpdates.Attr.Value is not a struct', function(done) {
      assertType('AttributeUpdates.Attr.Value', 'Structure', done)
    })

    it('should return SerializationException when AttributeUpdates.Attr.Value.S is not a string', function(done) {
      assertType('AttributeUpdates.Attr.Value.S', 'String', done)
    })

    it('should return SerializationException when AttributeUpdates.Attr.Value.B is not a blob', function(done) {
      assertType('AttributeUpdates.Attr.Value.B', 'Blob', done)
    })

    it('should return SerializationException when AttributeUpdates.Attr.Value.N is not a string', function(done) {
      assertType('AttributeUpdates.Attr.Value.N', 'String', done)
    })

    it('should return SerializationException when AttributeUpdates.Attr.Value.SS is not a list', function(done) {
      assertType('AttributeUpdates.Attr.Value.SS', 'List', done)
    })

    it('should return SerializationException when AttributeUpdates.Attr.Value.SS.0 is not a string', function(done) {
      assertType('AttributeUpdates.Attr.Value.SS.0', 'String', done)
    })

    it('should return SerializationException when AttributeUpdates.Attr.Value.NS is not a list', function(done) {
      assertType('AttributeUpdates.Attr.Value.NS', 'List', done)
    })

    it('should return SerializationException when AttributeUpdates.Attr.Value.NS.0 is not a string', function(done) {
      assertType('AttributeUpdates.Attr.Value.NS.0', 'String', done)
    })

    it('should return SerializationException when AttributeUpdates.Attr.Value.BS is not a list', function(done) {
      assertType('AttributeUpdates.Attr.Value.BS', 'List', done)
    })

    it('should return SerializationException when AttributeUpdates.Attr.Value.BS.0 is not a blob', function(done) {
      assertType('AttributeUpdates.Attr.Value.BS.0', 'Blob', done)
    })

    it('should return SerializationException when ReturnConsumedCapacity is not a string', function(done) {
      assertType('ReturnConsumedCapacity', 'String', done)
    })

    it('should return SerializationException when ReturnItemCollectionMetrics is not a string', function(done) {
      assertType('ReturnItemCollectionMetrics', 'String', done)
    })

    it('should return SerializationException when ReturnValues is not a string', function(done) {
      assertType('ReturnValues', 'String', done)
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
      assertValidation({TableName: 'abc;', ReturnConsumedCapacity: 'hi',
        ReturnItemCollectionMetrics: 'hi', ReturnValues: 'hi'},
        '5 validation errors detected: ' +
        'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [TOTAL, NONE]; ' +
        'Value \'abc;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'hi\' at \'returnItemCollectionMetrics\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [SIZE, NONE]; ' +
        'Value \'hi\' at \'returnValues\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [ALL_NEW, UPDATED_OLD, ALL_OLD, NONE, UPDATED_NEW]; ' +
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

    it('should return ValidationException for empty update value', function(done) {
      assertValidation({TableName: 'abc', Key: {a: {S: 'a'}}, AttributeUpdates: {a: {Value: {}}}},
        'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException if update value has empty numeric in set', function(done) {
      assertValidation({TableName: 'abc', Key: {a: {S: 'a'}}, AttributeUpdates: {a: {Value: {NS: ['1', '']}}}},
        'The parameter cannot be converted to a numeric value', done)
    })

    it('should return ValidationException for empty expected value', function(done) {
      assertValidation({TableName: 'abc', Key: {a: {S: 'a'}}, Expected: {a: {Value: {}}}},
        'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException if expected value has empty numeric in set', function(done) {
      assertValidation({TableName: 'abc', Key: {a: {S: 'a'}}, Expected: {a: {Value: {NS: ['1', '']}}}},
        'The parameter cannot be converted to a numeric value', done)
    })

    it('should return ValidationException if update has no value', function(done) {
      assertValidation({
        TableName: helpers.testHashTable,
        Key: {a: {S: helpers.randomString()}},
        AttributeUpdates: {a: {x: 'whatever'}},
      }, 'One or more parameter values were invalid: ' +
        'Only DELETE action is allowed when no attribute value is specified', done)
    })

    it('should return ValidationException if trying to delete type S', function(done) {
      assertValidation({
        TableName: helpers.testHashTable,
        Key: {a: {S: helpers.randomString()}},
        AttributeUpdates: {a: {Action: 'DELETE', Value: {S: helpers.randomString()}}},
      }, 'One or more parameter values were invalid: ' +
        'Action DELETE is not supported for the type S', done)
    })

    it('should return ValidationException if trying to delete type N', function(done) {
      assertValidation({
        TableName: helpers.testHashTable,
        Key: {a: {S: helpers.randomString()}},
        AttributeUpdates: {a: {Action: 'DELETE', Value: {N: helpers.randomString()}}},
      }, 'One or more parameter values were invalid: ' +
        'Action DELETE is not supported for the type N', done)
    })

    it('should return ValidationException if trying to delete type B', function(done) {
      assertValidation({
        TableName: helpers.testHashTable,
        Key: {a: {S: helpers.randomString()}},
        AttributeUpdates: {a: {Action: 'DELETE', Value: {B: 'Yg=='}}},
      }, 'One or more parameter values were invalid: ' +
        'Action DELETE is not supported for the type B', done)
    })

    it('should return ValidationException if trying to update key', function(done) {
      assertValidation({
        TableName: helpers.testHashTable,
        Key: {a: {S: helpers.randomString()}},
        AttributeUpdates: {a: {Value: {S: helpers.randomString()}}},
      }, 'One or more parameter values were invalid: ' +
        'Cannot update attribute a. This attribute is part of the key', done)
    })

  })

  describe('functionality', function() {

    it('should return ConditionalCheckFailedException if expecting non-existent key to exist', function(done) {
      assertConditional({
        TableName: helpers.testHashTable,
        Key: {a: {S: helpers.randomString()}},
        Expected: {a: {Value: {S: helpers.randomString()}}},
      }, done)
    })

    it('should just add item with key if no action', function(done) {
      var key = {a: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Key: key}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({})
        request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: key, ConsistentRead: true}), function(err, res) {
          res.statusCode.should.equal(200)
          res.body.should.eql({Item: key})
          done()
        })
      })
    })

    it('should return empty when there are no old values', function(done) {
      var key = {a: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Key: key, ReturnValues: 'ALL_OLD'}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({})
        done()
      })
    })

    it('should return all old values when they exist', function(done) {
      var key = {a: {S: helpers.randomString()}}
      var updates = {b: {Value: {S: 'a'}}}
      request(opts({TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates}), function(err) {
        if (err) return done(err)
        updates.b.Value.S = 'b'
        request(opts({TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates, ReturnValues: 'ALL_OLD'}), function(err, res) {
          res.statusCode.should.equal(200)
          res.body.should.eql({Attributes: {a: key.a, b: {S: 'a'}}})
          done()
        })
      })
    })

    it('should return updated old values when they exist', function(done) {
      var key = {a: {S: helpers.randomString()}}
      var updates = {b: {Value: {S: 'a'}}, c: {Value: {S: 'a'}}}
      request(opts({TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates}), function(err) {
        if (err) return done(err)
        updates.b.Value.S = 'b'
        request(opts({TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates, ReturnValues: 'UPDATED_OLD'}), function(err, res) {
          res.statusCode.should.equal(200)
          res.body.should.eql({Attributes: {b: {S: 'a'}, c: {S: 'a'}}})
          done()
        })
      })
    })

    it('should return all new values when they exist', function(done) {
      var key = {a: {S: helpers.randomString()}}
      var updates = {b: {Value: {S: 'a'}}}
      request(opts({TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates}), function(err) {
        if (err) return done(err)
        updates.b.Value.S = 'b'
        request(opts({TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates, ReturnValues: 'ALL_NEW'}), function(err, res) {
          res.statusCode.should.equal(200)
          res.body.should.eql({Attributes: {a: key.a, b: {S: 'b'}}})
          done()
        })
      })
    })

    it('should return updated new values when they exist', function(done) {
      var key = {a: {S: helpers.randomString()}}
      var updates = {b: {Value: {S: 'a'}}, c: {Value: {S: 'a'}}}
      request(opts({TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates}), function(err) {
        if (err) return done(err)
        updates.b.Value.S = 'b'
        request(opts({TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates, ReturnValues: 'UPDATED_NEW'}), function(err, res) {
          res.statusCode.should.equal(200)
          res.body.should.eql({Attributes: {b: {S: 'b'}, c: {S: 'a'}}})
          done()
        })
      })
    })

  })
})


