var async = require('async'),
    helpers = require('./helpers'),
    should = require('should')

var target = 'PutItem',
    request = helpers.request,
    randomName = helpers.randomName,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target),
    assertConditional = helpers.assertConditional.bind(null, target)

describe('putItem', function() {

  describe('serializations', function() {

    it('should return SerializationException when TableName is not a string', function(done) {
      assertType('TableName', 'String', done)
    })

    it('should return SerializationException when Item is not a map', function(done) {
      assertType('Item', 'Map', done)
    })

    it('should return SerializationException when Item.Attr is not a struct', function(done) {
      assertType('Item.Attr', 'Structure', done)
    })

    it('should return SerializationException when Item.Attr.S is not a string', function(done) {
      assertType('Item.Attr.S', 'String', done)
    })

    it('should return SerializationException when Item.Attr.B is not a blob', function(done) {
      assertType('Item.Attr.B', 'Blob', done)
    })

    it('should return SerializationException when Item.Attr.N is not a string', function(done) {
      assertType('Item.Attr.N', 'String', done)
    })

    it('should return SerializationException when Item.Attr.SS is not a list', function(done) {
      assertType('Item.Attr.SS', 'List', done)
    })

    it('should return SerializationException when Item.Attr.SS.0 is not a string', function(done) {
      assertType('Item.Attr.SS.0', 'String', done)
    })

    it('should return SerializationException when Item.Attr.NS is not a list', function(done) {
      assertType('Item.Attr.NS', 'List', done)
    })

    it('should return SerializationException when Item.Attr.NS.0 is not a string', function(done) {
      assertType('Item.Attr.NS.0', 'String', done)
    })

    it('should return SerializationException when Item.Attr.BS is not a list', function(done) {
      assertType('Item.Attr.BS', 'List', done)
    })

    it('should return SerializationException when Item.Attr.BS.0 is not a blob', function(done) {
      assertType('Item.Attr.BS.0', 'Blob', done)
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
        '2 validation errors detected: ' +
        'Value null at \'tableName\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'item\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for empty TableName', function(done) {
      assertValidation({TableName: ''},
        '3 validation errors detected: ' +
        'Value \'\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 3; ' +
        'Value null at \'item\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for short TableName', function(done) {
      assertValidation({TableName: 'a;'},
        '3 validation errors detected: ' +
        'Value \'a;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'a;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 3; ' +
        'Value null at \'item\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for long TableName', function(done) {
      var name = '', i
      for (i = 0; i < 256; i++) name += 'a'
      assertValidation({TableName: name},
        '2 validation errors detected: ' +
        'Value \'' + name + '\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 255; ' +
        'Value null at \'item\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for incorrect attributes', function(done) {
      assertValidation({TableName: 'abc;', ReturnConsumedCapacity: 'hi',
        ReturnItemCollectionMetrics: 'hi', ReturnValues: 'hi'},
        '5 validation errors detected: ' +
        'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [INDEXES, TOTAL, NONE]; ' +
        'Value \'abc;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value null at \'item\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value \'hi\' at \'returnValues\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [ALL_NEW, UPDATED_OLD, ALL_OLD, NONE, UPDATED_NEW]; ' +
        'Value \'hi\' at \'returnItemCollectionMetrics\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [SIZE, NONE]', done)
    })

    it('should return ValidationException if no value and no exists', function(done) {
      assertValidation({TableName: 'abc', Item: {}, Expected: {a: {}}},
        'One or more parameter values were invalid: ' +
        '\'Exists\' is set to null. ' +
        '\'Exists\' must be set to false when no Attribute value is specified', done)
    })

    it('should return ValidationException for Exists true with no value', function(done) {
      assertValidation({TableName: 'abc', Item: {}, Expected: {a: {Exists: true}}},
        'One or more parameter values were invalid: ' +
        '\'Exists\' is set to true. ' +
        '\'Exists\' must be set to false when no Attribute value is specified', done)
    })

    it('should return ValidationException for Exists false with value', function(done) {
      assertValidation({TableName: 'abc', Item: {}, Expected: {a: {Exists: false, Value: {S: 'a'}}}},
        'One or more parameter values were invalid: ' +
        'Cannot expect an attribute to have a specified value while expecting it to not exist', done)
    })

    it('should return ValidationException for incorrect ReturnValues', function(done) {
      async.forEach(['UPDATED_OLD', 'ALL_NEW', 'UPDATED_NEW'], function(returnValues, cb) {
        assertValidation({TableName: 'abc', Item: {}, ReturnValues: returnValues},
          'ReturnValues can only be ALL_OLD or NONE', cb)
      }, done)
    })

    it('should return ValidationException for empty key type', function(done) {
      assertValidation({TableName: 'abc', Item: {a: {}}},
        'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException for bad key type', function(done) {
      assertValidation({TableName: 'abc', Item: {a: {a: ''}}},
        'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException for empty string', function(done) {
      assertValidation({TableName: 'abc', Item: {a: {S: ''}}},
        'One or more parameter values were invalid: An AttributeValue may not contain an empty string.', done)
    })

    it('should return ValidationException for empty binary', function(done) {
      assertValidation({TableName: 'abc', Item: {a: {B: ''}}},
        'One or more parameter values were invalid: An AttributeValue may not contain an empty binary type.', done)
    })

    // Somehow allows set types for keys
    it('should return ValidationException for empty set key', function(done) {
      assertValidation({TableName: 'abc', Item: {a: {SS: []}}},
        'One or more parameter values were invalid: An AttributeValue may not contain an empty set.', done)
    })

    it('should return ValidationException for empty string in set', function(done) {
      assertValidation({TableName: 'abc', Item: {a: {SS: ['a', '']}}},
        'One or more parameter values were invalid: An AttributeValue may not contain an empty string.', done)
    })

    it('should return ValidationException for empty binary in set', function(done) {
      assertValidation({TableName: 'abc', Item: {a: {BS: ['aaaa', '']}}},
        'One or more parameter values were invalid: Binary sets may not contain null or empty values', done)
    })

    it('should return ValidationException if key has empty numeric in set', function(done) {
      assertValidation({TableName: 'abc', Item: {a: {NS: ['1', '']}}},
        'The parameter cannot be converted to a numeric value', done)
    })

    it('should return ValidationException for duplicate string in set', function(done) {
      assertValidation({TableName: 'abc', Item: {a: {SS: ['a', 'a']}}},
        'One or more parameter values were invalid: Input collection [a, a] contains duplicates.', done)
    })

    it('should return ValidationException for duplicate number in set', function(done) {
      assertValidation({TableName: 'abc', Item: {a: {NS: ['1', '1']}}},
        'Input collection contains duplicates', done)
    })

    it('should return ValidationException for duplicate binary in set', function(done) {
      assertValidation({TableName: 'abc', Item: {a: {BS: ['Yg==', 'Yg==']}}},
        'One or more parameter values were invalid: Input collection [Yg==, Yg==]of type BS contains duplicates.', done)
    })

    it('should return ValidationException for multiple types', function(done) {
      assertValidation({TableName: 'abc', Item: {a: {S: 'a', N: '1'}}},
        'Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException if key has empty numeric type', function(done) {
      assertValidation({TableName: 'abc', Item: {a: {N: ''}}},
        'The parameter cannot be converted to a numeric value', done)
    })

    it('should return ValidationException if key has incorrect numeric type', function(done) {
      assertValidation({TableName: 'abc', Item: {a: {N: 'b'}}},
        'The parameter cannot be converted to a numeric value: b', done)
    })

    it('should return ValidationException if key has incorrect numeric type in set', function(done) {
      assertValidation({TableName: 'abc', Item: {a: {NS: ['1', 'b', 'a']}}},
        'The parameter cannot be converted to a numeric value: b', done)
    })

    it('should return ValidationException for empty expected value', function(done) {
      assertValidation({TableName: 'abc', Item: {}, Expected: {a: {Value: {}}}},
        'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException if expected value has empty numeric in set', function(done) {
      assertValidation({TableName: 'abc', Item: {}, Expected: {a: {Value: {NS: ['1', '']}}}},
        'The parameter cannot be converted to a numeric value', done)
    })

    it('should return ResourceNotFoundException if key is empty and table does not exist', function(done) {
      assertNotFound({TableName: helpers.randomString(), Item: {}},
        'Requested resource not found', done)
    })

    it('should return ValidationException if key is empty and table does exist', function(done) {
      assertValidation({TableName: helpers.testHashTable, Item: {}},
        'One or more parameter values were invalid: Missing the key a in the item', done)
    })

    it('should return ValidationException if key has incorrect attributes', function(done) {
      assertValidation({TableName: helpers.testHashTable, Item: {b: {S: 'a'}}},
        'One or more parameter values were invalid: Missing the key a in the item', done)
    })

    it('should return ValidationException if key is incorrect binary type', function(done) {
      assertValidation({TableName: helpers.testHashTable, Item: {a: {B: 'abcd'}}},
        'One or more parameter values were invalid: Type mismatch for key a expected: S actual: B', done)
    })

    it('should return ValidationException if key is incorrect numeric type', function(done) {
      assertValidation({TableName: helpers.testHashTable, Item: {a: {N: '1'}}},
        'One or more parameter values were invalid: Type mismatch for key a expected: S actual: N', done)
    })

    it('should return ValidationException if key is incorrect string set type', function(done) {
      assertValidation({TableName: helpers.testHashTable, Item: {a: {SS: ['a']}}},
        'One or more parameter values were invalid: Type mismatch for key a expected: S actual: SS', done)
    })

    it('should return ValidationException if key is incorrect numeric set type', function(done) {
      assertValidation({TableName: helpers.testHashTable, Item: {a: {NS: ['1']}}},
        'One or more parameter values were invalid: Type mismatch for key a expected: S actual: NS', done)
    })

    it('should return ValidationException if key is incorrect binary set type', function(done) {
      assertValidation({TableName: helpers.testHashTable, Item: {a: {BS: ['aaaa']}}},
        'One or more parameter values were invalid: Type mismatch for key a expected: S actual: BS', done)
    })

    it('should return ValidationException if missing range key', function(done) {
      assertValidation({TableName: helpers.testRangeTable, Item: {a: {S: 'a'}}},
        'One or more parameter values were invalid: Missing the key b in the item', done)
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
        assertNotFound({TableName: table.TableName, Item: {a: {S: 'a'}}},
          'Requested resource not found', done)
      })
    })
  })

  describe('functionality', function() {

    it('should put basic item', function(done) {
      var item = {a: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({})
        request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item.a}, ConsistentRead: true}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({Item: item})
          done()
        })
      })
    })

    it('should put multi attribute item', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'a'}, c: {S: 'a'}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({})
        request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item.a}, ConsistentRead: true}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({Item: item})
          done()
        })
      })
    })

    it('should return empty when there are no old values', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'a'}, c: {S: 'a'}}
      request(opts({TableName: helpers.testHashTable, Item: item, ReturnValues: 'ALL_OLD'}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({})
        done()
      })
    })

    it('should return correct old values when they exist', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'a'}, c: {S: 'a'}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err) {
        if (err) return done(err)
        item.b.S = 'b'
        request(opts({TableName: helpers.testHashTable, Item: item, ReturnValues: 'ALL_OLD'}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          item.b.S = 'a'
          res.body.should.eql({Attributes: item})
          done()
        })
      })
    })

    it('should put basic range item', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'a'}, c: {S: 'a'}}
      request(opts({TableName: helpers.testRangeTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({})
        // Put another item with the same hash key to prove we're retrieving the correct one
        request(opts({TableName: helpers.testRangeTable, Item: {a: item.a, b: {S: 'b'}}}), function(err, res) {
          if (err) return done(err)
          request(helpers.opts('GetItem', {TableName: helpers.testRangeTable, Key: {a: item.a, b: item.b}, ConsistentRead: true}), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({Item: item})
            done()
          })
        })
      })
    })

    it('should return ConditionalCheckFailedException if expecting non-existent key to exist', function(done) {
      assertConditional({
        TableName: helpers.testHashTable,
        Item: {a: {S: helpers.randomString()}},
        Expected: {a: {Value: {S: helpers.randomString()}}},
      }, done)
    })

    it('should return ConditionalCheckFailedException if expecting existing key to not exist', function(done) {
      var item = {a: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err) {
        if (err) return done(err)
        assertConditional({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {Exists: false}},
        }, done)
      })
    })

    it('should succeed if conditional key is different and exists is false', function(done) {
      var item = {a: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err) {
        if (err) return done(err)
        request(opts({
          TableName: helpers.testHashTable,
          Item: {a: {S: helpers.randomString()}},
          Expected: {a: {Exists: false}},
        }), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({})
          done()
        })
      })
    })

    it('should succeed if conditional key is same and exists is true', function(done) {
      var item = {a: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err) {
        if (err) return done(err)
        request(opts({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {Value: item.a}},
        }), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({})
          done()
        })
      })
    })

    it('should succeed if expecting non-existant value to not exist', function(done) {
      var item = {a: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err) {
        if (err) return done(err)
        request(opts({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {b: {Exists: false}},
        }), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({})
          done()
        })
      })
    })

    it('should return ConditionalCheckFailedException if expecting existing value to not exist if different value specified', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err) {
        if (err) return done(err)
        assertConditional({
          TableName: helpers.testHashTable,
          Item: {a: item.a, b: {S: helpers.randomString()}},
          Expected: {b: {Exists: false}},
        }, done)
      })
    })

    it('should return ConditionalCheckFailedException if expecting existing value to not exist if value not specified', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err) {
        if (err) return done(err)
        assertConditional({
          TableName: helpers.testHashTable,
          Item: {a: item.a},
          Expected: {b: {Exists: false}},
        }, done)
      })
    })

    it('should return ConditionalCheckFailedException if expecting existing value to not exist if same value specified', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err) {
        if (err) return done(err)
        assertConditional({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {b: {Exists: false}},
        }, done)
      })
    })

    it('should succeed for multiple conditional checks if all are valid', function(done) {
      var item = {a: {S: helpers.randomString()}, c: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err) {
        if (err) return done(err)
        request(opts({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {Value: item.a}, b: {Exists: false}, c: {Value: item.c}},
        }), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({})
          done()
        })
      })
    })

    it('should return ConditionalCheckFailedException for multiple conditional checks if one is invalid', function(done) {
      var item = {a: {S: helpers.randomString()}, c: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err) {
        if (err) return done(err)
        assertConditional({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {Value: item.a}, b: {Exists: false}, c: {Value: {S: helpers.randomString()}}},
        }, done)
      })
    })

  })

})


