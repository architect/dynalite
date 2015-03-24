var helpers = require('./helpers')

var target = 'DeleteItem',
    request = helpers.request,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertConditional = helpers.assertConditional.bind(null, target)

describe('deleteItem', function() {

  describe('serializations', function() {

    it('should return SerializationException when TableName is not a string', function(done) {
      assertType('TableName', 'String', done)
    })

    it('should return SerializationException when Key is not a map', function(done) {
      assertType('Key', 'Map', done)
    })

    it('should return SerializationException when Key.Attr is not an attr struct', function(done) {
      assertType('Key.Attr', 'AttrStructure', done)
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

    it('should return SerializationException when Expected.Attr.Value is not an attr struct', function(done) {
      assertType('Expected.Attr.Value', 'AttrStructure', done)
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
      assertValidation({TableName: 'abc;', ReturnConsumedCapacity: 'hi',
        ReturnItemCollectionMetrics: 'hi', ReturnValues: 'hi'},
        '5 validation errors detected: ' +
        'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [INDEXES, TOTAL, NONE]; ' +
        'Value \'abc;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'hi\' at \'returnValues\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [ALL_NEW, UPDATED_OLD, ALL_OLD, NONE, UPDATED_NEW]; ' +
        'Value \'hi\' at \'returnItemCollectionMetrics\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [SIZE, NONE]; ' +
        'Value null at \'key\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for bad key type', function(done) {
      assertValidation({TableName: 'abc', Key: {a: {a: ''}}},
        'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException for empty key', function(done) {
      assertValidation({TableName: 'abc', Key: {a: {S: ''}}},
        'One or more parameter values were invalid: An AttributeValue may not contain an empty string', done)
    })

    it('should return empty response if key has incorrect numeric type', function(done) {
      assertValidation({TableName: 'abc', Key: {a: {N: 'b'}}},
        'The parameter cannot be converted to a numeric value: b', done)
    })

    it('should return ValidationException if ComparisonOperator used alone', function(done) {
      assertValidation({TableName: 'aaa', Key: {}, Expected: {a: {ComparisonOperator: 'LT'}}},
        'One or more parameter values were invalid: Value or AttributeValueList must be used with ComparisonOperator: LT for Attribute: a', done)
    })

    it('should return ValidationException if ComparisonOperator and Exists are used together', function(done) {
      assertValidation({TableName: 'aaa', Key: {}, Expected: {a: { Exists: true, ComparisonOperator: 'LT'}}},
        'One or more parameter values were invalid: Exists and ComparisonOperator cannot be used together for Attribute: a', done)
    })

    it('should return ValidationException if AttributeValueList is used alone', function(done) {
      assertValidation({TableName: 'aaa', Key: {}, Expected: {a: { AttributeValueList: []}}},
        'One or more parameter values were invalid: AttributeValueList can only be used with a ComparisonOperator for Attribute: a', done)
    })

    it('should return ValidationException if AttributeValueList and Exists are used together', function(done) {
      assertValidation({TableName: 'aaa', Key: {}, Expected: {a: { Exists: true, AttributeValueList: []}}},
        'One or more parameter values were invalid: AttributeValueList can only be used with a ComparisonOperator for Attribute: a', done)
    })

    it('should return ValidationException if AttributeValueList and Value are used together', function(done) {
      assertValidation({TableName: 'aaa', Key: {}, Expected: {a: { Value: {S: 'a'}, AttributeValueList: []}}},
        'One or more parameter values were invalid: Value and AttributeValueList cannot be used together for Attribute: a', done)
    })

    it('should return ValidationException if Value provides incorrect number of attributes: BETWEEN', function(done) {
      var expected = { a: {
        Value: {S:'a'},
        ComparisonOperator: 'BETWEEN'
      } }
      assertValidation({TableName: 'aaa', Key: {}, Expected: expected},
        'One or more parameter values were invalid: Invalid number of argument(s) for the BETWEEN ComparisonOperator', done)
    })

    it('should return ValidationException if Value provides incorrect number of attributes: NULL', function(done) {
      var expected = { a: {
        Value: {S:'a'},
        ComparisonOperator: 'NULL'
      } }
      assertValidation({TableName: 'aaa', Key: {}, Expected: expected},
        'One or more parameter values were invalid: Invalid number of argument(s) for the NULL ComparisonOperator', done)
    })

  })

  describe('functionality', function() {

    it('should return nothing if item does not exist', function(done) {
      request(opts({TableName: helpers.testHashTable, Key: {a: {S: helpers.randomString()}}}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({})
        done()
      })
    })

    it('should return ConsumedCapacity if specified and item does not exist', function(done) {
      request(opts({TableName: helpers.testHashTable, Key: {a: {S: helpers.randomString()}}, ReturnConsumedCapacity: 'TOTAL'}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({ConsumedCapacity: {CapacityUnits: 1, TableName: helpers.testHashTable}})
        done()
      })
    })

    it('should delete item successfully', function(done) {
      var item = {a: {S: helpers.randomString()}}
      request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({TableName: helpers.testHashTable, Key: {a: item.a}}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({})
          request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item.a}, ConsistentRead: true}), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            done()
          })
        })
      })
    })

    it('should delete item successfully and return old values', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'b'}}
      request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({TableName: helpers.testHashTable, Key: {a: item.a}, ReturnValues: 'ALL_OLD'}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({Attributes: item})
          done()
        })
      })
    })

    it('should return ConditionalCheckFailedException if expecting non-existent key to exist', function(done) {
      assertConditional({
        TableName: helpers.testHashTable,
        Key: {a: {S: helpers.randomString()}},
        Expected: {a: {Value: {S: helpers.randomString()}}},
      }, done)
    })

    it('should return ConditionalCheckFailedException if expecting existing key to not exist', function(done) {
      var item = {a: {S: helpers.randomString()}}
      request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err) {
        if (err) return done(err)
        assertConditional({
          TableName: helpers.testHashTable,
          Key: {a: item.a},
          Expected: {a: {Exists: false}},
        }, done)
      })
    })

    it('should succeed if conditional key is different and exists is false', function(done) {
      var item = {a: {S: helpers.randomString()}}
      request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err) {
        if (err) return done(err)
        request(opts({
          TableName: helpers.testHashTable,
          Key: {a: {S: helpers.randomString()}},
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
      request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err) {
        if (err) return done(err)
        request(opts({
          TableName: helpers.testHashTable,
          Key: {a: item.a},
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
      request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err) {
        if (err) return done(err)
        request(opts({
          TableName: helpers.testHashTable,
          Key: {a: item.a},
          Expected: {b: {Exists: false}},
        }), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({})
          done()
        })
      })
    })

    it('should return ConditionalCheckFailedException if expecting existing value to not exist', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: helpers.randomString()}}
      request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err) {
        if (err) return done(err)
        assertConditional({
          TableName: helpers.testHashTable,
          Key: {a: item.a},
          Expected: {b: {Exists: false}},
        }, done)
      })
    })

    it('should succeed for multiple conditional checks if all are valid', function(done) {
      var item = {a: {S: helpers.randomString()}, c: {S: helpers.randomString()}}
      request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err) {
        if (err) return done(err)
        request(opts({
          TableName: helpers.testHashTable,
          Key: {a: item.a},
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
      request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err) {
        if (err) return done(err)
        assertConditional({
          TableName: helpers.testHashTable,
          Key: {a: item.a},
          Expected: {a: {Value: item.a}, b: {Exists: false}, c: {Value: {S: helpers.randomString()}}},
        }, done)
      })
    })

    it('should return ConsumedCapacity for small item', function(done) {
      var a = helpers.randomString(), b = new Array(1010 - a.length).join('b'),
        item = {a: {S: a}, b: {S: b}, c: {N: '12.3456'}, d: {B: 'AQI='}, e: {BS: ['AQI=', 'Ag==', 'AQ==']}}
      request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({TableName: helpers.testHashTable, Key: {a: item.a}, ReturnConsumedCapacity: 'TOTAL'}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ConsumedCapacity: {CapacityUnits: 1, TableName: helpers.testHashTable}})
          done()
        })
      })
    })

    it('should return ConsumedCapacity for larger item', function(done) {
      var a = helpers.randomString(), b = new Array(1012 - a.length).join('b'),
        item = {a: {S: a}, b: {S: b}, c: {N: '12.3456'}, d: {B: 'AQI='}, e: {BS: ['AQI=', 'Ag==']}}
      request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({TableName: helpers.testHashTable, Key: {a: item.a}, ReturnConsumedCapacity: 'TOTAL'}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ConsumedCapacity: {CapacityUnits: 2, TableName: helpers.testHashTable}})
          done()
        })
      })
    })

  })
})


