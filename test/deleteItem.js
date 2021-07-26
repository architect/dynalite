var async = require('async'),
    helpers = require('./helpers')

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
      assertType('Key', 'Map<AttributeValue>', done)
    })

    it('should return SerializationException when Key.Attr is not an attr struct', function(done) {
      this.timeout(60000)
      assertType('Key.Attr', 'AttrStruct<ValueStruct>', done)
    })

    it('should return SerializationException when Expected is not a map', function(done) {
      assertType('Expected', 'Map<ExpectedAttributeValue>', done)
    })

    it('should return SerializationException when Expected.Attr is not a struct', function(done) {
      assertType('Expected.Attr', 'ValueStruct<ExpectedAttributeValue>', done)
    })

    it('should return SerializationException when Expected.Attr.Exists is not a boolean', function(done) {
      assertType('Expected.Attr.Exists', 'Boolean', done)
    })

    it('should return SerializationException when Expected.Attr.Value is not an attr struct', function(done) {
      this.timeout(60000)
      assertType('Expected.Attr.Value', 'AttrStruct<FieldStruct>', done)
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

    it('should return SerializationException when ConditionExpression is not a string', function(done) {
      assertType('ConditionExpression', 'String', done)
    })

    it('should return SerializationException when ExpressionAttributeValues is not a map', function(done) {
      assertType('ExpressionAttributeValues', 'Map<AttributeValue>', done)
    })

    it('should return SerializationException when ExpressionAttributeValues.Attr is not an attr struct', function(done) {
      this.timeout(60000)
      assertType('ExpressionAttributeValues.Attr', 'AttrStruct<ValueStruct>', done)
    })

    it('should return SerializationException when ExpressionAttributeNames is not a map', function(done) {
      assertType('ExpressionAttributeNames', 'Map<java.lang.String>', done)
    })

    it('should return SerializationException when ExpressionAttributeNames.Attr is not a string', function(done) {
      assertType('ExpressionAttributeNames.Attr', 'String', done)
    })

  })

  describe('validations', function() {

    it('should return ValidationException for no TableName', function(done) {
      assertValidation({}, [
        'Value null at \'tableName\' failed to satisfy constraint: ' +
        'Member must not be null',
        'Value null at \'key\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for empty TableName', function(done) {
      assertValidation({TableName: ''}, [
        'Value \'\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        'Value \'\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 3',
        'Value null at \'key\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for short TableName', function(done) {
      assertValidation({TableName: 'a;'}, [
        'Value \'a;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        'Value \'a;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 3',
        'Value null at \'key\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for long TableName', function(done) {
      var name = new Array(256 + 1).join('a')
      assertValidation({TableName: name}, [
        'Value \'' + name + '\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 255',
        'Value null at \'key\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for incorrect attributes', function(done) {
      assertValidation({TableName: 'abc;', ReturnConsumedCapacity: 'hi',
        ReturnItemCollectionMetrics: 'hi', ReturnValues: 'hi'}, [
          'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
          'Member must satisfy enum value set: [INDEXES, TOTAL, NONE]',
          'Value \'abc;\' at \'tableName\' failed to satisfy constraint: ' +
          'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
          'Value \'hi\' at \'returnValues\' failed to satisfy constraint: ' +
          'Member must satisfy enum value set: [ALL_NEW, UPDATED_OLD, ALL_OLD, NONE, UPDATED_NEW]',
          'Value \'hi\' at \'returnItemCollectionMetrics\' failed to satisfy constraint: ' +
          'Member must satisfy enum value set: [SIZE, NONE]',
          'Value null at \'key\' failed to satisfy constraint: ' +
          'Member must not be null',
        ], done)
    })

    it('should return ValidationException if expression and non-expression', function(done) {
      assertValidation({
        TableName: 'abc',
        Key: {},
        Expected: {},
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
        ConditionExpression: '',
      }, 'Can not use both expression and non-expression parameters in the same request: ' +
        'Non-expression parameters: {Expected} Expression parameters: {ConditionExpression}', done)
    })

    it('should return ValidationException if ExpressionAttributeNames but no ConditionExpression', function(done) {
      assertValidation({
        TableName: 'abc',
        Key: {},
        Expected: {},
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
      }, 'ExpressionAttributeNames can only be specified when using expressions', done)
    })

    it('should return ValidationException if ExpressionAttributeValues but no ConditionExpression', function(done) {
      assertValidation({
        TableName: 'abc',
        Key: {},
        Expected: {},
        ExpressionAttributeValues: {},
      }, 'ExpressionAttributeValues can only be specified when using expressions: ConditionExpression is null', done)
    })

    it('should return ValidationException for empty ExpressionAttributeNames', function(done) {
      assertValidation({
        TableName: 'abc',
        Key: {},
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
        ConditionExpression: '',
      }, 'ExpressionAttributeNames must not be empty', done)
    })

    it('should return ValidationException for invalid ExpressionAttributeNames', function(done) {
      assertValidation({
        TableName: 'abc',
        Key: {},
        ExpressionAttributeNames: {'a': 'a'},
        ExpressionAttributeValues: {},
        ConditionExpression: '',
      }, 'ExpressionAttributeNames contains invalid key: Syntax error; key: "a"', done)
    })

    it('should return ValidationException for empty ExpressionAttributeValues', function(done) {
      assertValidation({
        TableName: 'abc',
        Key: {},
        ExpressionAttributeValues: {},
        ConditionExpression: '',
      }, 'ExpressionAttributeValues must not be empty', done)
    })

    it('should return ValidationException for invalid keys in ExpressionAttributeValues', function(done) {
      assertValidation({
        TableName: 'abc',
        Key: {},
        ExpressionAttributeValues: {':b': {a: ''}, 'b': {S: 'a'}},
        ConditionExpression: '',
      }, 'ExpressionAttributeValues contains invalid key: Syntax error; key: "b"', done)
    })

    it('should return ValidationException for unsupported datatype in ExpressionAttributeValues', function(done) {
      async.forEach([
        {},
        {a: ''},
        {M: {a: {}}},
        {L: [{}]},
        {L: [{a: {}}]},
      ], function(expr, cb) {
        assertValidation({
          TableName: 'abc',
          Key: {},
          ExpressionAttributeValues: {':b': expr},
          ConditionExpression: '',
        }, 'ExpressionAttributeValues contains invalid value: ' +
          'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes for key :b', cb)
      }, done)
    })

    it('should return ValidationException for invalid values in ExpressionAttributeValues', function(done) {
      async.forEach([
        [{NULL: 'no'}, 'Null attribute value types must have the value of true'],
        [{SS: []}, 'An string set  may not be empty'],
        [{NS: []}, 'An number set  may not be empty'],
        [{BS: []}, 'Binary sets should not be empty'],
        [{SS: ['a', 'a']}, 'Input collection [a, a] contains duplicates.'],
        [{BS: ['Yg==', 'Yg==']}, 'Input collection [Yg==, Yg==]of type BS contains duplicates.'],
      ], function(expr, cb) {
        assertValidation({
          TableName: 'abc',
          Key: {},
          ExpressionAttributeValues: {':b': expr[0]},
          ConditionExpression: '',
        }, 'ExpressionAttributeValues contains invalid value: ' +
          'One or more parameter values were invalid: ' + expr[1] + ' for key :b', cb)
      }, done)
    })

    it('should return ValidationException for empty/invalid numbers in ExpressionAttributeValues', function(done) {
      async.forEach([
        [{S: '', N: ''}, 'The parameter cannot be converted to a numeric value'],
        [{S: 'a', N: ''}, 'The parameter cannot be converted to a numeric value'],
        [{S: 'a', N: 'b'}, 'The parameter cannot be converted to a numeric value: b'],
        [{NS: ['1', '']}, 'The parameter cannot be converted to a numeric value'],
        [{NS: ['1', 'b']}, 'The parameter cannot be converted to a numeric value: b'],
        [{NS: ['1', '1']}, 'Input collection contains duplicates'],
        [{N: '123456789012345678901234567890123456789'}, 'Attempting to store more than 38 significant digits in a Number'],
        [{N: '-1.23456789012345678901234567890123456789'}, 'Attempting to store more than 38 significant digits in a Number'],
        [{N: '1e126'}, 'Number overflow. Attempting to store a number with magnitude larger than supported range'],
        [{N: '-1e126'}, 'Number overflow. Attempting to store a number with magnitude larger than supported range'],
        [{N: '1e-131'}, 'Number underflow. Attempting to store a number with magnitude smaller than supported range'],
        [{N: '-1e-131'}, 'Number underflow. Attempting to store a number with magnitude smaller than supported range'],
      ], function(expr, cb) {
        assertValidation({
          TableName: 'abc',
          Key: {},
          ExpressionAttributeValues: {':b': expr[0]},
          ConditionExpression: '',
        }, 'ExpressionAttributeValues contains invalid value: ' + expr[1] + ' for key :b', cb)
      }, done)
    })

    it('should return ValidationException for multiple datatypes in ExpressionAttributeValues', function(done) {
      assertValidation({
        TableName: 'abc',
        Key: {},
        ExpressionAttributeValues: {':b': {S: 'a', N: '1'}},
        ConditionExpression: '',
      }, 'ExpressionAttributeValues contains invalid value: ' +
        'Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes for key :b', done)
    })

    it('should return ValidationException for empty ConditionExpression', function(done) {
      assertValidation({
        TableName: 'abc',
        Key: {},
        ConditionExpression: '',
      }, 'Invalid ConditionExpression: The expression can not be empty;', done)
    })

    it('should return ValidationException for incorrect ConditionExpression', function(done) {
      assertValidation({
        TableName: 'abc',
        Key: {},
        ConditionExpression: 'whatever',
      }, /^Invalid ConditionExpression: Syntax error; /, done)
    })

    it('should return ValidationException for unsupported datatype in Key', function(done) {
      async.forEach([
        {},
        {a: ''},
        {M: {a: {}}},
        {L: [{}]},
        {L: [{a: {}}]},
      ], function(expr, cb) {
        assertValidation({TableName: 'abc', Key: {a: expr}},
          'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', cb)
      }, done)
    })

    it('should return ValidationException for invalid values in Key', function(done) {
      async.forEach([
        [{NULL: 'no'}, 'Null attribute value types must have the value of true'],
        [{SS: []}, 'An string set  may not be empty'],
        [{NS: []}, 'An number set  may not be empty'],
        [{BS: []}, 'Binary sets should not be empty'],
        [{SS: ['a', 'a']}, 'Input collection [a, a] contains duplicates.'],
        [{BS: ['Yg==', 'Yg==']}, 'Input collection [Yg==, Yg==]of type BS contains duplicates.'],
      ], function(expr, cb) {
        assertValidation({TableName: 'abc', Key: {a: expr[0]}},
          'One or more parameter values were invalid: ' + expr[1], cb)
      }, done)
    })

    it('should return ValidationException for empty/invalid numbers in Key', function(done) {
      async.forEach([
        [{S: '', N: ''}, 'The parameter cannot be converted to a numeric value'],
        [{S: 'a', N: ''}, 'The parameter cannot be converted to a numeric value'],
        [{S: 'a', N: 'b'}, 'The parameter cannot be converted to a numeric value: b'],
        [{NS: ['1', '']}, 'The parameter cannot be converted to a numeric value'],
        [{NS: ['1', 'b']}, 'The parameter cannot be converted to a numeric value: b'],
        [{NS: ['1', '1']}, 'Input collection contains duplicates'],
        [{N: '123456789012345678901234567890123456789'}, 'Attempting to store more than 38 significant digits in a Number'],
        [{N: '-1.23456789012345678901234567890123456789'}, 'Attempting to store more than 38 significant digits in a Number'],
        [{N: '1e126'}, 'Number overflow. Attempting to store a number with magnitude larger than supported range'],
        [{N: '-1e126'}, 'Number overflow. Attempting to store a number with magnitude larger than supported range'],
        [{N: '1e-131'}, 'Number underflow. Attempting to store a number with magnitude smaller than supported range'],
        [{N: '-1e-131'}, 'Number underflow. Attempting to store a number with magnitude smaller than supported range'],
      ], function(expr, cb) {
        assertValidation({TableName: 'abc', Key: {a: expr[0]}}, expr[1], cb)
      }, done)
    })

    it('should return ValidationException for multiple datatypes in Key', function(done) {
      assertValidation({TableName: 'abc', Key: {'a': {S: 'a', N: '1'}}},
        'Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException if ComparisonOperator used alone', function(done) {
      assertValidation({TableName: 'aaa', Key: {}, Expected: {a: {ComparisonOperator: 'LT'}}},
        'One or more parameter values were invalid: Value or AttributeValueList must be used with ComparisonOperator: LT for Attribute: a', done)
    })

    it('should return ValidationException if ComparisonOperator and Exists are used together', function(done) {
      assertValidation({TableName: 'aaa', Key: {}, Expected: {a: {Exists: true, ComparisonOperator: 'LT'}}},
        'One or more parameter values were invalid: Exists and ComparisonOperator cannot be used together for Attribute: a', done)
    })

    it('should return ValidationException if AttributeValueList is used alone', function(done) {
      assertValidation({TableName: 'aaa', Key: {}, Expected: {a: {AttributeValueList: []}}},
        'One or more parameter values were invalid: AttributeValueList can only be used with a ComparisonOperator for Attribute: a', done)
    })

    it('should return ValidationException if AttributeValueList and Exists are used together', function(done) {
      assertValidation({TableName: 'aaa', Key: {}, Expected: {a: {Exists: true, AttributeValueList: []}}},
        'One or more parameter values were invalid: AttributeValueList can only be used with a ComparisonOperator for Attribute: a', done)
    })

    it('should return ValidationException if AttributeValueList and Value are used together', function(done) {
      assertValidation({TableName: 'aaa', Key: {}, Expected: {a: {Value: {S: 'a'}, AttributeValueList: []}}},
        'One or more parameter values were invalid: Value and AttributeValueList cannot be used together for Attribute: a', done)
    })

    it('should return ValidationException if Value provides incorrect number of attributes: BETWEEN', function(done) {
      var expected = {a: {
        Value: {S: 'a'},
        ComparisonOperator: 'BETWEEN',
      }}
      assertValidation({TableName: 'aaa', Key: {}, Expected: expected},
        'One or more parameter values were invalid: Invalid number of argument(s) for the BETWEEN ComparisonOperator', done)
    })

    it('should return ValidationException if Value provides incorrect number of attributes: NULL', function(done) {
      var expected = {a: {
        Value: {S: 'a'},
        ComparisonOperator: 'NULL',
      }}
      assertValidation({TableName: 'aaa', Key: {}, Expected: expected},
        'One or more parameter values were invalid: Invalid number of argument(s) for the NULL ComparisonOperator', done)
    })

    it('should return ValidationException if AttributeValueList has different types', function(done) {
      assertValidation({
        TableName: 'aaa',
        Key: {},
        Expected: {a: {ComparisonOperator: 'IN', AttributeValueList: [{S: 'b'}, {N: '1'}]}},
      }, 'One or more parameter values were invalid: AttributeValues inside AttributeValueList must be of same type', done)
    })

    it('should return ValidationException if BETWEEN arguments are in the incorrect order', function(done) {
      assertValidation({
        TableName: 'aaa',
        Key: {},
        Expected: {a: {ComparisonOperator: 'BETWEEN', AttributeValueList: [{S: 'b'}, {S: 'a'}]}},
      }, 'The BETWEEN condition was provided a range where the lower bound is greater than the upper bound', done)
    })

    it('should return ValidationException if ConditionExpression BETWEEN args have different types', function(done) {
      assertValidation({
        TableName: 'aaa',
        Key: {},
        ConditionExpression: 'a between :b and :a',
        ExpressionAttributeValues: {':a': {S: 'a'}, ':b': {N: '1'}},
      }, 'Invalid ConditionExpression: The BETWEEN operator requires same data type for lower and upper bounds; ' +
        'lower bound operand: AttributeValue: {N:1}, upper bound operand: AttributeValue: {S:a}', done)
    })

    it('should return ValidationException if ConditionExpression BETWEEN args are in the incorrect order', function(done) {
      assertValidation({
        TableName: 'aaa',
        Key: {},
        ConditionExpression: 'a between :b and :a',
        ExpressionAttributeValues: {':a': {S: 'a'}, ':b': {S: 'b'}},
      }, 'Invalid ConditionExpression: The BETWEEN operator requires upper bound to be greater than or equal to lower bound; ' +
        'lower bound operand: AttributeValue: {S:b}, upper bound operand: AttributeValue: {S:a}', done)
    })

    it('should return ValidationException if key does not match schema', function(done) {
      async.forEach([
        {},
        {b: {S: 'a'}},
        {a: {S: 'a'}, b: {S: 'a'}},
        {a: {B: 'abcd'}},
        {a: {N: '1'}},
        {a: {BOOL: true}},
        {a: {NULL: true}},
        {a: {SS: ['a']}},
        {a: {NS: ['1']}},
        {a: {BS: ['aaaa']}},
        {a: {M: {}}},
        {a: {L: []}},
      ], function(expr, cb) {
        assertValidation({TableName: helpers.testHashTable, Key: expr},
          'The provided key element does not match the schema', cb)
      }, done)
    })

    it('should return ValidationException if range key does not match schema', function(done) {
      assertValidation({TableName: helpers.testRangeTable, Key: {a: {S: 'a'}}},
        'The provided key element does not match the schema', done)
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
      var req = {TableName: helpers.testHashTable, Key: {a: {S: helpers.randomString()}}, ReturnConsumedCapacity: 'TOTAL'}
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
      async.forEach([
        {Expected: {a: {Value: {S: helpers.randomString()}}}},
        {ConditionExpression: 'a = :a', ExpressionAttributeValues: {':a': {S: helpers.randomString()}}},
        {ConditionExpression: '#a = :a', ExpressionAttributeNames: {'#a': 'a'}, ExpressionAttributeValues: {':a': {S: helpers.randomString()}}},
      ], function(deleteOpts, cb) {
        deleteOpts.TableName = helpers.testHashTable
        deleteOpts.Key = {a: {S: helpers.randomString()}}
        assertConditional(deleteOpts, cb)
      }, done)
    })

    it('should return ConditionalCheckFailedException if expecting existing key to not exist', function(done) {
      var item = {a: {S: helpers.randomString()}}
      request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err) {
        if (err) return done(err)
        async.forEach([
          {Expected: {a: {Exists: false}}},
          {ConditionExpression: 'attribute_not_exists(a)'},
        ], function(deleteOpts, cb) {
          deleteOpts.TableName = helpers.testHashTable
          deleteOpts.Key = {a: item.a}
          assertConditional(deleteOpts, cb)
        }, done)
      })
    })

    it('should succeed if conditional key is different and exists is false', function(done) {
      var item = {a: {S: helpers.randomString()}}
      request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err) {
        if (err) return done(err)
        async.forEach([
          {Expected: {a: {Exists: false}}},
          {ConditionExpression: 'attribute_not_exists(a)'},
        ], function(deleteOpts, cb) {
          deleteOpts.TableName = helpers.testHashTable
          deleteOpts.Key = {a: {S: helpers.randomString()}}
          request(opts(deleteOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should succeed if conditional key is same and exists is true', function(done) {
      async.forEach([
        {Expected: {a: {Value: {S: helpers.randomString()}}}},
        {ConditionExpression: 'a = :a', ExpressionAttributeValues: {':a': {S: helpers.randomString()}}},
        {ConditionExpression: '#a = :a', ExpressionAttributeNames: {'#a': 'a'}, ExpressionAttributeValues: {':a': {S: helpers.randomString()}}},
      ], function(deleteOpts, cb) {
        var item = {a: deleteOpts.Expected ? deleteOpts.Expected.a.Value : deleteOpts.ExpressionAttributeValues[':a']}
        request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err) {
          if (err) return cb(err)
          deleteOpts.TableName = helpers.testHashTable
          deleteOpts.Key = item
          request(opts(deleteOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        })
      }, done)
    })

    it('should succeed if expecting non-existant value to not exist', function(done) {
      async.forEach([
        {Expected: {b: {Exists: false}}, Key: {a: {S: helpers.randomString()}}},
        {ConditionExpression: 'attribute_not_exists(b)', Key: {a: {S: helpers.randomString()}}},
        {ConditionExpression: 'attribute_not_exists(#b)', ExpressionAttributeNames: {'#b': 'b'}, Key: {a: {S: helpers.randomString()}}},
      ], function(deleteOpts, cb) {
        var item = deleteOpts.Key
        request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err) {
          if (err) return cb(err)
          deleteOpts.TableName = helpers.testHashTable
          request(opts(deleteOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        })
      }, done)
    })

    it('should return ConditionalCheckFailedException if expecting existing value to not exist', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: helpers.randomString()}}
      request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err) {
        if (err) return done(err)
        async.forEach([
          {Expected: {b: {Exists: false}}},
          {ConditionExpression: 'attribute_not_exists(b)'},
          {ConditionExpression: 'attribute_not_exists(#b)', ExpressionAttributeNames: {'#b': 'b'}},
        ], function(deleteOpts, cb) {
          deleteOpts.TableName = helpers.testHashTable
          deleteOpts.Key = {a: item.a}
          assertConditional(deleteOpts, cb)
        }, done)
      })
    })

    it('should succeed for multiple conditional checks if all are valid', function(done) {
      async.forEach([
        {Expected: {a: {Value: {S: helpers.randomString()}}, b: {Exists: false}, c: {Value: {S: helpers.randomString()}}}},
        {ConditionExpression: 'a = :a AND attribute_not_exists(b) AND c = :c', ExpressionAttributeValues: {':a': {S: helpers.randomString()}, ':c': {S: helpers.randomString()}}},
        {ConditionExpression: '#a = :a AND attribute_not_exists(#b) AND #c = :c', ExpressionAttributeNames: {'#a': 'a', '#b': 'b', '#c': 'c'}, ExpressionAttributeValues: {':a': {S: helpers.randomString()}, ':c': {S: helpers.randomString()}}},
      ], function(deleteOpts, cb) {
        var item = deleteOpts.Expected ? {a: deleteOpts.Expected.a.Value, c: deleteOpts.Expected.c.Value} :
          {a: deleteOpts.ExpressionAttributeValues[':a'], c: deleteOpts.ExpressionAttributeValues[':c']}
        request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err) {
          if (err) return cb(err)
          deleteOpts.TableName = helpers.testHashTable
          deleteOpts.Key = {a: item.a}
          request(opts(deleteOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        })
      }, done)
    })

    it('should return ConditionalCheckFailedException for multiple conditional checks if one is invalid', function(done) {
      var item = {a: {S: helpers.randomString()}, c: {S: helpers.randomString()}}
      request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err) {
        if (err) return done(err)
        async.forEach([
          {Expected: {a: {Value: item.a}, b: {Exists: false}, c: {Value: {S: helpers.randomString()}}}},
          {ConditionExpression: 'a = :a AND attribute_not_exists(b) AND c = :c', ExpressionAttributeValues: {':a': item.a, ':c': {S: helpers.randomString()}}},
          {ConditionExpression: '#a = :a AND attribute_not_exists(#b) AND #c = :c', ExpressionAttributeNames: {'#a': 'a', '#b': 'b', '#c': 'c'}, ExpressionAttributeValues: {':a': item.a, ':c': {S: helpers.randomString()}}},
        ], function(deleteOpts, cb) {
          deleteOpts.TableName = helpers.testHashTable
          deleteOpts.Key = {a: item.a}
          assertConditional(deleteOpts, cb)
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
