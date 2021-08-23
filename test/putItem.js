var async = require('async'),
    helpers = require('./helpers')

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
      assertType('Item', 'Map<AttributeValue>', done)
    })

    it('should return SerializationException when Item.Attr is not an attr struct', function(done) {
      this.timeout(60000)
      assertType('Item.Attr', 'AttrStruct<ValueStruct>', done)
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
        'Value null at \'item\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for empty TableName', function(done) {
      assertValidation({TableName: ''}, [
        'Value \'\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        'Value \'\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 3',
        'Value null at \'item\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for short TableName', function(done) {
      assertValidation({TableName: 'a;'}, [
        'Value \'a;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        'Value \'a;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 3',
        'Value null at \'item\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for long TableName', function(done) {
      var name = new Array(256 + 1).join('a')
      assertValidation({TableName: name}, [
        'Value \'' + name + '\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 255',
        'Value null at \'item\' failed to satisfy constraint: ' +
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
          'Value null at \'item\' failed to satisfy constraint: ' +
          'Member must not be null',
          'Value \'hi\' at \'returnValues\' failed to satisfy constraint: ' +
          'Member must satisfy enum value set: [ALL_NEW, UPDATED_OLD, ALL_OLD, NONE, UPDATED_NEW]',
          'Value \'hi\' at \'returnItemCollectionMetrics\' failed to satisfy constraint: ' +
          'Member must satisfy enum value set: [SIZE, NONE]',
        ], done)
    })

    it('should return ValidationException if expression and non-expression', function(done) {
      assertValidation({
        TableName: 'abc',
        Item: {a: {}},
        Expected: {a: {}},
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
        ConditionExpression: '',
      }, 'Can not use both expression and non-expression parameters in the same request: ' +
        'Non-expression parameters: {Expected} Expression parameters: {ConditionExpression}', done)
    })

    it('should return ValidationException if ExpressionAttributeNames but no ConditionExpression', function(done) {
      assertValidation({
        TableName: 'abc',
        Item: {a: {}},
        Expected: {a: {}},
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
      }, 'ExpressionAttributeNames can only be specified when using expressions', done)
    })

    it('should return ValidationException if ExpressionAttributeValues but no ConditionExpression', function(done) {
      assertValidation({
        TableName: 'abc',
        Item: {a: {}},
        Expected: {a: {}},
        ExpressionAttributeValues: {},
      }, 'ExpressionAttributeValues can only be specified when using expressions: ConditionExpression is null', done)
    })

    it('should return ValidationException for unsupported datatype in Item', function(done) {
      async.forEach([
        {},
        {a: ''},
        {M: {a: {}}},
        {L: [{}]},
        {L: [{a: {}}]},
      ], function(expr, cb) {
        assertValidation({TableName: 'abc', Item: {a: expr}, Expected: {a: {}}},
          'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', cb)
      }, done)
    })

    it('should return ValidationException for invalid values in Item', function(done) {
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
          Item: {a: expr[0]},
          ExpressionAttributeNames: {},
          ExpressionAttributeValues: {},
          ConditionExpression: '',
        }, 'One or more parameter values were invalid: ' + expr[1], cb)
      }, done)
    })

    it('should return ValidationException for empty/invalid numbers in Item', function(done) {
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
        assertValidation({TableName: 'abc', Item: {a: expr[0]}}, expr[1], cb)
      }, done)
    })

    it('should return ValidationException for multiple datatypes in Item', function(done) {
      assertValidation({TableName: 'abc', Item: {'a': {S: 'a', N: '1'}}},
        'Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException if item is too big with small attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1).join('a')
      assertValidation({TableName: 'aaa', Item: {a: {S: keyStr}, b: {S: b}}, Expected: {a: {}}},
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ResourceNotFoundException if item is just small enough with small attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 2).join('a')
      assertNotFound({TableName: 'aaa', Item: {a: {S: keyStr}, b: {S: b}}},
        'Requested resource not found', done)
    })

    it('should return ValidationException if item is too big with larger attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 27).join('a')
      assertValidation({TableName: 'aaa', Item: {a: {S: keyStr}, bbbbbbbbbbbbbbbbbbbbbbbbbbb: {S: b}}},
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ResourceNotFoundException if item is just small enough with larger attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 28).join('a')
      assertNotFound({TableName: 'aaa', Item: {a: {S: keyStr}, bbbbbbbbbbbbbbbbbbbbbbbbbbb: {S: b}}},
        'Requested resource not found', done)
    })

    it('should return ValidationException if item is too big with multi attributes', function(done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 7).join('a')
      assertValidation({TableName: 'aaa', Item: {a: {S: keyStr}, bb: {S: b}, ccc: {S: 'cc'}}},
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ResourceNotFoundException if item is just small enough with multi attributes', function(done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 8).join('a')
      assertNotFound({TableName: 'aaa', Item: {a: {S: keyStr}, bb: {S: b}, ccc: {S: 'cc'}}},
        'Requested resource not found', done)
    })

    it('should return ValidationException if item is too big with big number attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 20).join('a'),
        c = new Array(38 + 1).join('1') + new Array(89).join('0')
      assertValidation({TableName: 'aaa', Item: {a: {S: keyStr}, b: {S: b}, c: {N: c}}},
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ValidationException if item is too big with smallest number attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 2).join('a'),
        c = '1' + new Array(126).join('0')
      assertValidation({TableName: 'aaa', Item: {a: {S: keyStr}, b: {S: b}, c: {N: c}}},
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ValidationException if item is too big with smaller number attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 2).join('a'),
        c = '11' + new Array(125).join('0')
      assertValidation({TableName: 'aaa', Item: {a: {S: keyStr}, b: {S: b}, c: {N: c}}},
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ValidationException if item is too big with medium number attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 4).join('a'),
        c = '11111' + new Array(122).join('0')
      assertValidation({TableName: 'aaa', Item: {a: {S: keyStr}, b: {S: b}, c: {N: c}}},
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ValidationException if item is too big with medium number attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 4).join('a'),
        c = '111111' + new Array(121).join('0')
      assertValidation({TableName: 'aaa', Item: {a: {S: keyStr}, b: {S: b}, c: {N: c}}},
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ValidationException if item is too big with medium number attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 5).join('a'),
        c = '1111111' + new Array(120).join('0')
      assertValidation({TableName: 'aaa', Item: {a: {S: keyStr}, b: {S: b}, c: {N: c}}},
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ValidationException if item is too big with multi number attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 5 - 1 - 5).join('a'),
        c = '1111111' + new Array(120).join('0'), d = '1111111' + new Array(120).join('0')
      assertValidation({TableName: 'aaa', Item: {a: {S: keyStr}, b: {S: b}, c: {N: c}, d: {N: d}}},
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ResourceNotFoundException if item is just small enough with multi number attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 5 - 1 - 6).join('a'),
        c = '1111111' + new Array(120).join('0'), d = '1111111' + new Array(120).join('0')
      assertNotFound({TableName: 'aaa', Item: {a: {S: keyStr}, b: {S: b}, c: {N: c}, d: {N: d}}},
        'Requested resource not found', done)
    })

    it('should return ValidationException if no value and no exists', function(done) {
      assertValidation({TableName: 'abc', Item: {}, Expected: {a: {}}},
        'One or more parameter values were invalid: Value must be provided when Exists is null for Attribute: a', done)
    })

    it('should return ValidationException for Exists true with no value', function(done) {
      assertValidation({TableName: 'abc', Item: {}, Expected: {a: {Exists: true}}},
        'One or more parameter values were invalid: Value must be provided when Exists is true for Attribute: a', done)
    })

    it('should return ValidationException for Exists false with value', function(done) {
      assertValidation({TableName: 'abc', Item: {}, Expected: {a: {Exists: false, Value: {S: 'a'}}}},
        'One or more parameter values were invalid: Value cannot be used when Exists is false for Attribute: a', done)
    })

    it('should return ValidationException for incorrect ReturnValues', function(done) {
      async.forEach(['UPDATED_OLD', 'ALL_NEW', 'UPDATED_NEW'], function(returnValues, cb) {
        assertValidation({TableName: 'abc', Item: {}, ReturnValues: returnValues},
          'ReturnValues can only be ALL_OLD or NONE', cb)
      }, done)
    })

    it('should return ValidationException if ComparisonOperator used alone', function(done) {
      assertValidation({TableName: 'aaa', Item: {}, Expected: {a: {ComparisonOperator: 'LT'}}},
        'One or more parameter values were invalid: Value or AttributeValueList must be used with ComparisonOperator: LT for Attribute: a', done)
    })

    it('should return ValidationException if ComparisonOperator and Exists are used together', function(done) {
      assertValidation({TableName: 'aaa', Item: {}, Expected: {a: {Exists: true, ComparisonOperator: 'LT'}}},
        'One or more parameter values were invalid: Exists and ComparisonOperator cannot be used together for Attribute: a', done)
    })

    it('should return ValidationException if AttributeValueList and Value are used together', function(done) {
      var expected = {a: {
        AttributeValueList: [{S: 'a'}],
        Value: {S: 'a'},
        ComparisonOperator: 'LT',
      }}
      assertValidation({TableName: 'aaa', Item: {}, Expected: expected},
        'One or more parameter values were invalid: Value and AttributeValueList cannot be used together for Attribute: a', done)
    })

    it('should return ValidationException if AttributeValueList used without ComparisonOperator', function(done) {
      assertValidation({TableName: 'aaa', Item: {}, Expected: {a: {AttributeValueList: [{S: 'a'}]}}},
        'One or more parameter values were invalid: AttributeValueList can only be used with a ComparisonOperator for Attribute: a', done)
    })

    it('should return ValidationException if AttributeValueList used with Exists', function(done) {
      assertValidation({TableName: 'aaa', Item: {}, Expected: {a: {Exists: true, AttributeValueList: [{S: 'a'}]}}},
        'One or more parameter values were invalid: AttributeValueList can only be used with a ComparisonOperator for Attribute: a', done)
    })

    it('should return ValidationException if AttributeValueList is incorrect length: EQ', function(done) {
      var expected = {a: {
        AttributeValueList: [],
        ComparisonOperator: 'EQ',
      }}
      assertValidation({TableName: 'aaa', Item: {}, Expected: expected},
        'One or more parameter values were invalid: Invalid number of argument(s) for the EQ ComparisonOperator', done)
    })

    it('should return ValidationException if AttributeValueList is incorrect length: NULL', function(done) {
      var expected = {a: {
        AttributeValueList: [{S: 'a'}],
        ComparisonOperator: 'NULL',
      }}
      assertValidation({TableName: 'aaa', Item: {}, Expected: expected},
        'One or more parameter values were invalid: Invalid number of argument(s) for the NULL ComparisonOperator', done)
    })

    it('should return ValidationException if AttributeValueList is incorrect length: IN', function(done) {
      var expected = {a: {
        AttributeValueList: [],
        ComparisonOperator: 'IN',
      }}
      assertValidation({TableName: 'aaa', Item: {}, Expected: expected},
        'One or more parameter values were invalid: Invalid number of argument(s) for the IN ComparisonOperator', done)
    })

    it('should return ValidationException if AttributeValueList is incorrect length: BETWEEN', function(done) {
      var expected = {a: {
        AttributeValueList: [{N: '1'}, {N: '10'}, {N: '12'}],
        ComparisonOperator: 'BETWEEN',
      }}
      assertValidation({TableName: 'aaa', Item: {}, Expected: expected},
        'One or more parameter values were invalid: Invalid number of argument(s) for the BETWEEN ComparisonOperator', done)
    })

    it('should return ValidationException if Value provides incorrect number of attributes: NULL', function(done) {
      var expected = {a: {
        Value: {S: 'a'},
        ComparisonOperator: 'NULL',
      }}
      assertValidation({TableName: 'aaa', Item: {}, Expected: expected},
        'One or more parameter values were invalid: Invalid number of argument(s) for the NULL ComparisonOperator', done)
    })

    it('should return ValidationException if Value provides incorrect number of attributes: BETWEEN', function(done) {
      var expected = {a: {
        Value: {S: 'a'},
        ComparisonOperator: 'BETWEEN',
      }}
      assertValidation({TableName: 'aaa', Item: {}, Expected: expected},
        'One or more parameter values were invalid: Invalid number of argument(s) for the BETWEEN ComparisonOperator', done)
    })

    it('should return ValidationException for empty ExpressionAttributeNames', function(done) {
      assertValidation({
        TableName: 'abc',
        Item: {},
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
        ConditionExpression: '',
      }, 'ExpressionAttributeNames must not be empty', done)
    })

    it('should return ValidationException for invalid ExpressionAttributeNames', function(done) {
      assertValidation({
        TableName: 'abc',
        Item: {},
        ExpressionAttributeNames: {'a': 'a'},
        ExpressionAttributeValues: {},
        ConditionExpression: '',
      }, 'ExpressionAttributeNames contains invalid key: Syntax error; key: "a"', done)
    })

    it('should return ValidationException for empty ExpressionAttributeValues', function(done) {
      assertValidation({
        TableName: 'abc',
        Item: {},
        ExpressionAttributeValues: {},
        ConditionExpression: '',
      }, 'ExpressionAttributeValues must not be empty', done)
    })

    it('should return ValidationException for invalid ExpressionAttributeValues', function(done) {
      assertValidation({
        TableName: 'abc',
        Item: {},
        ExpressionAttributeValues: {'a': {S: 'a'}},
        ConditionExpression: '',
      }, 'ExpressionAttributeValues contains invalid key: Syntax error; key: "a"', done)
    })

    it('should return ValidationException for empty ConditionExpression', function(done) {
      assertValidation({
        TableName: 'abc',
        Item: {},
        ConditionExpression: '',
      }, 'Invalid ConditionExpression: The expression can not be empty;', done)
    })

    it('should return ResourceNotFoundException if key is empty and table does not exist', function(done) {
      assertNotFound({TableName: helpers.randomString(), Item: {}},
        'Requested resource not found', done)
    })

    it('should return ValidationException if missing key', function(done) {
      async.forEach([
        {},
        {b: {S: 'a'}},
      ], function(expr, cb) {
        assertValidation({TableName: helpers.testHashTable, Item: expr},
          'One or more parameter values were invalid: Missing the key a in the item', cb)
      }, done)
    })

    it('should return ValidationException if type mismatch for key', function(done) {
      async.forEach([
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
        assertValidation({TableName: helpers.testHashTable, Item: expr},
          'One or more parameter values were invalid: Type mismatch for key a expected: S actual: ' + Object.keys(expr.a)[0], cb)
      }, done)
    })

    it('should return ValidationException if empty string key', function(done) {
      assertValidation({TableName: helpers.testHashTable, Item: {a: {S: ''}}},
        'One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty string value. Key: a', done)
    })

    it('should return ValidationException if empty binary key', function(done) {
      assertValidation({TableName: helpers.testRangeBTable, Item: {a: {S: 'a'}, b: {B: ''}}},
        'One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty binary value. Key: b', done)
    })

    it('should return ValidationException if missing range key', function(done) {
      assertValidation({TableName: helpers.testRangeTable, Item: {a: {S: 'a'}}},
        'One or more parameter values were invalid: Missing the key b in the item', done)
    })

    it('should return ValidationException if secondary index key is incorrect type', function(done) {
      assertValidation({TableName: helpers.testRangeTable, Item: {a: {S: 'a'}, b: {S: 'a'}, c: {N: '1'}}},
        new RegExp('^One or more parameter values were invalid: ' +
          'Type mismatch for Index Key c Expected: S Actual: N IndexName: index\\d$'), done)
    })

    it('should return ValidationException if hash key is too big', function(done) {
      var keyStr = (helpers.randomString() + new Array(2048).join('a')).slice(0, 2049)
      assertValidation({TableName: helpers.testHashTable, Item: {a: {S: keyStr}}},
        'One or more parameter values were invalid: ' +
        'Size of hashkey has exceeded the maximum size limit of2048 bytes', done)
    })

    it('should return ValidationException if range key is too big', function(done) {
      var keyStr = (helpers.randomString() + new Array(1024).join('a')).slice(0, 1025)
      assertValidation({TableName: helpers.testRangeTable, Item: {a: {S: 'a'}, b: {S: keyStr}}},
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
      request(helpers.opts('CreateTable', table), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        assertNotFound({TableName: table.TableName, Item: {a: {S: 'a'}}},
          'Requested resource not found', done)
        helpers.deleteWhenActive(table.TableName)
      })
    })

    it('should return ValidationException if path token starts with underscore', function(done) {
      async.forEach([
        { ConditionExpression: 'attribute_not_exists(_c)' },
        { ConditionExpression: 'attribute_not_exists(c._d)'},
      ], function(putData, cb) {
        putData.TableName = helpers.testRangeTable
        putData.Item = {a: {S: 'a'}, b: {S: 'b'}}
        assertValidation(putData, /^Invalid ConditionExpression: Syntax error;/, cb)
      }, done)
    })

  })

  // A number can have up to 38 digits precision and can be between 10^-128 to 10^+126

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

    it('should put empty values', function(done) {
      var item = {
        a: {S: helpers.randomString()},
        b: {S: ''},
        c: {B: ''},
        d: {SS: ['']},
        e: {BS: ['']},
      }
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.body.should.eql({})
        res.statusCode.should.equal(200)
        request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item.a}, ConsistentRead: true}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          item.b = {S: ''}
          item.c = {B: ''}
          item.d = {SS: ['']}
          item.e = {BS: ['']}
          res.body.should.eql({Item: item})
          done()
        })
      })
    })

    it('should put really long numbers', function(done) {
      var item = {
        a: {S: helpers.randomString()},
        b: {N: '0000012345678901234567890123456789012345678'},
        c: {N: '-00001.23456789012345678901234567890123456780000'},
        d: {N: '0009.99999999999999999999999999999999999990000e125'},
        e: {N: '-0009.99999999999999999999999999999999999990000e125'},
        f: {N: '0001.000e-130'},
        g: {N: '-0001.000e-130'},
      }
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({})
        request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item.a}, ConsistentRead: true}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          item.b = {N: '12345678901234567890123456789012345678'}
          item.c = {N: '-1.2345678901234567890123456789012345678'}
          item.d = {N: Array(39).join('9') + Array(89).join('0')}
          item.e = {N: '-' + Array(39).join('9') + Array(89).join('0')}
          item.f = {N: '0.' + Array(130).join('0') + '1'}
          item.g = {N: '-0.' + Array(130).join('0') + '1'}
          res.body.should.eql({Item: item})
          done()
        })
      })
    })

    it('should put multi attribute item', function(done) {
      var item = {
        a: {S: helpers.randomString()},
        b: {N: '-56.789'},
        c: {B: 'Yg=='},
        d: {BOOL: false},
        e: {NULL: true},
        f: {SS: ['a']},
        g: {NS: ['-56.789']},
        h: {BS: ['Yg==']},
        i: {L: [
          {S: 'a'},
          {N: '-56.789'},
          {B: 'Yg=='},
          {BOOL: true},
          {NULL: true},
          {SS: ['a']},
          {NS: ['-56.789']},
          {BS: ['Yg==']},
          {L: []},
          {M: {}},
        ]},
        j: {M: {
          a: {S: 'a'},
          b: {N: '-56.789'},
          c: {B: 'Yg=='},
          d: {BOOL: true},
          e: {NULL: true},
          f: {SS: ['a']},
          g: {NS: ['-56.789']},
          h: {BS: ['Yg==']},
          i: {L: []},
          j: {M: {a: {M: {}}, b: {L: []}}},
        }},
      }
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
      var item = {a: {S: helpers.randomString()}, b: {N: '-0015.789e6'}, c: {S: 'a'}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        item.b = {S: 'b'}
        request(opts({TableName: helpers.testHashTable, Item: item, ReturnValues: 'ALL_OLD'}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          item.b = {N: '-15789000'}
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
          res.statusCode.should.equal(200)
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
      async.forEach([
        {Expected: {a: {Value: {S: helpers.randomString()}}}},
        {Expected: {a: {ComparisonOperator: 'NOT_NULL'}}},
        {ConditionExpression: 'a = :a', ExpressionAttributeValues: {':a': {S: helpers.randomString()}}},
        {ConditionExpression: '#a = :a', ExpressionAttributeNames: {'#a': 'a'}, ExpressionAttributeValues: {':a': {S: helpers.randomString()}}},
        {ConditionExpression: 'attribute_exists(a)'},
        {ConditionExpression: 'attribute_exists(#a)', ExpressionAttributeNames: {'#a': 'a'}},
      ], function(putOpts, cb) {
        putOpts.TableName = helpers.testHashTable
        putOpts.Item = {a: {S: helpers.randomString()}}
        assertConditional(putOpts, cb)
      }, done)
    })

    it('should return ConditionalCheckFailedException if expecting existing key to not exist', function(done) {
      var item = {a: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          {Expected: {a: {Exists: false}}},
          {Expected: {a: {ComparisonOperator: 'NULL'}}},
          {ConditionExpression: 'attribute_not_exists(a)'},
          {ConditionExpression: 'attribute_not_exists(#a)', ExpressionAttributeNames: {'#a': 'a'}},
        ], function(putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          assertConditional(putOpts, cb)
        }, done)
      })
    })

    it('should succeed if conditional key is different and exists is false', function(done) {
      var item = {a: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          {Expected: {a: {Exists: false}}},
          {Expected: {a: {ComparisonOperator: 'NULL'}}},
          {ConditionExpression: 'attribute_not_exists(a)'},
          {ConditionExpression: 'attribute_not_exists(#a)', ExpressionAttributeNames: {'#a': 'a'}},
        ], function(putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = {a: {S: helpers.randomString()}}
          request(opts(putOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should succeed if conditional key is same', function(done) {
      var item = {a: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          {Expected: {a: {Value: item.a}}},
          {Expected: {a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]}}},
          {Expected: {a: {Value: item.a, ComparisonOperator: 'EQ'}}},
          {Expected: {b: {Exists: false}}},
          {Expected: {b: {ComparisonOperator: 'NULL'}}},
          {ConditionExpression: 'a = :a', ExpressionAttributeValues: {':a': item.a}},
          {ConditionExpression: '#a = :a', ExpressionAttributeNames: {'#a': 'a'}, ExpressionAttributeValues: {':a': item.a}},
          {ConditionExpression: 'attribute_not_exists(b)'},
          {ConditionExpression: 'attribute_not_exists(#b)', ExpressionAttributeNames: {'#b': 'b'}},
        ], function(putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          request(opts(putOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should return ConditionalCheckFailedException if expecting existing value to not exist if different value specified', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          {Expected: {b: {Exists: false}}},
          {Expected: {b: {ComparisonOperator: 'NULL'}}},
          {ConditionExpression: 'attribute_not_exists(b)'},
          {ConditionExpression: 'attribute_not_exists(#b)', ExpressionAttributeNames: {'#b': 'b'}},
        ], function(putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = {a: item.a, b: {S: helpers.randomString()}}
          assertConditional(putOpts, cb)
        }, done)
      })
    })

    it('should return ConditionalCheckFailedException if expecting existing value to not exist if value not specified', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          {Expected: {b: {Exists: false}}},
          {Expected: {b: {ComparisonOperator: 'NULL'}}},
          {ConditionExpression: 'attribute_not_exists(b)'},
          {ConditionExpression: 'attribute_not_exists(#b)', ExpressionAttributeNames: {'#b': 'b'}},
        ], function(putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = {a: item.a}
          assertConditional(putOpts, cb)
        }, done)
      })
    })

    it('should return ConditionalCheckFailedException if expecting existing value to not exist if same value specified', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          {Expected: {b: {Exists: false}}},
          {Expected: {b: {ComparisonOperator: 'NULL'}}},
          {ConditionExpression: 'attribute_not_exists(b)'},
          {ConditionExpression: 'attribute_not_exists(#b)', ExpressionAttributeNames: {'#b': 'b'}},
        ], function(putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          assertConditional(putOpts, cb)
        }, done)
      })
    })

    it('should succeed for multiple conditional checks if all are valid', function(done) {
      var item = {a: {S: helpers.randomString()}, c: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          {Expected: {a: {Value: item.a}, b: {Exists: false}, c: {ComparisonOperator: 'GE', Value: item.c}}},
          {Expected: {
            a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
            b: {ComparisonOperator: 'NULL'},
            c: {ComparisonOperator: 'GE', AttributeValueList: [item.c]},
          }},
          {
            ConditionExpression: 'a = :a AND attribute_not_exists(#b) AND c >= :c',
            ExpressionAttributeValues: {':a': item.a, ':c': item.c},
            ExpressionAttributeNames: {'#b': 'b'},
          },
        ], function(putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          request(opts(putOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should return ConditionalCheckFailedException for multiple conditional checks if one is invalid', function(done) {
      var item = {a: {S: helpers.randomString()}, c: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          {Expected: {a: {Value: item.a}, b: {Exists: false}, c: {Value: {S: helpers.randomString()}}}},
          {Expected: {
            a: {AttributeValueList: [item.a], ComparisonOperator: 'EQ'},
            b: {ComparisonOperator: 'NULL'},
            c: {AttributeValueList: [{S: helpers.randomString()}], ComparisonOperator: 'EQ'},
          }},
          {
            ConditionExpression: 'a = :a AND attribute_not_exists(#b) AND c = :c',
            ExpressionAttributeValues: {':a': item.a, ':c': {S: helpers.randomString()}},
            ExpressionAttributeNames: {'#b': 'b'},
          },
        ], function(putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          assertConditional(putOpts, cb)
        }, done)
      })
    })

    it('should succeed for multiple conditional checks if one is invalid and OR is specified', function(done) {
      var item = {a: {S: helpers.randomString()}, c: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          {Expected: {
            a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
            b: {ComparisonOperator: 'NULL'},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [{S: helpers.randomString()}]},
          }, ConditionalOperator: 'OR'},
          {
            ConditionExpression: 'a = :a OR attribute_not_exists(#b) OR c = :c',
            ExpressionAttributeValues: {':a': item.a, ':c': {S: helpers.randomString()}},
            ExpressionAttributeNames: {'#b': 'b'},
          },
        ], function(putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          request(opts(putOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should succeed if condition is valid: NE', function(done) {
      var item = {a: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          {Expected: {a: {ComparisonOperator: 'NE', AttributeValueList: [{S: helpers.randomString()}]}}},
          {
            ConditionExpression: 'a <> :a',
            ExpressionAttributeValues: {':a': {S: helpers.randomString()}},
          },
          {
            ConditionExpression: '#a <> :a',
            ExpressionAttributeValues: {':a': {S: helpers.randomString()}},
            ExpressionAttributeNames: {'#a': 'a'},
          },
        ], function(putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          request(opts(putOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should fail if condition is invalid: NE', function(done) {
      var item = {a: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          {Expected: {a: {ComparisonOperator: 'NE', AttributeValueList: [item.a]}}},
          {
            ConditionExpression: 'a <> :a',
            ExpressionAttributeValues: {':a': item.a},
          },
          {
            ConditionExpression: '#a <> :a',
            ExpressionAttributeValues: {':a': item.a},
            ExpressionAttributeNames: {'#a': 'a'},
          },
        ], function(putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          assertConditional(putOpts, cb)
        }, done)
      })
    })

    it('should succeed if condition is valid: LE', function(done) {
      var item = {a: {S: 'b'}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          {Expected: {a: {ComparisonOperator: 'LE', AttributeValueList: [{S: 'c'}]}}},
          {
            ConditionExpression: 'a <= :a',
            ExpressionAttributeValues: {':a': {S: 'c'}},
          },
          {
            ConditionExpression: '#a <= :a',
            ExpressionAttributeValues: {':a': {S: 'c'}},
            ExpressionAttributeNames: {'#a': 'a'},
          },
        ], function(putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          request(opts(putOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should fail if condition is invalid: LE', function(done) {
      var item = {a: {S: 'd'}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          {Expected: {a: {ComparisonOperator: 'LE', AttributeValueList: [{S: 'c'}]}}},
          {
            ConditionExpression: 'a <= :a',
            ExpressionAttributeValues: {':a': {S: 'c'}},
          },
          {
            ConditionExpression: '#a <= :a',
            ExpressionAttributeValues: {':a': {S: 'c'}},
            ExpressionAttributeNames: {'#a': 'a'},
          },
        ], function(putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          assertConditional(putOpts, cb)
        }, done)
      })
    })

    it('should succeed if condition is valid: LT', function(done) {
      var item = {a: {S: 'b'}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          {Expected: {a: {ComparisonOperator: 'LT', AttributeValueList: [{S: 'c'}]}}},
          {
            ConditionExpression: 'a < :a',
            ExpressionAttributeValues: {':a': {S: 'c'}},
          },
          {
            ConditionExpression: '#a < :a',
            ExpressionAttributeValues: {':a': {S: 'c'}},
            ExpressionAttributeNames: {'#a': 'a'},
          },
        ], function(putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          request(opts(putOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should fail if condition is invalid: LT', function(done) {
      var item = {a: {S: 'd'}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          {Expected: {a: {ComparisonOperator: 'LT', AttributeValueList: [{S: 'c'}]}}},
          {
            ConditionExpression: 'a < :a',
            ExpressionAttributeValues: {':a': {S: 'c'}},
          },
          {
            ConditionExpression: '#a < :a',
            ExpressionAttributeValues: {':a': {S: 'c'}},
            ExpressionAttributeNames: {'#a': 'a'},
          },
        ], function(putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          assertConditional(putOpts, cb)
        }, done)
      })
    })

    it('should succeed if condition is valid: GE', function(done) {
      var item = {a: {S: 'b'}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          {Expected: {a: {ComparisonOperator: 'GE', AttributeValueList: [{S: 'a'}]}}},
          {
            ConditionExpression: 'a >= :a',
            ExpressionAttributeValues: {':a': {S: 'a'}},
          },
          {
            ConditionExpression: '#a >= :a',
            ExpressionAttributeValues: {':a': {S: 'a'}},
            ExpressionAttributeNames: {'#a': 'a'},
          },
        ], function(putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          request(opts(putOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should fail if condition is invalid: GE', function(done) {
      var item = {a: {S: 'b'}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          {Expected: {a: {ComparisonOperator: 'GE', AttributeValueList: [{S: 'c'}]}}},
          {
            ConditionExpression: 'a >= :a',
            ExpressionAttributeValues: {':a': {S: 'c'}},
          },
          {
            ConditionExpression: '#a >= :a',
            ExpressionAttributeValues: {':a': {S: 'c'}},
            ExpressionAttributeNames: {'#a': 'a'},
          },
        ], function(putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          assertConditional(putOpts, cb)
        }, done)
      })
    })

    it('should succeed if condition is valid: GT', function(done) {
      var item = {a: {S: 'b'}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          {Expected: {a: {ComparisonOperator: 'GT', AttributeValueList: [{S: 'a'}]}}},
          {
            ConditionExpression: 'a > :a',
            ExpressionAttributeValues: {':a': {S: 'a'}},
          },
          {
            ConditionExpression: '#a > :a',
            ExpressionAttributeValues: {':a': {S: 'a'}},
            ExpressionAttributeNames: {'#a': 'a'},
          },
        ], function(putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          request(opts(putOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should fail if condition is invalid: GT', function(done) {
      var item = {a: {S: 'a'}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          {Expected: {a: {ComparisonOperator: 'GT', AttributeValueList: [{S: 'c'}]}}},
          {
            ConditionExpression: 'a > :a',
            ExpressionAttributeValues: {':a': {S: 'c'}},
          },
          {
            ConditionExpression: '#a > :a',
            ExpressionAttributeValues: {':a': {S: 'c'}},
            ExpressionAttributeNames: {'#a': 'a'},
          },
        ], function(putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          assertConditional(putOpts, cb)
        }, done)
      })
    })

    it('should succeed if condition is valid: CONTAINS', function(done) {
      var item = {a: {S: 'hello'}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          {Expected: {a: {ComparisonOperator: 'CONTAINS', AttributeValueList: [{S: 'ell'}]}}},
          {
            ConditionExpression: 'contains(a, :a)',
            ExpressionAttributeValues: {':a': {S: 'ell'}},
          },
          {
            ConditionExpression: 'contains(#a, :a)',
            ExpressionAttributeValues: {':a': {S: 'ell'}},
            ExpressionAttributeNames: {'#a': 'a'},
          },
        ], function(putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          request(opts(putOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should fail if condition is invalid: CONTAINS', function(done) {
      var item = {a: {S: 'hello'}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          {Expected: {a: {ComparisonOperator: 'CONTAINS', AttributeValueList: [{S: 'goodbye'}]}}},
          {
            ConditionExpression: 'contains(a, :a)',
            ExpressionAttributeValues: {':a': {S: 'goodbye'}},
          },
          {
            ConditionExpression: 'contains(#a, :a)',
            ExpressionAttributeValues: {':a': {S: 'goodbye'}},
            ExpressionAttributeNames: {'#a': 'a'},
          },
        ], function(putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          assertConditional(putOpts, cb)
        }, done)
      })
    })

    it('should succeed if condition is valid: BEGINS_WITH', function(done) {
      var item = {a: {S: 'hello'}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          {Expected: {a: {ComparisonOperator: 'BEGINS_WITH', AttributeValueList: [{S: 'he'}]}}},
          {
            ConditionExpression: 'begins_with(a, :a)',
            ExpressionAttributeValues: {':a': {S: 'he'}},
          },
          {
            ConditionExpression: 'begins_with(#a, :a)',
            ExpressionAttributeValues: {':a': {S: 'he'}},
            ExpressionAttributeNames: {'#a': 'a'},
          },
        ], function(putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          request(opts(putOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should fail if condition is invalid: BEGINS_WITH', function(done) {
      var item = {a: {S: 'hello'}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          {Expected: {a: {ComparisonOperator: 'BEGINS_WITH', AttributeValueList: [{S: 'goodbye'}]}}},
          {
            ConditionExpression: 'begins_with(a, :a)',
            ExpressionAttributeValues: {':a': {S: 'goodbye'}},
          },
          {
            ConditionExpression: 'begins_with(#a, :a)',
            ExpressionAttributeValues: {':a': {S: 'goodbye'}},
            ExpressionAttributeNames: {'#a': 'a'},
          },
        ], function(putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          assertConditional(putOpts, cb)
        }, done)
      })
    })

    it('should succeed if condition is valid: NOT_CONTAINS', function(done) {
      var item = {a: {S: 'hello'}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          {Expected: {a: {ComparisonOperator: 'NOT_CONTAINS', AttributeValueList: [{S: 'goodbye'}]}}},
          {
            ConditionExpression: 'not contains(a, :a)',
            ExpressionAttributeValues: {':a': {S: 'goodbye'}},
          },
          {
            ConditionExpression: 'not contains(#a, :a)',
            ExpressionAttributeValues: {':a': {S: 'goodbye'}},
            ExpressionAttributeNames: {'#a': 'a'},
          },
        ], function(putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          request(opts(putOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should fail if condition is invalid: NOT_CONTAINS', function(done) {
      var item = {a: {S: 'hello'}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          {Expected: {a: {ComparisonOperator: 'NOT_CONTAINS', AttributeValueList: [{S: 'ell'}]}}},
          {
            ConditionExpression: 'not contains(a, :a)',
            ExpressionAttributeValues: {':a': {S: 'ell'}},
          },
          {
            ConditionExpression: 'not contains(#a, :a)',
            ExpressionAttributeValues: {':a': {S: 'ell'}},
            ExpressionAttributeNames: {'#a': 'a'},
          },
        ], function(putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          assertConditional(putOpts, cb)
        }, done)
      })
    })

    it('should succeed if condition is valid: IN', function(done) {
      var item = {a: {S: 'b'}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          {Expected: {a: {ComparisonOperator: 'IN', AttributeValueList: [{S: 'c'}, {S: 'b'}]}}},
          {
            ConditionExpression: 'a in (:a, :b)',
            ExpressionAttributeValues: {':a': {S: 'c'}, ':b': {S: 'b'}},
          },
          {
            ConditionExpression: '#a in (:a, :b)',
            ExpressionAttributeValues: {':a': {S: 'c'}, ':b': {S: 'b'}},
            ExpressionAttributeNames: {'#a': 'a'},
          },
        ], function(putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          request(opts(putOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should fail if condition is invalid: IN', function(done) {
      var item = {a: {S: 'd'}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          {Expected: {a: {ComparisonOperator: 'IN', AttributeValueList: [{S: 'c'}]}}},
          {
            ConditionExpression: 'a in (:a)',
            ExpressionAttributeValues: {':a': {S: 'c'}},
          },
          {
            ConditionExpression: '#a in (:a)',
            ExpressionAttributeValues: {':a': {S: 'c'}},
            ExpressionAttributeNames: {'#a': 'a'},
          },
        ], function(putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          assertConditional(putOpts, cb)
        }, done)
      })
    })

    it('should succeed if condition is valid: BETWEEN', function(done) {
      var item = {a: {S: 'b'}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          {Expected: {a: {ComparisonOperator: 'BETWEEN', AttributeValueList: [{S: 'a'}, {S: 'c'}]}}},
          {
            ConditionExpression: 'a between :a and :b',
            ExpressionAttributeValues: {':a': {S: 'a'}, ':b': {S: 'c'}},
          },
          {
            ConditionExpression: '#a between :a and :b',
            ExpressionAttributeValues: {':a': {S: 'a'}, ':b': {S: 'c'}},
            ExpressionAttributeNames: {'#a': 'a'},
          },
        ], function(putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          request(opts(putOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should fail if condition is invalid: BETWEEN', function(done) {
      var item = {a: {S: 'b'}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          {Expected: {a: {ComparisonOperator: 'BETWEEN', AttributeValueList: [{S: 'c'}, {S: 'd'}]}}},
          {
            ConditionExpression: 'a between :a and :b',
            ExpressionAttributeValues: {':a': {S: 'c'}, ':b': {S: 'd'}},
          },
          {
            ConditionExpression: '#a between :a and :b',
            ExpressionAttributeValues: {':a': {S: 'c'}, ':b': {S: 'd'}},
            ExpressionAttributeNames: {'#a': 'a'},
          },
        ], function(putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          assertConditional(putOpts, cb)
        }, done)
      })
    })

    it('should return ConsumedCapacity for small item', function(done) {
      var a = helpers.randomString(), b = new Array(1010 - a.length).join('b'),
        item = {a: {S: a}, b: {S: b}, c: {N: '12.3456'}, d: {B: 'AQI='}, e: {BS: ['AQI=', 'Ag==', 'AQ==']}},
        req = {TableName: helpers.testHashTable, Item: item, ReturnConsumedCapacity: 'TOTAL'}
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

    it('should return ConsumedCapacity for larger item', function(done) {
      var a = helpers.randomString(), b = new Array(1012 - a.length).join('b'),
        item = {a: {S: a}, b: {S: b}, c: {N: '12.3456'}, d: {B: 'AQI='}, e: {BS: ['AQI=', 'Ag==']}},
        req = {TableName: helpers.testHashTable, Item: item, ReturnConsumedCapacity: 'TOTAL'}
      request(opts(req), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({ConsumedCapacity: {CapacityUnits: 2, TableName: helpers.testHashTable}})
        req.ReturnConsumedCapacity = 'INDEXES'
        request(opts(req), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ConsumedCapacity: {CapacityUnits: 2, Table: {CapacityUnits: 2}, TableName: helpers.testHashTable}})
          req.Item = {a: item.a}
          request(opts(req), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({ConsumedCapacity: {CapacityUnits: 2, Table: {CapacityUnits: 2}, TableName: helpers.testHashTable}})
            request(opts(req), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)
              res.body.should.eql({ConsumedCapacity: {CapacityUnits: 1, Table: {CapacityUnits: 1}, TableName: helpers.testHashTable}})
              done()
            })
          })
        })
      })
    })

  })

})
