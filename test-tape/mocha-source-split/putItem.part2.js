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

describe('putItem', function () {
  describe('validations', function () {

    it('should return ValidationException for no TableName', function (done) {
      assertValidation({}, [
        'Value null at \'tableName\' failed to satisfy constraint: ' +
        'Member must not be null',
        'Value null at \'item\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for empty TableName', function (done) {
      assertValidation({ TableName: '' }, [
        'Value \'\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        'Value \'\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 3',
        'Value null at \'item\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for short TableName', function (done) {
      assertValidation({ TableName: 'a;' }, [
        'Value \'a;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        'Value \'a;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 3',
        'Value null at \'item\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for long TableName', function (done) {
      var name = new Array(256 + 1).join('a')
      assertValidation({ TableName: name }, [
        'Value \'' + name + '\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 255',
        'Value null at \'item\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for incorrect attributes', function (done) {
      assertValidation({ TableName: 'abc;', ReturnConsumedCapacity: 'hi',
        ReturnItemCollectionMetrics: 'hi', ReturnValues: 'hi' }, [
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

    it('should return ValidationException if expression and non-expression', function (done) {
      assertValidation({
        TableName: 'abc',
        Item: { a: {} },
        Expected: { a: {} },
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
        ConditionExpression: '',
      }, 'Can not use both expression and non-expression parameters in the same request: ' +
        'Non-expression parameters: {Expected} Expression parameters: {ConditionExpression}', done)
    })

    it('should return ValidationException if ExpressionAttributeNames but no ConditionExpression', function (done) {
      assertValidation({
        TableName: 'abc',
        Item: { a: {} },
        Expected: { a: {} },
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
      }, 'ExpressionAttributeNames can only be specified when using expressions', done)
    })

    it('should return ValidationException if ExpressionAttributeValues but no ConditionExpression', function (done) {
      assertValidation({
        TableName: 'abc',
        Item: { a: {} },
        Expected: { a: {} },
        ExpressionAttributeValues: {},
      }, 'ExpressionAttributeValues can only be specified when using expressions: ConditionExpression is null', done)
    })

    it('should return ValidationException for unsupported datatype in Item', function (done) {
      async.forEach([
        {},
        { a: '' },
        { M: { a: {} } },
        { L: [ {} ] },
        { L: [ { a: {} } ] },
      ], function (expr, cb) {
        assertValidation({ TableName: 'abc', Item: { a: expr }, Expected: { a: {} } },
          'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', cb)
      }, done)
    })

    it('should return ValidationException for invalid values in Item', function (done) {
      async.forEach([
        [ { NULL: 'no' }, 'Null attribute value types must have the value of true' ],
        [ { SS: [] }, 'An string set  may not be empty' ],
        [ { NS: [] }, 'An number set  may not be empty' ],
        [ { BS: [] }, 'Binary sets should not be empty' ],
        [ { SS: [ 'a', 'a' ] }, 'Input collection [a, a] contains duplicates.' ],
        [ { BS: [ 'Yg==', 'Yg==' ] }, 'Input collection [Yg==, Yg==]of type BS contains duplicates.' ],
      ], function (expr, cb) {
        assertValidation({
          TableName: 'abc',
          Item: { a: expr[0] },
          ExpressionAttributeNames: {},
          ExpressionAttributeValues: {},
          ConditionExpression: '',
        }, 'One or more parameter values were invalid: ' + expr[1], cb)
      }, done)
    })

    it('should return ValidationException for empty/invalid numbers in Item', function (done) {
      async.forEach([
        [ { S: '', N: '' }, 'The parameter cannot be converted to a numeric value' ],
        [ { S: 'a', N: '' }, 'The parameter cannot be converted to a numeric value' ],
        [ { S: 'a', N: 'b' }, 'The parameter cannot be converted to a numeric value: b' ],
        [ { NS: [ '1', '' ] }, 'The parameter cannot be converted to a numeric value' ],
        [ { NS: [ '1', 'b' ] }, 'The parameter cannot be converted to a numeric value: b' ],
        [ { NS: [ '1', '1' ] }, 'Input collection contains duplicates' ],
        [ { N: '123456789012345678901234567890123456789' }, 'Attempting to store more than 38 significant digits in a Number' ],
        [ { N: '-1.23456789012345678901234567890123456789' }, 'Attempting to store more than 38 significant digits in a Number' ],
        [ { N: '1e126' }, 'Number overflow. Attempting to store a number with magnitude larger than supported range' ],
        [ { N: '-1e126' }, 'Number overflow. Attempting to store a number with magnitude larger than supported range' ],
        [ { N: '1e-131' }, 'Number underflow. Attempting to store a number with magnitude smaller than supported range' ],
        [ { N: '-1e-131' }, 'Number underflow. Attempting to store a number with magnitude smaller than supported range' ],
      ], function (expr, cb) {
        assertValidation({ TableName: 'abc', Item: { a: expr[0] } }, expr[1], cb)
      }, done)
    })

    it('should return ValidationException for multiple datatypes in Item', function (done) {
      assertValidation({ TableName: 'abc', Item: { 'a': { S: 'a', N: '1' } } },
        'Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException if item is too big with small attribute', function (done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1).join('a')
      assertValidation({ TableName: 'aaa', Item: { a: { S: keyStr }, b: { S: b } }, Expected: { a: {} } },
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ResourceNotFoundException if item is just small enough with small attribute', function (done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 2).join('a')
      assertNotFound({ TableName: 'aaa', Item: { a: { S: keyStr }, b: { S: b } } },
        'Requested resource not found', done)
    })

    it('should return ValidationException if item is too big with larger attribute', function (done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 27).join('a')
      assertValidation({ TableName: 'aaa', Item: { a: { S: keyStr }, bbbbbbbbbbbbbbbbbbbbbbbbbbb: { S: b } } },
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ResourceNotFoundException if item is just small enough with larger attribute', function (done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 28).join('a')
      assertNotFound({ TableName: 'aaa', Item: { a: { S: keyStr }, bbbbbbbbbbbbbbbbbbbbbbbbbbb: { S: b } } },
        'Requested resource not found', done)
    })

    it('should return ValidationException if item is too big with multi attributes', function (done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 7).join('a')
      assertValidation({ TableName: 'aaa', Item: { a: { S: keyStr }, bb: { S: b }, ccc: { S: 'cc' } } },
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ResourceNotFoundException if item is just small enough with multi attributes', function (done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 8).join('a')
      assertNotFound({ TableName: 'aaa', Item: { a: { S: keyStr }, bb: { S: b }, ccc: { S: 'cc' } } },
        'Requested resource not found', done)
    })

    it('should return ValidationException if item is too big with big number attribute', function (done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 20).join('a'),
        c = new Array(38 + 1).join('1') + new Array(89).join('0')
      assertValidation({ TableName: 'aaa', Item: { a: { S: keyStr }, b: { S: b }, c: { N: c } } },
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ValidationException if item is too big with smallest number attribute', function (done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 2).join('a'),
        c = '1' + new Array(126).join('0')
      assertValidation({ TableName: 'aaa', Item: { a: { S: keyStr }, b: { S: b }, c: { N: c } } },
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ValidationException if item is too big with smaller number attribute', function (done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 2).join('a'),
        c = '11' + new Array(125).join('0')
      assertValidation({ TableName: 'aaa', Item: { a: { S: keyStr }, b: { S: b }, c: { N: c } } },
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ValidationException if item is too big with medium number attribute', function (done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 4).join('a'),
        c = '11111' + new Array(122).join('0')
      assertValidation({ TableName: 'aaa', Item: { a: { S: keyStr }, b: { S: b }, c: { N: c } } },
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ValidationException if item is too big with medium number attribute', function (done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 4).join('a'),
        c = '111111' + new Array(121).join('0')
      assertValidation({ TableName: 'aaa', Item: { a: { S: keyStr }, b: { S: b }, c: { N: c } } },
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ValidationException if item is too big with medium number attribute', function (done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 5).join('a'),
        c = '1111111' + new Array(120).join('0')
      assertValidation({ TableName: 'aaa', Item: { a: { S: keyStr }, b: { S: b }, c: { N: c } } },
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ValidationException if item is too big with multi number attribute', function (done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 5 - 1 - 5).join('a'),
        c = '1111111' + new Array(120).join('0'), d = '1111111' + new Array(120).join('0')
      assertValidation({ TableName: 'aaa', Item: { a: { S: keyStr }, b: { S: b }, c: { N: c }, d: { N: d } } },
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ResourceNotFoundException if item is just small enough with multi number attribute', function (done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 5 - 1 - 6).join('a'),
        c = '1111111' + new Array(120).join('0'), d = '1111111' + new Array(120).join('0')
      assertNotFound({ TableName: 'aaa', Item: { a: { S: keyStr }, b: { S: b }, c: { N: c }, d: { N: d } } },
        'Requested resource not found', done)
    })

    it('should return ValidationException if no value and no exists', function (done) {
      assertValidation({ TableName: 'abc', Item: {}, Expected: { a: {} } },
        'One or more parameter values were invalid: Value must be provided when Exists is null for Attribute: a', done)
    })

    it('should return ValidationException for Exists true with no value', function (done) {
      assertValidation({ TableName: 'abc', Item: {}, Expected: { a: { Exists: true } } },
        'One or more parameter values were invalid: Value must be provided when Exists is true for Attribute: a', done)
    })

    it('should return ValidationException for Exists false with value', function (done) {
      assertValidation({ TableName: 'abc', Item: {}, Expected: { a: { Exists: false, Value: { S: 'a' } } } },
        'One or more parameter values were invalid: Value cannot be used when Exists is false for Attribute: a', done)
    })

    it('should return ValidationException for incorrect ReturnValues', function (done) {
      async.forEach([ 'UPDATED_OLD', 'ALL_NEW', 'UPDATED_NEW' ], function (returnValues, cb) {
        assertValidation({ TableName: 'abc', Item: {}, ReturnValues: returnValues },
          'ReturnValues can only be ALL_OLD or NONE', cb)
      }, done)
    })

    it('should return ValidationException if ComparisonOperator used alone', function (done) {
      assertValidation({ TableName: 'aaa', Item: {}, Expected: { a: { ComparisonOperator: 'LT' } } },
        'One or more parameter values were invalid: Value or AttributeValueList must be used with ComparisonOperator: LT for Attribute: a', done)
    })

    it('should return ValidationException if ComparisonOperator and Exists are used together', function (done) {
      assertValidation({ TableName: 'aaa', Item: {}, Expected: { a: { Exists: true, ComparisonOperator: 'LT' } } },
        'One or more parameter values were invalid: Exists and ComparisonOperator cannot be used together for Attribute: a', done)
    })

    it('should return ValidationException if AttributeValueList and Value are used together', function (done) {
      var expected = { a: {
        AttributeValueList: [ { S: 'a' } ],
        Value: { S: 'a' },
        ComparisonOperator: 'LT',
      } }
      assertValidation({ TableName: 'aaa', Item: {}, Expected: expected },
        'One or more parameter values were invalid: Value and AttributeValueList cannot be used together for Attribute: a', done)
    })

    it('should return ValidationException if AttributeValueList used without ComparisonOperator', function (done) {
      assertValidation({ TableName: 'aaa', Item: {}, Expected: { a: { AttributeValueList: [ { S: 'a' } ] } } },
        'One or more parameter values were invalid: AttributeValueList can only be used with a ComparisonOperator for Attribute: a', done)
    })

    it('should return ValidationException if AttributeValueList used with Exists', function (done) {
      assertValidation({ TableName: 'aaa', Item: {}, Expected: { a: { Exists: true, AttributeValueList: [ { S: 'a' } ] } } },
        'One or more parameter values were invalid: AttributeValueList can only be used with a ComparisonOperator for Attribute: a', done)
    })

    it('should return ValidationException if AttributeValueList is incorrect length: EQ', function (done) {
      var expected = { a: {
        AttributeValueList: [],
        ComparisonOperator: 'EQ',
      } }
      assertValidation({ TableName: 'aaa', Item: {}, Expected: expected },
        'One or more parameter values were invalid: Invalid number of argument(s) for the EQ ComparisonOperator', done)
    })

    it('should return ValidationException if AttributeValueList is incorrect length: NULL', function (done) {
      var expected = { a: {
        AttributeValueList: [ { S: 'a' } ],
        ComparisonOperator: 'NULL',
      } }
      assertValidation({ TableName: 'aaa', Item: {}, Expected: expected },
        'One or more parameter values were invalid: Invalid number of argument(s) for the NULL ComparisonOperator', done)
    })

    it('should return ValidationException if AttributeValueList is incorrect length: IN', function (done) {
      var expected = { a: {
        AttributeValueList: [],
        ComparisonOperator: 'IN',
      } }
      assertValidation({ TableName: 'aaa', Item: {}, Expected: expected },
        'One or more parameter values were invalid: Invalid number of argument(s) for the IN ComparisonOperator', done)
    })

    it('should return ValidationException if AttributeValueList is incorrect length: BETWEEN', function (done) {
      var expected = { a: {
        AttributeValueList: [ { N: '1' }, { N: '10' }, { N: '12' } ],
        ComparisonOperator: 'BETWEEN',
      } }
      assertValidation({ TableName: 'aaa', Item: {}, Expected: expected },
        'One or more parameter values were invalid: Invalid number of argument(s) for the BETWEEN ComparisonOperator', done)
    })

    it('should return ValidationException if Value provides incorrect number of attributes: NULL', function (done) {
      var expected = { a: {
        Value: { S: 'a' },
        ComparisonOperator: 'NULL',
      } }
      assertValidation({ TableName: 'aaa', Item: {}, Expected: expected },
        'One or more parameter values were invalid: Invalid number of argument(s) for the NULL ComparisonOperator', done)
    })

    it('should return ValidationException if Value provides incorrect number of attributes: BETWEEN', function (done) {
      var expected = { a: {
        Value: { S: 'a' },
        ComparisonOperator: 'BETWEEN',
      } }
      assertValidation({ TableName: 'aaa', Item: {}, Expected: expected },
        'One or more parameter values were invalid: Invalid number of argument(s) for the BETWEEN ComparisonOperator', done)
    })

    it('should return ValidationException for empty ExpressionAttributeNames', function (done) {
      assertValidation({
        TableName: 'abc',
        Item: {},
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
        ConditionExpression: '',
      }, 'ExpressionAttributeNames must not be empty', done)
    })

    it('should return ValidationException for invalid ExpressionAttributeNames', function (done) {
      assertValidation({
        TableName: 'abc',
        Item: {},
        ExpressionAttributeNames: { 'a': 'a' },
        ExpressionAttributeValues: {},
        ConditionExpression: '',
      }, 'ExpressionAttributeNames contains invalid key: Syntax error; key: "a"', done)
    })

    it('should return ValidationException for empty ExpressionAttributeValues', function (done) {
      assertValidation({
        TableName: 'abc',
        Item: {},
        ExpressionAttributeValues: {},
        ConditionExpression: '',
      }, 'ExpressionAttributeValues must not be empty', done)
    })

    it('should return ValidationException for invalid ExpressionAttributeValues', function (done) {
      assertValidation({
        TableName: 'abc',
        Item: {},
        ExpressionAttributeValues: { 'a': { S: 'a' } },
        ConditionExpression: '',
      }, 'ExpressionAttributeValues contains invalid key: Syntax error; key: "a"', done)
    })

    it('should return ValidationException for empty ConditionExpression', function (done) {
      assertValidation({
        TableName: 'abc',
        Item: {},
        ConditionExpression: '',
      }, 'Invalid ConditionExpression: The expression can not be empty;', done)
    })

    it('should return ResourceNotFoundException if key is empty and table does not exist', function (done) {
      assertNotFound({ TableName: helpers.randomString(), Item: {} },
        'Requested resource not found', done)
    })

    it('should return ValidationException if missing key', function (done) {
      async.forEach([
        {},
        { b: { S: 'a' } },
      ], function (expr, cb) {
        assertValidation({ TableName: helpers.testHashTable, Item: expr },
          'One or more parameter values were invalid: Missing the key a in the item', cb)
      }, done)
    })

    it('should return ValidationException if type mismatch for key', function (done) {
      async.forEach([
        { a: { B: 'abcd' } },
        { a: { N: '1' } },
        { a: { BOOL: true } },
        { a: { NULL: true } },
        { a: { SS: [ 'a' ] } },
        { a: { NS: [ '1' ] } },
        { a: { BS: [ 'aaaa' ] } },
        { a: { M: {} } },
        { a: { L: [] } },
      ], function (expr, cb) {
        assertValidation({ TableName: helpers.testHashTable, Item: expr },
          'One or more parameter values were invalid: Type mismatch for key a expected: S actual: ' + Object.keys(expr.a)[0], cb)
      }, done)
    })

    it('should return ValidationException if empty string key', function (done) {
      assertValidation({ TableName: helpers.testHashTable, Item: { a: { S: '' } } },
        'One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty string value. Key: a', done)
    })

    it('should return ValidationException if empty binary key', function (done) {
      assertValidation({ TableName: helpers.testRangeBTable, Item: { a: { S: 'a' }, b: { B: '' } } },
        'One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty binary value. Key: b', done)
    })

    it('should return ValidationException if missing range key', function (done) {
      assertValidation({ TableName: helpers.testRangeTable, Item: { a: { S: 'a' } } },
        'One or more parameter values were invalid: Missing the key b in the item', done)
    })

    it('should return ValidationException if secondary index key is incorrect type', function (done) {
      assertValidation({ TableName: helpers.testRangeTable, Item: { a: { S: 'a' }, b: { S: 'a' }, c: { N: '1' } } },
        new RegExp('^One or more parameter values were invalid: ' +
          'Type mismatch for Index Key c Expected: S Actual: N IndexName: index\\d$'), done)
    })

    it('should return ValidationException if hash key is too big', function (done) {
      var keyStr = (helpers.randomString() + new Array(2048).join('a')).slice(0, 2049)
      assertValidation({ TableName: helpers.testHashTable, Item: { a: { S: keyStr } } },
        'One or more parameter values were invalid: ' +
        'Size of hashkey has exceeded the maximum size limit of2048 bytes', done)
    })

    it('should return ValidationException if range key is too big', function (done) {
      var keyStr = (helpers.randomString() + new Array(1024).join('a')).slice(0, 1025)
      assertValidation({ TableName: helpers.testRangeTable, Item: { a: { S: 'a' }, b: { S: keyStr } } },
        'One or more parameter values were invalid: ' +
        'Aggregated size of all range keys has exceeded the size limit of 1024 bytes', done)
    })

    it('should return ResourceNotFoundException if table is being created', function (done) {
      var table = {
        TableName: randomName(),
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
      }
      request(helpers.opts('CreateTable', table), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        assertNotFound({ TableName: table.TableName, Item: { a: { S: 'a' } } },
          'Requested resource not found', done)
        helpers.deleteWhenActive(table.TableName)
      })
    })
  })
})