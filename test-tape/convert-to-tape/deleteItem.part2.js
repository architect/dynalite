const test = require('tape')
const async = require('async')
const helpers = require('./helpers')

const target = 'DeleteItem'
// const request = helpers.request // Not used
// const opts = helpers.opts.bind(null, target) // Not used
// const assertType = helpers.assertType.bind(null, target) // Not used
const assertValidation = helpers.assertValidation.bind(null, target)
// const assertConditional = helpers.assertConditional.bind(null, target) // Not used

test('deleteItem - validations - should return ValidationException for no TableName', function (t) {
  assertValidation({},
    [
      'Value null at \'tableName\' failed to satisfy constraint: ' +
      'Member must not be null',
      'Value null at \'key\' failed to satisfy constraint: ' +
      'Member must not be null',
    ],
    function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('deleteItem - validations - should return ValidationException for empty TableName', function (t) {
  assertValidation({ TableName: '' },
    [
      'Value \'\' at \'tableName\' failed to satisfy constraint: ' +
      'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
      'Value \'\' at \'tableName\' failed to satisfy constraint: ' +
      'Member must have length greater than or equal to 3',
      'Value null at \'key\' failed to satisfy constraint: ' +
      'Member must not be null',
    ],
    function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('deleteItem - validations - should return ValidationException for short TableName', function (t) {
  assertValidation({ TableName: 'a;' },
    [
      'Value \'a;\' at \'tableName\' failed to satisfy constraint: ' +
      'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
      'Value \'a;\' at \'tableName\' failed to satisfy constraint: ' +
      'Member must have length greater than or equal to 3',
      'Value null at \'key\' failed to satisfy constraint: ' +
      'Member must not be null',
    ],
    function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('deleteItem - validations - should return ValidationException for long TableName', function (t) {
  const name = new Array(256 + 1).join('a')
  assertValidation({ TableName: name },
    [
      'Value \'' + name + '\' at \'tableName\' failed to satisfy constraint: ' +
      'Member must have length less than or equal to 255',
      'Value null at \'key\' failed to satisfy constraint: ' +
      'Member must not be null',
    ],
    function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('deleteItem - validations - should return ValidationException for incorrect attributes', function (t) {
  assertValidation({ TableName: 'abc;', ReturnConsumedCapacity: 'hi',
    ReturnItemCollectionMetrics: 'hi', ReturnValues: 'hi' },
  [
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
  ],
  function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('deleteItem - validations - should return ValidationException if expression and non-expression', function (t) {
  assertValidation({
    TableName: 'abc',
    Key: {},
    Expected: {},
    ExpressionAttributeNames: {},
    ExpressionAttributeValues: {},
    ConditionExpression: '',
  }, 'Can not use both expression and non-expression parameters in the same request: ' +
    'Non-expression parameters: {Expected} Expression parameters: {ConditionExpression}', function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('deleteItem - validations - should return ValidationException if ExpressionAttributeNames but no ConditionExpression', function (t) {
  assertValidation({
    TableName: 'abc',
    Key: {},
    Expected: {},
    ExpressionAttributeNames: {},
    ExpressionAttributeValues: {},
  }, 'ExpressionAttributeNames can only be specified when using expressions', function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('deleteItem - validations - should return ValidationException if ExpressionAttributeValues but no ConditionExpression', function (t) {
  assertValidation({
    TableName: 'abc',
    Key: {},
    Expected: {},
    ExpressionAttributeValues: {},
  }, 'ExpressionAttributeValues can only be specified when using expressions: ConditionExpression is null', function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('deleteItem - validations - should return ValidationException for empty ExpressionAttributeNames', function (t) {
  assertValidation({
    TableName: 'abc',
    Key: {},
    ExpressionAttributeNames: {},
    ExpressionAttributeValues: {},
    ConditionExpression: '',
  }, 'ExpressionAttributeNames must not be empty', function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('deleteItem - validations - should return ValidationException for invalid ExpressionAttributeNames', function (t) {
  assertValidation({
    TableName: 'abc',
    Key: {},
    ExpressionAttributeNames: { 'a': 'a' },
    ExpressionAttributeValues: {},
    ConditionExpression: '',
  }, 'ExpressionAttributeNames contains invalid key: Syntax error; key: "a"', function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('deleteItem - validations - should return ValidationException for empty ExpressionAttributeValues', function (t) {
  assertValidation({
    TableName: 'abc',
    Key: {},
    ExpressionAttributeValues: {},
    ConditionExpression: '',
  }, 'ExpressionAttributeValues must not be empty', function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('deleteItem - validations - should return ValidationException for invalid keys in ExpressionAttributeValues', function (t) {
  assertValidation({
    TableName: 'abc',
    Key: {},
    ExpressionAttributeValues: { ':b': { a: '' }, 'b': { S: 'a' } },
    ConditionExpression: '',
  }, 'ExpressionAttributeValues contains invalid key: Syntax error; key: "b"', function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('deleteItem - validations - should return ValidationException for unsupported datatype in ExpressionAttributeValues', function (t) {
  async.forEach([
    {},
    { a: '' },
    { M: { a: {} } },
    { L: [ {} ] },
    { L: [ { a: {} } ] },
  ], function (expr, cb) {
    assertValidation({
      TableName: 'abc',
      Key: {},
      ExpressionAttributeValues: { ':b': expr },
      ConditionExpression: '',
    }, 'ExpressionAttributeValues contains invalid value: ' +
      'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes for key :b', cb)
  }, function (err) {
    t.error(err, 'async.forEach should complete without error')
    t.end()
  })
})

test('deleteItem - validations - should return ValidationException for invalid values in ExpressionAttributeValues', function (t) {
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
      Key: {},
      ExpressionAttributeValues: { ':b': expr[0] },
      ConditionExpression: '',
    }, 'ExpressionAttributeValues contains invalid value: ' +
      'One or more parameter values were invalid: ' + expr[1] + ' for key :b', cb)
  }, function (err) {
    t.error(err, 'async.forEach should complete without error')
    t.end()
  })
})

test('deleteItem - validations - should return ValidationException for empty/invalid numbers in ExpressionAttributeValues', function (t) {
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
    assertValidation({
      TableName: 'abc',
      Key: {},
      ExpressionAttributeValues: { ':b': expr[0] },
      ConditionExpression: '',
    }, 'ExpressionAttributeValues contains invalid value: ' + expr[1] + ' for key :b', cb)
  }, function (err) {
    t.error(err, 'async.forEach should complete without error')
    t.end()
  })
})

test('deleteItem - validations - should return ValidationException for multiple datatypes in ExpressionAttributeValues', function (t) {
  assertValidation({
    TableName: 'abc',
    Key: {},
    ExpressionAttributeValues: { ':b': { S: 'a', N: '1' } },
    ConditionExpression: '',
  }, 'ExpressionAttributeValues contains invalid value: ' +
    'Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes for key :b', function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('deleteItem - validations - should return ValidationException for empty ConditionExpression', function (t) {
  assertValidation({
    TableName: 'abc',
    Key: {},
    ConditionExpression: '',
  }, 'Invalid ConditionExpression: The expression can not be empty;', function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('deleteItem - validations - should return ValidationException for incorrect ConditionExpression', function (t) {
  assertValidation({
    TableName: 'abc',
    Key: {},
    ConditionExpression: 'whatever',
  }, /^Invalid ConditionExpression: Syntax error; /, function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('deleteItem - validations - should return ValidationException for unsupported datatype in Key', function (t) {
  async.forEach([
    {},
    { a: '' },
    { M: { a: {} } },
    { L: [ {} ] },
    { L: [ { a: {} } ] },
  ], function (expr, cb) {
    assertValidation({ TableName: 'abc', Key: { a: expr } },
      'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', cb)
  }, function (err) {
    t.error(err, 'async.forEach should complete without error')
    t.end()
  })
})

test('deleteItem - validations - should return ValidationException for invalid values in Key', function (t) {
  async.forEach([
    [ { NULL: 'no' }, 'Null attribute value types must have the value of true' ],
    [ { SS: [] }, 'An string set  may not be empty' ],
    [ { NS: [] }, 'An number set  may not be empty' ],
    [ { BS: [] }, 'Binary sets should not be empty' ],
    [ { SS: [ 'a', 'a' ] }, 'Input collection [a, a] contains duplicates.' ],
    [ { BS: [ 'Yg==', 'Yg==' ] }, 'Input collection [Yg==, Yg==]of type BS contains duplicates.' ],
  ], function (expr, cb) {
    assertValidation({ TableName: 'abc', Key: { a: expr[0] } },
      'One or more parameter values were invalid: ' + expr[1], cb)
  }, function (err) {
    t.error(err, 'async.forEach should complete without error')
    t.end()
  })
})

test('deleteItem - validations - should return ValidationException for empty/invalid numbers in Key', function (t) {
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
    assertValidation({ TableName: 'abc', Key: { a: expr[0] } }, expr[1], cb)
  }, function (err) {
    t.error(err, 'async.forEach should complete without error')
    t.end()
  })
})

test('deleteItem - validations - should return ValidationException for multiple datatypes in Key', function (t) {
  assertValidation({ TableName: 'abc', Key: { 'a': { S: 'a', N: '1' } } },
    'Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('deleteItem - validations - should return ValidationException if ComparisonOperator used alone', function (t) {
  assertValidation({ TableName: 'aaa', Key: {}, Expected: { a: { ComparisonOperator: 'LT' } } },
    'One or more parameter values were invalid: Value or AttributeValueList must be used with ComparisonOperator: LT for Attribute: a', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('deleteItem - validations - should return ValidationException if ComparisonOperator and Exists are used together', function (t) {
  assertValidation({ TableName: 'aaa', Key: {}, Expected: { a: { Exists: true, ComparisonOperator: 'LT' } } },
    'One or more parameter values were invalid: Exists and ComparisonOperator cannot be used together for Attribute: a', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('deleteItem - validations - should return ValidationException if AttributeValueList is used alone', function (t) {
  assertValidation({ TableName: 'aaa', Key: {}, Expected: { a: { AttributeValueList: [] } } },
    'One or more parameter values were invalid: AttributeValueList can only be used with a ComparisonOperator for Attribute: a', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('deleteItem - validations - should return ValidationException if AttributeValueList and Exists are used together', function (t) {
  assertValidation({ TableName: 'aaa', Key: {}, Expected: { a: { Exists: true, AttributeValueList: [] } } },
    'One or more parameter values were invalid: AttributeValueList can only be used with a ComparisonOperator for Attribute: a', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('deleteItem - validations - should return ValidationException if AttributeValueList and Value are used together', function (t) {
  assertValidation({ TableName: 'aaa', Key: {}, Expected: { a: { Value: { S: 'a' }, AttributeValueList: [] } } },
    'One or more parameter values were invalid: Value and AttributeValueList cannot be used together for Attribute: a', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('deleteItem - validations - should return ValidationException if Value provides incorrect number of attributes: BETWEEN', function (t) {
  const expected = { a: {
    Value: { S: 'a' },
    ComparisonOperator: 'BETWEEN',
  } }
  assertValidation({ TableName: 'aaa', Key: {}, Expected: expected },
    'One or more parameter values were invalid: Invalid number of argument(s) for the BETWEEN ComparisonOperator', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('deleteItem - validations - should return ValidationException if Value provides incorrect number of attributes: NULL', function (t) {
  const expected = { a: {
    Value: { S: 'a' },
    ComparisonOperator: 'NULL',
  } }
  assertValidation({ TableName: 'aaa', Key: {}, Expected: expected },
    'One or more parameter values were invalid: Invalid number of argument(s) for the NULL ComparisonOperator', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('deleteItem - validations - should return ValidationException if AttributeValueList has different types', function (t) {
  assertValidation({
    TableName: 'aaa',
    Key: {},
    Expected: { a: { ComparisonOperator: 'IN', AttributeValueList: [ { S: 'b' }, { N: '1' } ] } },
  }, 'One or more parameter values were invalid: AttributeValues inside AttributeValueList must be of same type', function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('deleteItem - validations - should return ValidationException if BETWEEN arguments are in the incorrect order', function (t) {
  assertValidation({
    TableName: 'aaa',
    Key: {},
    Expected: { a: { ComparisonOperator: 'BETWEEN', AttributeValueList: [ { S: 'b' }, { S: 'a' } ] } },
  }, 'The BETWEEN condition was provided a range where the lower bound is greater than the upper bound', function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('deleteItem - validations - should return ValidationException if ConditionExpression BETWEEN args have different types', function (t) {
  assertValidation({
    TableName: 'aaa',
    Key: {},
    ConditionExpression: 'a between :b and :a',
    ExpressionAttributeValues: { ':a': { S: 'a' }, ':b': { N: '1' } },
  }, 'Invalid ConditionExpression: The BETWEEN operator requires same data type for lower and upper bounds; ' +
    'lower bound operand: AttributeValue: {N:1}, upper bound operand: AttributeValue: {S:a}', function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('deleteItem - validations - should return ValidationException if ConditionExpression BETWEEN args are in the incorrect order', function (t) {
  assertValidation({
    TableName: 'aaa',
    Key: {},
    ConditionExpression: 'a between :b and :a',
    ExpressionAttributeValues: { ':a': { S: 'a' }, ':b': { S: 'b' } },
  }, 'Invalid ConditionExpression: The BETWEEN operator requires upper bound to be greater than or equal to lower bound; ' +
    'lower bound operand: AttributeValue: {S:b}, upper bound operand: AttributeValue: {S:a}', function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('deleteItem - validations - should return ValidationException if key does not match schema', function (t) {
  async.forEach([
    {},
    { b: { S: 'a' } },
    { a: { S: 'a' }, b: { S: 'a' } },
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
    assertValidation({ TableName: helpers.testHashTable, Key: expr },
      'The provided key element does not match the schema', cb)
  }, function (err) {
    t.error(err, 'async.forEach should complete without error')
    t.end()
  })
})

test('deleteItem - validations - should return ValidationException if range key does not match schema', function (t) {
  assertValidation({ TableName: helpers.testRangeTable, Key: { a: { S: 'a' } } },
    'The provided key element does not match the schema', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})
