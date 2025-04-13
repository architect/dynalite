const test = require('tape')
const async = require('async')
const helpers = require('./helpers')

const target = 'GetItem'
const request = helpers.request // Used in the last test
// const randomName = helpers.randomName // Used in the last test
// const opts = helpers.opts.bind(null, target) // Removed unused variable
// const assertType = helpers.assertType.bind(null, target) // Not used
const assertValidation = helpers.assertValidation.bind(null, target)
const assertNotFound = helpers.assertNotFound.bind(null, target)

test('getItem - validations - should return ValidationException for no TableName', function (t) {
  assertValidation({},
    [
      'Value null at \'key\' failed to satisfy constraint: ' +
      'Member must not be null',
      'Value null at \'tableName\' failed to satisfy constraint: ' +
      'Member must not be null',
    ],
    function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('getItem - validations - should return ValidationException for empty TableName', function (t) {
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

test('getItem - validations - should return ValidationException for short TableName', function (t) {
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

test('getItem - validations - should return ValidationException for long TableName', function (t) {
  const name = new Array(256 + 1).join('a')
  assertValidation({ TableName: name },
    [
      'Value null at \'key\' failed to satisfy constraint: ' +
      'Member must not be null',
      'Value \'' + name + '\' at \'tableName\' failed to satisfy constraint: ' +
      'Member must have length less than or equal to 255',
    ],
    function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('getItem - validations - should return ValidationException for incorrect attributes', function (t) {
  assertValidation({ TableName: 'abc;', ReturnConsumedCapacity: 'hi', AttributesToGet: [] },
    [
      'Value \'[]\' at \'attributesToGet\' failed to satisfy constraint: ' +
      'Member must have length greater than or equal to 1',
      'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
      'Member must satisfy enum value set: [INDEXES, TOTAL, NONE]',
      'Value null at \'key\' failed to satisfy constraint: ' +
      'Member must not be null',
      'Value \'abc;\' at \'tableName\' failed to satisfy constraint: ' +
      'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
    ],
    function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('getItem - validations - should return ValidationException if expression and non-expression', function (t) {
  assertValidation({
    TableName: 'abc',
    Key: { a: {} },
    AttributesToGet: [ 'a' ],
    ExpressionAttributeNames: {},
    ProjectionExpression: '',
  }, 'Can not use both expression and non-expression parameters in the same request: ' +
    'Non-expression parameters: {AttributesToGet} Expression parameters: {ProjectionExpression}', function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('getItem - validations - should return ValidationException if ExpressionAttributeNames but no ProjectionExpression', function (t) {
  assertValidation({
    TableName: 'abc',
    Key: { a: {} },
    AttributesToGet: [ 'a' ],
    ExpressionAttributeNames: {},
  }, 'ExpressionAttributeNames can only be specified when using expressions', function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('getItem - validations - should return ValidationException for unsupported datatype in Key', function (t) {
  async.forEach([
    {},
    { a: '' },
    { M: { a: {} } },
    { L: [ {} ] },
    { L: [ { a: {} } ] },
  ], function (expr, cb) {
    assertValidation({ TableName: 'abc', Key: { a: expr }, ProjectionExpression: '', ExpressionAttributeNames: {} },
      'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', cb)
  }, function (err) {
    t.error(err, 'async.forEach should complete without error')
    t.end()
  })
})

test('getItem - validations - should return ValidationException for invalid values in Key', function (t) {
  async.forEach([
    [ { NULL: 'no' }, 'Null attribute value types must have the value of true' ],
    [ { SS: [] }, 'An string set  may not be empty' ],
    [ { NS: [] }, 'An number set  may not be empty' ],
    [ { BS: [] }, 'Binary sets should not be empty' ],
    [ { SS: [ 'a', 'a' ] }, 'Input collection [a, a] contains duplicates.' ],
    [ { BS: [ 'Yg==', 'Yg==' ] }, 'Input collection [Yg==, Yg==]of type BS contains duplicates.' ],
  ], function (expr, cb) {
    assertValidation({ TableName: 'abc', Key: { a: expr[0] }, AttributesToGet: [ 'a', 'a' ] },
      'One or more parameter values were invalid: ' + expr[1], cb)
  }, function (err) {
    t.error(err, 'async.forEach should complete without error')
    t.end()
  })
})

test('getItem - validations - should return ValidationException for empty/invalid numbers in Key', function (t) {
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

test('getItem - validations - should return ValidationException for multiple datatypes in Key', function (t) {
  assertValidation({ TableName: 'abc', Key: { 'a': { S: 'a', N: '1' } } },
    'Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('getItem - validations - should return ValidationException duplicate values in AttributesToGet', function (t) {
  assertValidation({ TableName: 'abc', Key: {}, AttributesToGet: [ 'a', 'a' ] },
    'One or more parameter values were invalid: Duplicate value in attribute name: a', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('getItem - validations - should return ValidationException for empty ExpressionAttributeNames', function (t) {
  assertValidation({
    TableName: 'abc',
    Key: {},
    ExpressionAttributeNames: {},
    ProjectionExpression: '',
  }, 'ExpressionAttributeNames must not be empty', function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('getItem - validations - should return ValidationException for invalid ExpressionAttributeNames', function (t) {
  assertValidation({
    TableName: 'abc',
    Key: {},
    ExpressionAttributeNames: { 'a': 'a' },
    ProjectionExpression: '',
  }, 'ExpressionAttributeNames contains invalid key: Syntax error; key: "a"', function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('getItem - validations - should return ValidationException for empty ProjectionExpression', function (t) {
  assertValidation({
    TableName: 'abc',
    Key: {},
    ProjectionExpression: '',
  }, 'Invalid ProjectionExpression: The expression can not be empty;', function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('getItem - validations - should return ValidationException for syntax error in ProjectionExpression', function (t) {
  async.forEach([
    'whatever(stuff)',
    ':a',
    'abort,',
    'a,,b',
    'a..b',
    'a[b]',
    '(a.b).c',
    '(a)',
    '(a),(b)',
    '(a,b)',
    'a-b',
  ], function (expr, cb) {
    assertValidation({
      TableName: 'abc',
      Key: {},
      ProjectionExpression: expr,
    }, /^Invalid ProjectionExpression: Syntax error; /, cb)
  }, function (err) {
    t.error(err, 'async.forEach should complete without error')
    t.end()
  })
})

test('getItem - validations - should return ValidationException for reserved keywords in ProjectionExpression', function (t) {
  async.forEach([
    'a.abORt',
    '#a,ABSoLUTE',
  ], function (expr, cb) {
    assertValidation({
      TableName: 'abc',
      Key: {},
      ProjectionExpression: expr,
    }, /^Invalid ProjectionExpression: Attribute name is a reserved keyword; reserved keyword: /, cb)
  }, function (err) {
    t.error(err, 'async.forEach should complete without error')
    t.end()
  })
})

test('getItem - validations - should return ValidationException for missing names in ProjectionExpression', function (t) {
  async.forEach([
    'a,b,a,#a',
  ], function (expr, cb) {
    assertValidation({
      TableName: 'abc',
      Key: {},
      ProjectionExpression: expr,
    }, 'Invalid ProjectionExpression: An expression attribute name used in the document path is not defined; attribute name: #a', cb)
  }, function (err) {
    t.error(err, 'async.forEach should complete without error')
    t.end()
  })
})

test('getItem - validations - should return ValidationException for overlapping paths in ProjectionExpression', function (t) {
  async.forEach([
    [ 'b[1], b.a, #a.b, a', '[a, b]', '[a]' ],
    [ 'a, #a[1]', '[a]', '[a, [1]]' ],
    // TODO: This changed at some point, now conflicts with [b] instead of [a]?
    // ['a,b,a', '[a]', '[b]'],
  ], function (expr, cb) {
    assertValidation({
      TableName: 'abc',
      Key: {},
      ProjectionExpression: expr[0],
      ExpressionAttributeNames: { '#a': 'a' },
    }, 'Invalid ProjectionExpression: Two document paths overlap with each other; ' +
      'must remove or rewrite one of these paths; path one: ' + expr[1] + ', path two: ' + expr[2], cb)
  }, function (err) {
    t.error(err, 'async.forEach should complete without error')
    t.end()
  })
})

test('getItem - validations - should return ValidationException for conflicting paths in ProjectionExpression', function (t) {
  async.forEach([
    [ 'a.b, #a[1], #b', '[a, b]', '[a, [1]]' ],
    [ 'a.b[1], #a[1], #b', '[a, b, [1]]', '[a, [1]]' ],
    [ 'a[3].b, #a.#b.b', '[a, [3], b]', '[a, [3], b]' ],
  ], function (expr, cb) {
    assertValidation({
      TableName: 'abc',
      Key: {},
      ProjectionExpression: expr[0],
      ExpressionAttributeNames: { '#a': 'a', '#b': '[3]' },
    }, 'Invalid ProjectionExpression: Two document paths conflict with each other; ' +
      'must remove or rewrite one of these paths; path one: ' + expr[1] + ', path two: ' + expr[2], cb)
  }, function (err) {
    t.error(err, 'async.forEach should complete without error')
    t.end()
  })
})

test('getItem - validations - should return ValidationException for unused names in ProjectionExpression', function (t) {
  async.forEach([
    'a',
    'a,b',
  ], function (expr, cb) {
    assertValidation({
      TableName: 'abc',
      Key: {},
      ProjectionExpression: expr,
      ExpressionAttributeNames: { '#a': 'a', '#b': 'b' },
    }, 'Value provided in ExpressionAttributeNames unused in expressions: keys: {#a, #b}', cb)
  }, function (err) {
    t.error(err, 'async.forEach should complete without error')
    t.end()
  })
})

test('getItem - validations - should return ResourceNotFoundException if key is empty and table does not exist', function (t) {
  assertNotFound({ TableName: helpers.randomName(), Key: {} },
    'Requested resource not found', function (err) {
      t.error(err, 'assertNotFound should not error')
      t.end()
    })
})

test('getItem - validations - should return ValidationException if key does not match schema', function (t) {
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

test('getItem - validations - should return ValidationException if range key does not match schema', function (t) {
  assertValidation({ TableName: helpers.testRangeTable, Key: { a: { S: 'a' } } },
    'The provided key element does not match the schema', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('getItem - validations - should return ValidationException if string key has empty string', function (t) {
  assertValidation({ TableName: helpers.testHashTable, Key: { a: { S: '' } } },
    'One or more parameter values were invalid: ' +
    'The AttributeValue for a key attribute cannot contain an empty string value. Key: a', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('getItem - validations - should return ValidationException if binary key has empty string', function (t) {
  assertValidation({ TableName: helpers.testRangeBTable, Key: { a: { S: 'a' }, b: { B: '' } } },
    'One or more parameter values were invalid: ' +
    'The AttributeValue for a key attribute cannot contain an empty binary value. Key: b', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('getItem - validations - should return ValidationException if hash key is too big', function (t) {
  const keyStr = (helpers.randomString() + new Array(2048).join('a')).slice(0, 2049)
  assertValidation({ TableName: helpers.testHashTable, Key: { a: { S: keyStr } } },
    'One or more parameter values were invalid: ' +
    'Size of hashkey has exceeded the maximum size limit of2048 bytes', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('getItem - validations - should return ValidationException if range key is too big', function (t) {
  const keyStr = (helpers.randomString() + new Array(1024).join('a')).slice(0, 1025)
  assertValidation({ TableName: helpers.testRangeTable, Key: { a: { S: 'a' }, b: { S: keyStr } } },
    'One or more parameter values were invalid: ' +
    'Aggregated size of all range keys has exceeded the size limit of 1024 bytes', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('getItem - validations - should return ValidationException for non-scalar key access in ProjectionExpression', function (t) {
  async.forEach([
    '#a.b.c',
    '#a[0]',
  ], function (expr, cb) {
    assertValidation({
      TableName: helpers.testHashTable,
      Key: { a: { S: helpers.randomString() } },
      ProjectionExpression: expr,
      ExpressionAttributeNames: { '#a': 'a' },
    }, 'Key attributes must be scalars; list random access \'[]\' and map lookup \'.\' are not allowed: Key: a', cb)
  }, function (err) {
    t.error(err, 'async.forEach should complete without error')
    t.end()
  })
})

test('getItem - validations - should return ValidationException for non-scalar index access in ProjectionExpression', function (t) {
  async.forEach([
    '#d.b.c',
    '#d[0]',
  ], function (expr, cb) {
    assertValidation({
      TableName: helpers.testRangeTable,
      Key: { a: { S: helpers.randomString() }, b: { S: helpers.randomString() } },
      ProjectionExpression: expr,
      ExpressionAttributeNames: { '#d': 'd' },
    }, 'Key attributes must be scalars; list random access \'[]\' and map lookup \'.\' are not allowed: IndexKey: d', cb)
  }, function (err) {
    t.error(err, 'async.forEach should complete without error')
    t.end()
  })
})

test('getItem - validations - should return ResourceNotFoundException if table is being created', function (t) {
  const table = {
    TableName: helpers.randomName(),
    AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' } ],
    KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
    ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
  }
  request(helpers.opts('CreateTable', table), function (err) {
    if (err) {
      t.error(err, 'CreateTable should not error')
      return t.end()
    }
    assertNotFound({ TableName: table.TableName, Key: { a: { S: 'a' } } },
      'Requested resource not found', function (errNotFound) {
        t.error(errNotFound, 'assertNotFound should not error')
        helpers.deleteWhenActive(table.TableName, function (errDelete) {
          t.error(errDelete, 'deleteWhenActive should not error during cleanup')
          t.end()
        })
      })
  })
})
