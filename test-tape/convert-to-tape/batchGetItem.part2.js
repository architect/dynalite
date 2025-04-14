const test = require('tape')
const async = require('async')
const helpers = require('./helpers')

const target = 'BatchGetItem'
// const request = helpers.request // Not used directly
// const randomName = helpers.randomName // Not used directly
// const opts = helpers.opts.bind(null, target) // Not used directly
// const assertType = helpers.assertType.bind(null, target) // Not used directly
const assertValidation = helpers.assertValidation.bind(null, target)
const assertNotFound = helpers.assertNotFound.bind(null, target)
// const runSlowTests = helpers.runSlowTests // Not used

test('batchGetItem - validations - should return ValidationException for empty RequestItems', function (t) {
  assertValidation({},
    '1 validation error detected: ' +
    'Value null at \'requestItems\' failed to satisfy constraint: ' +
    'Member must not be null', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('batchGetItem - validations - should return ValidationException for missing RequestItems', function (t) {
  assertValidation({ ReturnConsumedCapacity: 'hi', ReturnItemCollectionMetrics: 'hi' }, [
    'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
    'Member must satisfy enum value set: [INDEXES, TOTAL, NONE]',
    'Value null at \'requestItems\' failed to satisfy constraint: ' +
    'Member must not be null',
  ], function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('batchGetItem - validations - should return ValidationException for empty RequestItems map', function (t) {
  assertValidation({ RequestItems: {} },
    '1 validation error detected: ' +
    'Value \'{}\' at \'requestItems\' failed to satisfy constraint: ' +
    'Member must have length greater than or equal to 1', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('batchGetItem - validations - should return ValidationException for short table name with no keys', function (t) {
  assertValidation({ RequestItems: { a: {} }, ReturnConsumedCapacity: 'hi', ReturnItemCollectionMetrics: 'hi' }, [
    'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
    'Member must satisfy enum value set: [INDEXES, TOTAL, NONE]',
    new RegExp('Value \'{.+}\' at \'requestItems\' ' +
      'failed to satisfy constraint: Map keys must satisfy constraint: ' +
      '\\[Member must have length less than or equal to 255, ' +
      'Member must have length greater than or equal to 3, ' +
      'Member must satisfy regular expression pattern: \\[a-zA-Z0-9_.-\\]\\+\\]'),
    'Value null at \'requestItems.a.member.keys\' failed to satisfy constraint: ' +
    'Member must not be null',
  ], function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('batchGetItem - validations - should return ValidationException for empty keys array', function (t) {
  assertValidation({ RequestItems: { a: { Keys: [] } } }, [
    new RegExp('Value \'{.+}\' at \'requestItems\' ' +
      'failed to satisfy constraint: Map keys must satisfy constraint: ' +
      '\\[Member must have length less than or equal to 255, ' +
      'Member must have length greater than or equal to 3, ' +
      'Member must satisfy regular expression pattern: \\[a-zA-Z0-9_.-\\]\\+\\]'),
    'Value \'[]\' at \'requestItems.a.member.keys\' failed to satisfy constraint: ' +
    'Member must have length greater than or equal to 1',
  ], function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('batchGetItem - validations - should return ValidationException for incorrect attributes', function (t) {
  assertValidation({ RequestItems: { 'aa;': {} }, ReturnConsumedCapacity: 'hi' }, [
    'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
    'Member must satisfy enum value set: [INDEXES, TOTAL, NONE]',
    new RegExp('Value \'{.+}\' at \'requestItems\' ' +
      'failed to satisfy constraint: Map keys must satisfy constraint: ' +
      '\\[Member must have length less than or equal to 255, ' +
      'Member must have length greater than or equal to 3, ' +
      'Member must satisfy regular expression pattern: \\[a-zA-Z0-9_.-\\]\\+\\]'),
    'Value null at \'requestItems.aa;.member.keys\' failed to satisfy constraint: ' +
    'Member must not be null',
  ], function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('batchGetItem - validations - should return ValidationException for short table name with keys', function (t) {
  assertValidation({ RequestItems: { a: { Keys: [ { a: { S: 'a' } } ] } } },
    new RegExp('1 validation error detected: ' +
      'Value \'{.+}\' at \'requestItems\' ' +
      'failed to satisfy constraint: Map keys must satisfy constraint: ' +
      '\\[Member must have length less than or equal to 255, ' +
      'Member must have length greater than or equal to 3, ' +
      'Member must satisfy regular expression pattern: \\[a-zA-Z0-9_.-\\]\\+\\]'), function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('batchGetItem - validations - should return ValidationException when fetching more than 100 keys', function (t) {
  const keys = []
  let i
  for (i = 0; i < 101; i++) {
    keys.push({ a: { S: String(i) } })
  }
  assertValidation({ RequestItems: { abc: { Keys: keys } } },
    new RegExp('1 validation error detected: ' +
      'Value \'\\[.+\\]\' at \'requestItems.abc.member.keys\' failed to satisfy constraint: ' +
      'Member must have length less than or equal to 100'), function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('batchGetItem - validations - should return ValidationException if filter expression and non-expression', function (t) {
  assertValidation({
    RequestItems: {
      abc: {
        Keys: [ {} ],
        AttributesToGet: [ 'a' ],
        ExpressionAttributeNames: {},
        ProjectionExpression: '',
      },
    },
  }, 'Can not use both expression and non-expression parameters in the same request: ' +
    'Non-expression parameters: {AttributesToGet} Expression parameters: {ProjectionExpression}', function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('batchGetItem - validations - should return ValidationException if ExpressionAttributeNames but no ProjectionExpression', function (t) {
  assertValidation({
    RequestItems: {
      abc: {
        Keys: [ {} ],
        AttributesToGet: [ 'a' ],
        ExpressionAttributeNames: {},
      },
    },
  }, 'ExpressionAttributeNames can only be specified when using expressions', function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('batchGetItem - validations - should return ValidationException for empty ExpressionAttributeNames', function (t) {
  assertValidation({
    RequestItems: {
      abc: {
        Keys: [ {} ],
        ExpressionAttributeNames: {},
        ProjectionExpression: '',
      },
    },
  }, 'ExpressionAttributeNames must not be empty', function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('batchGetItem - validations - should return ValidationException for invalid ExpressionAttributeNames', function (t) {
  assertValidation({
    RequestItems: {
      abc: {
        Keys: [ {} ],
        ExpressionAttributeNames: { 'a': 'a' },
        ProjectionExpression: '',
      },
    },
  }, 'ExpressionAttributeNames contains invalid key: Syntax error; key: "a"', function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('batchGetItem - validations - should return ValidationException for empty ProjectionExpression', function (t) {
  assertValidation({
    RequestItems: {
      abc: {
        Keys: [ {} ],
        ProjectionExpression: '',
      },
    },
  }, 'Invalid ProjectionExpression: The expression can not be empty;', function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('batchGetItem - validations - should return ValidationException when fetching more than 100 keys over multiple tables', function (t) {
  const keys = []
  let i
  for (i = 0; i < 100; i++) {
    keys.push({ a: { S: String(i) } })
  }
  assertValidation({ RequestItems: { abc: { Keys: keys }, abd: { Keys: [ { a: { S: '100' } } ] } } },
    'Too many items requested for the BatchGetItem call', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('batchGetItem - validations - should return ResourceNotFoundException when fetching exactly 100 keys and table does not exist', function (t) {
  const keys = []
  let i
  for (i = 0; i < 100; i++) {
    keys.push({ a: { S: String(i) } })
  }
  assertNotFound({ RequestItems: { abc: { Keys: keys } } },
    'Requested resource not found', function (err) {
      t.error(err, 'assertNotFound should not error')
      t.end()
    })
})

test('batchGetItem - validations - should return ValidationException for unsupported datatype in Key', function (t) {
  async.forEach([
    {},
    { a: '' },
    { M: { a: {} } },
    { L: [ {} ] },
    { L: [ { a: {} } ] },
  ], function (expr, cb) {
    assertValidation({ RequestItems: { abc: { Keys: [ { a: expr } ] } } },
      'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', cb)
  }, function (err) {
    t.error(err, 'async.forEach should complete without error')
    t.end()
  })
})

test('batchGetItem - validations - should return ValidationException for invalid values in Key', function (t) {
  async.forEach([
    [ { NULL: 'no' }, 'Null attribute value types must have the value of true' ],
    [ { SS: [] }, 'An string set  may not be empty' ],
    [ { NS: [] }, 'An number set  may not be empty' ],
    [ { BS: [] }, 'Binary sets should not be empty' ],
    [ { SS: [ 'a', 'a' ] }, 'Input collection [a, a] contains duplicates.' ],
    [ { BS: [ 'Yg==', 'Yg==' ] }, 'Input collection [Yg==, Yg==]of type BS contains duplicates.' ],
  ], function (expr, cb) {
    assertValidation({ RequestItems: { abc: { Keys: [ { a: expr[0] } ] } } },
      'One or more parameter values were invalid: ' + expr[1], cb)
  }, function (err) {
    t.error(err, 'async.forEach should complete without error')
    t.end()
  })
})

test('batchGetItem - validations - should return ValidationException for empty/invalid numbers in Key', function (t) {
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
    assertValidation({ RequestItems: { abc: { Keys: [ { a: expr[0] } ] } } }, expr[1], cb)
  }, function (err) {
    t.error(err, 'async.forEach should complete without error')
    t.end()
  })
})

test('batchGetItem - validations - should return ValidationException for multiple datatypes in Key', function (t) {
  assertValidation({ RequestItems: { abc: { Keys: [ { 'a': { S: 'a', N: '1' } } ] } } },
    'Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('batchGetItem - validations - should return ValidationException for empty ExpressionAttributeNames (duplicate test?)', function (t) {
  const key = { a: { S: helpers.randomString() }, b: { N: helpers.randomNumber() } }
  assertValidation({
    RequestItems: {
      abc: {
        Keys: [ key, { b: key.b, a: key.a }, key ],
        ExpressionAttributeNames: {},
        ProjectionExpression: '',
      },
    },
  }, 'ExpressionAttributeNames must not be empty', function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('batchGetItem - validations - should return ValidationException for empty ProjectionExpression (duplicate test?)', function (t) {
  const key = { a: { S: helpers.randomString() }, b: { N: helpers.randomNumber() } }
  assertValidation({
    RequestItems: {
      abc: {
        Keys: [ key, { b: key.b, a: key.a }, key ],
        ProjectionExpression: '',
      },
    },
  }, 'Invalid ProjectionExpression: The expression can not be empty;', function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('batchGetItem - validations - should return ValidationException for duplicated keys', function (t) {
  const key = { a: { S: helpers.randomString() }, b: { N: helpers.randomNumber() } }
  assertValidation({
    RequestItems: {
      abc: {
        Keys: [ key, { b: key.b, a: key.a }, key ],
      },
    },
  }, 'Provided list of item keys contains duplicates', function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('batchGetItem - validations - should return ValidationException for duplicated mixed up keys', function (t) {
  const key = { a: { S: helpers.randomString() } }
  const key2 = { a: { S: helpers.randomString() } }
  assertValidation({
    RequestItems: {
      abc: {
        Keys: [ key, key2, key ],
        AttributesToGet: [ 'a', 'a' ],
      },
    },
  }, 'One or more parameter values were invalid: Duplicate value in attribute name: a', function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('batchGetItem - validations - should return ValidationException duplicate values in AttributesToGet', function (t) {
  assertValidation({ RequestItems: { abc: { Keys: [ {} ], AttributesToGet: [ 'a', 'a' ] } } },
    'One or more parameter values were invalid: Duplicate value in attribute name: a', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('batchGetItem - validations - should return ResourceNotFoundException if key is empty and table does not exist', function (t) {
  const batchReq = { RequestItems: {} }
  batchReq.RequestItems[helpers.randomName()] = { Keys: [ {} ] }
  assertNotFound(batchReq,
    'Requested resource not found', function (err) {
      t.error(err, 'assertNotFound should not error')
      t.end()
    })
})

test('batchGetItem - validations - should return ValidationException if key does not match schema', function (t) {
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
    const batchReq = { RequestItems: {} }
    batchReq.RequestItems[helpers.testHashTable] = { Keys: [ expr ] }
    assertValidation(batchReq,
      'The provided key element does not match the schema', cb)
  }, function (err) {
    t.error(err, 'async.forEach should complete without error')
    t.end()
  })
})

test('batchGetItem - validations - should return ValidationException if range key does not match schema', function (t) {
  const batchReq = { RequestItems: {} }
  batchReq.RequestItems[helpers.testRangeTable] = { Keys: [ { a: { S: 'a' } } ] }
  assertValidation(batchReq,
    'The provided key element does not match the schema', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('batchGetItem - validations - should return ValidationException if hash key is too big', function (t) {
  const batchReq = { RequestItems: {} }
  const keyStr = (helpers.randomString() + new Array(2048).join('a')).slice(0, 2049)
  batchReq.RequestItems[helpers.testHashTable] = { Keys: [ { a: { S: keyStr } } ] }
  assertValidation(batchReq,
    'One or more parameter values were invalid: ' +
    'Size of hashkey has exceeded the maximum size limit of2048 bytes', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('batchGetItem - validations - should return ValidationException if range key is too big', function (t) {
  const batchReq = { RequestItems: {} }
  const keyStr = (helpers.randomString() + new Array(1024).join('a')).slice(0, 1025)
  batchReq.RequestItems[helpers.testRangeTable] = { Keys: [ { a: { S: 'a' }, b: { S: keyStr } } ] }
  assertValidation(batchReq,
    'One or more parameter values were invalid: ' +
    'Aggregated size of all range keys has exceeded the size limit of 1024 bytes', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('batchGetItem - validations - should return ResourceNotFoundException if table is being created', function (t) {
  const table = {
    TableName: helpers.randomName(),
    AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' } ],
    KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
    ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
  }
  helpers.request(helpers.opts('CreateTable', table), function (err) {
    if (err) {
      t.error(err, 'CreateTable should not error')
      return t.end() // End test early if setup fails
    }
    const batchReq = { RequestItems: {} }
    batchReq.RequestItems[table.TableName] = { Keys: [ { a: { S: 'a' } } ] }
    assertNotFound(batchReq, 'Requested resource not found', function (errNotFound) {
      t.error(errNotFound, 'assertNotFound should not error')
      helpers.deleteWhenActive(table.TableName, function (errDelete) {
        t.error(errDelete, 'deleteWhenActive should not error during cleanup')
        t.end()
      })
    })
  })
})
