const test = require('tape')
const async = require('async')
const helpers = require('./helpers')
// const db = require('../../db') // Not used in these tests

const target = 'BatchWriteItem'
// const request = helpers.request // Not used
// const randomName = helpers.randomName // Not used
// const opts = helpers.opts.bind(null, target) // Not used
// const assertType = helpers.assertType.bind(null, target) // Not used
const assertValidation = helpers.assertValidation.bind(null, target)
const assertNotFound = helpers.assertNotFound.bind(null, target)

test('batchWriteItem - validations - should return ValidationException for empty body', function (t) {
  assertValidation({},
    '1 validation error detected: ' +
    'Value null at \'requestItems\' failed to satisfy constraint: ' +
    'Member must not be null', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('batchWriteItem - validations - should return ValidationException for missing RequestItems', function (t) {
  assertValidation({ ReturnConsumedCapacity: 'hi', ReturnItemCollectionMetrics: 'hi' }, [
    'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
    'Member must satisfy enum value set: [INDEXES, TOTAL, NONE]',
    'Value \'hi\' at \'returnItemCollectionMetrics\' failed to satisfy constraint: ' +
    'Member must satisfy enum value set: [SIZE, NONE]',
    'Value null at \'requestItems\' failed to satisfy constraint: ' +
    'Member must not be null',
  ], function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('batchWriteItem - validations - should return ValidationException for empty RequestItems map', function (t) {
  assertValidation({ RequestItems: {} },
    '1 validation error detected: ' +
    'Value \'{}\' at \'requestItems\' failed to satisfy constraint: ' +
    'Member must have length greater than or equal to 1', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('batchWriteItem - validations - should return ValidationException for empty list in RequestItems', function (t) {
  assertValidation({ RequestItems: { a: [] } }, [
    new RegExp('Value \'{.+}\' at \'requestItems\' failed to satisfy constraint: ' +
      'Map keys must satisfy constraint: ' +
      '\\[Member must have length less than or equal to 255, ' +
      'Member must have length greater than or equal to 3, ' +
      'Member must satisfy regular expression pattern: \\[a-zA-Z0-9_.-\\]\\+\\]'),
    new RegExp('Value \'{.+}\' at \'requestItems\' failed to satisfy constraint: ' +
      'Map value must satisfy constraint: ' +
      '\\[Member must have length less than or equal to 25, ' +
      'Member must have length greater than or equal to 1\\]'),
  ], function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('batchWriteItem - validations - should return ValidationException for empty item in RequestItems', function (t) {
  assertValidation({ RequestItems: { abc: [ {} ] } },
    'Supplied AttributeValue has more than one datatypes set, ' +
    'must contain exactly one of the supported datatypes', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('batchWriteItem - validations - should return ValidationException for short table name and missing requests', function (t) {
  assertValidation({ RequestItems: { a: [] }, ReturnConsumedCapacity: 'hi', ReturnItemCollectionMetrics: 'hi' }, [
    'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
    'Member must satisfy enum value set: [INDEXES, TOTAL, NONE]',
    'Value \'hi\' at \'returnItemCollectionMetrics\' failed to satisfy constraint: ' +
    'Member must satisfy enum value set: [SIZE, NONE]',
    new RegExp('Value \'{.+}\' at \'requestItems\' failed to satisfy constraint: ' +
      'Map keys must satisfy constraint: ' +
      '\\[Member must have length less than or equal to 255, ' +
      'Member must have length greater than or equal to 3, ' +
      'Member must satisfy regular expression pattern: \\[a-zA-Z0-9_.-\\]\\+\\]'),
    new RegExp('Value \'{.+}\' at \'requestItems\' failed to satisfy constraint: ' +
      'Map value must satisfy constraint: ' +
      '\\[Member must have length less than or equal to 25, ' +
      'Member must have length greater than or equal to 1\\]'),
  ], function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('batchWriteItem - validations - should return ValidationException for incorrect attributes', function (t) {
  assertValidation({ RequestItems: { 'aa;': [ { PutRequest: {}, DeleteRequest: {} } ] },
    ReturnConsumedCapacity: 'hi', ReturnItemCollectionMetrics: 'hi' }, [
    'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
    'Member must satisfy enum value set: [INDEXES, TOTAL, NONE]',
    'Value \'hi\' at \'returnItemCollectionMetrics\' failed to satisfy constraint: ' +
    'Member must satisfy enum value set: [SIZE, NONE]',
    new RegExp('Value \'{.+}\' at \'requestItems\' ' +
    'failed to satisfy constraint: Map keys must satisfy constraint: ' +
    '\\[Member must have length less than or equal to 255, ' +
    'Member must have length greater than or equal to 3, ' +
    'Member must satisfy regular expression pattern: \\[a-zA-Z0-9_.-\\]\\+\\]'),
    'Value null at \'requestItems.aa;.member.1.member.deleteRequest.key\' failed to satisfy constraint: ' +
    'Member must not be null',
    'Value null at \'requestItems.aa;.member.1.member.putRequest.item\' failed to satisfy constraint: ' +
    'Member must not be null',
  ], function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('batchWriteItem - validations - should return ValidationException when putting more than 25 items', function (t) {
  const requests = []
  let i
  for (i = 0; i < 26; i++) {
    requests.push(i % 2 ? { DeleteRequest: { Key: { a: { S: String(i) } } } } : { PutRequest: { Item: { a: { S: String(i) } } } })
  }
  assertValidation({ RequestItems: { abc: requests } },
    new RegExp('1 validation error detected: ' +
      'Value \'{.+}\' at \'requestItems\' failed to satisfy constraint: ' +
      'Map value must satisfy constraint: ' +
      '\\[Member must have length less than or equal to 25, ' +
      'Member must have length greater than or equal to 1\\]'), function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('batchWriteItem - validations - should return ResourceNotFoundException when fetching exactly 25 items and table does not exist', function (t) {
  const requests = []
  let i
  for (i = 0; i < 25; i++) {
    requests.push(i % 2 ? { DeleteRequest: { Key: { a: { S: String(i) } } } } : { PutRequest: { Item: { a: { S: String(i) } } } })
  }
  assertNotFound({ RequestItems: { abc: requests } },
    'Requested resource not found', function (err) {
      t.error(err, 'assertNotFound should not error')
      t.end()
    })
})

test('batchWriteItem - validations - should check table exists first before checking for duplicate keys', function (t) {
  const item = { a: { S: helpers.randomString() }, c: { S: 'c' } }
  assertNotFound({ RequestItems: { abc: [ { PutRequest: { Item: item } }, { DeleteRequest: { Key: { a: item.a } } } ] } },
    'Requested resource not found', function (err) {
      t.error(err, 'assertNotFound should not error')
      t.end()
    })
})

test('batchWriteItem - validations - should return ValidationException for puts and deletes of the same item with put first', function (t) {
  const item = { a: { S: helpers.randomString() }, c: { S: 'c' } }
  const batchReq = { RequestItems: {} }
  batchReq.RequestItems[helpers.testHashTable] = [ { PutRequest: { Item: item } }, { DeleteRequest: { Key: { a: item.a } } } ]
  assertValidation(batchReq, 'Provided list of item keys contains duplicates', function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('batchWriteItem - validations - should return ValidationException for puts and deletes of the same item with delete first', function (t) {
  const item = { a: { S: helpers.randomString() }, c: { S: 'c' } }
  const batchReq = { RequestItems: {} }
  batchReq.RequestItems[helpers.testHashTable] = [ { DeleteRequest: { Key: { a: item.a } } }, { PutRequest: { Item: item } } ]
  assertValidation(batchReq, 'Provided list of item keys contains duplicates', function (err) {
    t.error(err, 'assertValidation should not error')
    t.end()
  })
})

test('batchWriteItem - validations - should return ValidationException for short table name', function (t) {
  assertValidation({ RequestItems: { a: [ { PutRequest: { Item: { a: { S: 'a' } } } } ] } },
    new RegExp('1 validation error detected: ' +
      'Value \'{.+}\' at \'requestItems\' ' +
      'failed to satisfy constraint: ' +
      'Map keys must satisfy constraint: ' +
      '\\[Member must have length less than or equal to 255, ' +
      'Member must have length greater than or equal to 3, ' +
      'Member must satisfy regular expression pattern: \\[a-zA-Z0-9_.-\\]\\+\\]'), function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('batchWriteItem - validations - should return ValidationException for unsupported datatype in Item', function (t) {
  async.forEach([
    {},
    { a: '' },
    { M: { a: {} } },
    { L: [ {} ] },
    { L: [ { a: {} } ] },
  ], function (expr, cb) {
    assertValidation({ RequestItems: { abc: [ { PutRequest: { Item: { a: expr } } } ] } },
      'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', cb)
  }, function (err) {
    t.error(err, 'async.forEach should complete without error')
    t.end()
  })
})

test('batchWriteItem - validations - should return ValidationException for invalid values in Item', function (t) {
  async.forEach([
    [ { NULL: 'no' }, 'Null attribute value types must have the value of true' ],
    [ { SS: [] }, 'An string set  may not be empty' ],
    [ { NS: [] }, 'An number set  may not be empty' ],
    [ { BS: [] }, 'Binary sets should not be empty' ],
    [ { SS: [ 'a', 'a' ] }, 'Input collection [a, a] contains duplicates.' ],
    [ { BS: [ 'Yg==', 'Yg==' ] }, 'Input collection [Yg==, Yg==]of type BS contains duplicates.' ],
  ], function (expr, cb) {
    assertValidation({ RequestItems: { abc: [ { PutRequest: { Item: { a: expr[0] } } } ] } },
      'One or more parameter values were invalid: ' + expr[1], cb)
  }, function (err) {
    t.error(err, 'async.forEach should complete without error')
    t.end()
  })
})

test('batchWriteItem - validations - should return ValidationException for empty/invalid numbers in Item', function (t) {
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
    assertValidation({ RequestItems: { abc: [ { PutRequest: { Item: { a: expr[0] } } } ] } }, expr[1], cb)
  }, function (err) {
    t.error(err, 'async.forEach should complete without error')
    t.end()
  })
})

test('batchWriteItem - validations - should return ValidationException for multiple datatypes in Item', function (t) {
  assertValidation({ RequestItems: { abc: [ { PutRequest: { Item: { a: { S: 'a', N: '1' } } } } ] } },
    'Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('batchWriteItem - validations - should return ValidationException if item is too big with small attribute', function (t) {
  const keyStr = helpers.randomString()
  const b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1).join('a')
  assertValidation({ RequestItems: { abc: [ { PutRequest: { Item: { a: { S: keyStr }, b: { S: b } } } } ] } },
    'Item size has exceeded the maximum allowed size', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('batchWriteItem - validations - should return ResourceNotFoundException if item is just small enough with small attribute', function (t) {
  const keyStr = helpers.randomString()
  const b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 2).join('a')
  assertNotFound({ RequestItems: { abc: [ { PutRequest: { Item: { a: { S: keyStr }, b: { S: b } } } } ] } },
    'Requested resource not found', function (err) {
      t.error(err, 'assertNotFound should not error')
      t.end()
    })
})

test('batchWriteItem - validations - should return ValidationException if item is too big with larger attribute', function (t) {
  const keyStr = helpers.randomString()
  const b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 27).join('a')
  assertValidation({ RequestItems: { abc: [ { PutRequest: { Item: { a: { S: keyStr }, bbbbbbbbbbbbbbbbbbbbbbbbbbb: { S: b } } } } ] } },
    'Item size has exceeded the maximum allowed size', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('batchWriteItem - validations - should return ResourceNotFoundException if item is just small enough with larger attribute', function (t) {
  const keyStr = helpers.randomString()
  const b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 28).join('a')
  assertNotFound({ RequestItems: { abc: [ { PutRequest: { Item: { a: { S: keyStr }, bbbbbbbbbbbbbbbbbbbbbbbbbbb: { S: b } } } } ] } },
    'Requested resource not found', function (err) {
      t.error(err, 'assertNotFound should not error')
      t.end()
    })
})

test('batchWriteItem - validations - should return ValidationException if item is too big with multi attributes', function (t) {
  const keyStr = helpers.randomString()
  const b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 7).join('a')
  assertValidation({ RequestItems: { abc: [ { PutRequest: { Item: { a: { S: keyStr }, bb: { S: b }, ccc: { S: 'cc' } } } } ] } },
    'Item size has exceeded the maximum allowed size', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('batchWriteItem - validations - should return ResourceNotFoundException if item is just small enough with multi attributes', function (t) {
  const keyStr = helpers.randomString()
  const b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 8).join('a')
  assertNotFound({ RequestItems: { abc: [ { PutRequest: { Item: { a: { S: keyStr }, bb: { S: b }, ccc: { S: 'cc' } } } } ] } },
    'Requested resource not found', function (err) {
      t.error(err, 'assertNotFound should not error')
      t.end()
    })
})

test('batchWriteItem - validations - should return ValidationException if item is too big with big number attribute', function (t) {
  const keyStr = helpers.randomString()
  const b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 20).join('a')
  const c = new Array(38 + 1).join('1') + new Array(89).join('0')
  assertValidation({ RequestItems: { abc: [ { PutRequest: { Item: { a: { S: keyStr }, b: { S: b }, c: { N: c } } } } ] } },
    'Item size has exceeded the maximum allowed size', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('batchWriteItem - validations - should return ValidationException if item is too big with smallest number attribute', function (t) {
  const keyStr = helpers.randomString()
  const b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 2).join('a')
  const c = '1' + new Array(126).join('0')
  assertValidation({ RequestItems: { abc: [ { PutRequest: { Item: { a: { S: keyStr }, b: { S: b }, c: { N: c } } } } ] } },
    'Item size has exceeded the maximum allowed size', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('batchWriteItem - validations - should return ValidationException if item is too big with smaller number attribute', function (t) {
  const keyStr = helpers.randomString()
  const b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 2).join('a')
  const c = '11' + new Array(125).join('0')
  assertValidation({ RequestItems: { abc: [ { PutRequest: { Item: { a: { S: keyStr }, b: { S: b }, c: { N: c } } } } ] } },
    'Item size has exceeded the maximum allowed size', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('batchWriteItem - validations - should return ValidationException if item is too big with medium number attribute (1)', function (t) {
  const keyStr = helpers.randomString()
  const b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 4).join('a')
  const c = '11111' + new Array(122).join('0')
  assertValidation({ RequestItems: { abc: [ { PutRequest: { Item: { a: { S: keyStr }, b: { S: b }, c: { N: c } } } } ] } },
    'Item size has exceeded the maximum allowed size', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('batchWriteItem - validations - should return ValidationException if item is too big with medium number attribute (2)', function (t) {
  const keyStr = helpers.randomString()
  const b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 4).join('a')
  const c = '111111' + new Array(121).join('0')
  assertValidation({ RequestItems: { abc: [ { PutRequest: { Item: { a: { S: keyStr }, b: { S: b }, c: { N: c } } } } ] } },
    'Item size has exceeded the maximum allowed size', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('batchWriteItem - validations - should return ValidationException if item is too big with medium number attribute (3)', function (t) {
  const keyStr = helpers.randomString()
  const b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 5).join('a')
  const c = '1111111' + new Array(120).join('0')
  assertValidation({ RequestItems: { abc: [ { PutRequest: { Item: { a: { S: keyStr }, b: { S: b }, c: { N: c } } } } ] } },
    'Item size has exceeded the maximum allowed size', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('batchWriteItem - validations - should return ValidationException if item is too big with multi number attribute', function (t) {
  const keyStr = helpers.randomString()
  const b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 5 - 1 - 5).join('a')
  const c = '1111111' + new Array(120).join('0')
  const d = '1111111' + new Array(120).join('0')
  assertValidation({ RequestItems: { abc: [ { PutRequest: { Item: { a: { S: keyStr }, b: { S: b }, c: { N: c }, d: { N: d } } } } ] } },
    'Item size has exceeded the maximum allowed size', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('batchWriteItem - validations - should return ResourceNotFoundException if item is just small enough with multi number attribute', function (t) {
  const keyStr = helpers.randomString()
  const b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 5 - 1 - 6).join('a')
  const c = '1111111' + new Array(120).join('0')
  const d = '1111111' + new Array(120).join('0')
  assertNotFound({ RequestItems: { abc: [ { PutRequest: { Item: { a: { S: keyStr }, b: { S: b }, c: { N: c }, d: { N: d } } } } ] } },
    'Requested resource not found', function (err) {
      t.error(err, 'assertNotFound should not error')
      t.end()
    })
})

test('batchWriteItem - validations - should return ResourceNotFoundException if key is empty and table does not exist', function (t) {
  const batchReq = { RequestItems: {} }
  batchReq.RequestItems[helpers.randomName()] = [ { PutRequest: { Item: {} } } ]
  assertNotFound(batchReq,
    'Requested resource not found', function (err) {
      t.error(err, 'assertNotFound should not error')
      t.end()
    })
})

test('batchWriteItem - validations - should return ValidationException if key does not match schema', function (t) {
  async.forEach([
    {},
    { b: { S: 'a' } },
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
    batchReq.RequestItems[helpers.testHashTable] = [ { PutRequest: { Item: expr } } ]
    assertValidation(batchReq,
      'The provided key element does not match the schema', cb)
  }, function (err) {
    t.error(err, 'async.forEach should complete without error')
    t.end()
  })
})

test('batchWriteItem - validations - should return ValidationException if range key does not match schema', function (t) {
  const batchReq = { RequestItems: {} }
  batchReq.RequestItems[helpers.testRangeTable] = [ { PutRequest: { Item: { a: { S: 'a' } } } } ]
  assertValidation(batchReq,
    'The provided key element does not match the schema', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('batchWriteItem - validations - should return ValidationException if secondary index key is incorrect type', function (t) {
  const batchReq = { RequestItems: {} }
  batchReq.RequestItems[helpers.testRangeTable] = [ { PutRequest: { Item: { a: { S: 'a' }, b: { S: 'a' }, c: { N: '1' } } } } ]
  assertValidation(batchReq,
    new RegExp('^One or more parameter values were invalid: ' +
      'Type mismatch for Index Key c Expected: S Actual: N IndexName: index\\d$'), function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('batchWriteItem - validations - should return ValidationException if hash key is too big', function (t) {
  const batchReq = { RequestItems: {} }
  const keyStr = (helpers.randomString() + new Array(2048).join('a')).slice(0, 2049)
  batchReq.RequestItems[helpers.testHashTable] = [ { PutRequest: { Item: { a: { S: keyStr } } } } ]
  assertValidation(batchReq,
    'One or more parameter values were invalid: ' +
    'Size of hashkey has exceeded the maximum size limit of2048 bytes', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('batchWriteItem - validations - should return ValidationException if range key is too big', function (t) {
  const batchReq = { RequestItems: {} }
  const keyStr = (helpers.randomString() + new Array(1024).join('a')).slice(0, 1025)
  batchReq.RequestItems[helpers.testRangeTable] = [ { PutRequest: { Item: { a: { S: 'a' }, b: { S: keyStr } } } } ]
  assertValidation(batchReq,
    'One or more parameter values were invalid: ' +
    'Aggregated size of all range keys has exceeded the size limit of 1024 bytes', function (err) {
      t.error(err, 'assertValidation should not error')
      t.end()
    })
})

test('batchWriteItem - validations - should return ResourceNotFoundException if table is being created', function (t) {
  const table = {
    TableName: helpers.randomName(),
    AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' } ],
    KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
    ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
  }
  helpers.request(helpers.opts('CreateTable', table), function (err, res) {
    if (err) {
      t.error(err, 'CreateTable request should not return error')
      return t.end()
    }
    t.equal(res.statusCode, 200, 'CreateTable should return status code 200')
    const batchReq = { RequestItems: {} }
    batchReq.RequestItems[table.TableName] = [ { PutRequest: { Item: { a: { S: 'a' } } } } ]
    assertNotFound(batchReq, 'Requested resource not found', function (errNotFound) {
      t.error(errNotFound, 'assertNotFound should not error')
      helpers.deleteWhenActive(table.TableName, function (errDelete) {
        t.error(errDelete, 'deleteWhenActive should not error during cleanup')
        t.end()
      })
    })
  })
})
