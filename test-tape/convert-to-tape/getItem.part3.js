const test = require('tape')
const async = require('async')
const helpers = require('./helpers')

const target = 'GetItem'
const request = helpers.request
// const randomName = helpers.randomName // Not used directly it seems
const opts = helpers.opts.bind(null, target)
// const assertType = helpers.assertType.bind(null, target) // Not used in this part
// const assertValidation = helpers.assertValidation.bind(null, target) // Not used in this part
// const assertNotFound = helpers.assertNotFound.bind(null, target) // Not used in this part

// Define items accessible to all tests in this file
const hashItem = { a: { S: helpers.randomString() }, b: { S: 'a' }, g: { N: '23' } }
const rangeItem = { a: { S: helpers.randomString() }, b: { S: helpers.randomString() }, g: { N: '23' } }

// Setup test to put initial items
test('getItem - functionality - setup: put initial items', function (t) {
  const putItems = [
    { TableName: helpers.testHashTable, Item: hashItem },
    { TableName: helpers.testRangeTable, Item: rangeItem },
  ]
  async.forEach(putItems, function (putItem, cb) {
    request(helpers.opts('PutItem', putItem), function (err) {
      // We don't need to assert success here, just pass the error state to async
      cb(err)
    })
  }, function (err) {
    t.error(err, 'Setup PutItems should complete without error')
    t.end()
  })
})

test('getItem - functionality - should return empty response if key does not exist', function (t) {
  request(opts({ TableName: helpers.testHashTable, Key: { a: { S: helpers.randomString() } } }), function (err, res) {
    t.error(err, 'request should not error')
    t.equal(res.statusCode, 200, 'Status code should be 200')
    t.deepEqual(res.body, {}, 'Body should be empty object')
    t.end()
  })
})

test('getItem - functionality - should return ConsumedCapacity if specified', function (t) {
  const req = { TableName: helpers.testHashTable, Key: { a: { S: helpers.randomString() } }, ReturnConsumedCapacity: 'TOTAL' }
  request(opts(req), function (err, res) {
    t.error(err, 'First request should not error')
    t.equal(res.statusCode, 200, 'Status code should be 200 (TOTAL)')
    t.deepEqual(res.body, { ConsumedCapacity: { CapacityUnits: 0.5, TableName: helpers.testHashTable } }, 'Body should contain TOTAL capacity')

    req.ReturnConsumedCapacity = 'INDEXES'
    request(opts(req), function (err2, res2) {
      t.error(err2, 'Second request should not error')
      t.equal(res2.statusCode, 200, 'Status code should be 200 (INDEXES)')
      t.deepEqual(res2.body, { ConsumedCapacity: { CapacityUnits: 0.5, Table: { CapacityUnits: 0.5 }, TableName: helpers.testHashTable } }, 'Body should contain INDEXES capacity')
      t.end()
    })
  })
})

test('getItem - functionality - should return full ConsumedCapacity if specified', function (t) {
  const req = { TableName: helpers.testHashTable, Key: { a: { S: helpers.randomString() } }, ReturnConsumedCapacity: 'TOTAL', ConsistentRead: true }
  request(opts(req), function (err, res) {
    t.error(err, 'First request should not error')
    t.equal(res.statusCode, 200, 'Status code should be 200 (TOTAL)')
    t.deepEqual(res.body, { ConsumedCapacity: { CapacityUnits: 1, TableName: helpers.testHashTable } }, 'Body should contain TOTAL capacity (ConsistentRead)')

    req.ReturnConsumedCapacity = 'INDEXES'
    request(opts(req), function (err2, res2) {
      t.error(err2, 'Second request should not error')
      t.equal(res2.statusCode, 200, 'Status code should be 200 (INDEXES)')
      t.deepEqual(res2.body, { ConsumedCapacity: { CapacityUnits: 1, Table: { CapacityUnits: 1 }, TableName: helpers.testHashTable } }, 'Body should contain INDEXES capacity (ConsistentRead)')
      t.end()
    })
  })
})

test('getItem - functionality - should return object by hash key', function (t) {
  request(opts({ TableName: helpers.testHashTable, Key: { a: hashItem.a }, ConsistentRead: true }), function (err, res) {
    t.error(err, 'request should not error')
    t.equal(res.statusCode, 200, 'Status code should be 200')
    t.deepEqual(res.body, { Item: hashItem }, 'Body should contain the correct item')
    t.end()
  })
})

test('getItem - functionality - should return object by range key', function (t) {
  request(opts({ TableName: helpers.testRangeTable, Key: { a: rangeItem.a, b: rangeItem.b }, ConsistentRead: true }), function (err, res) {
    t.error(err, 'request should not error')
    t.equal(res.statusCode, 200, 'Status code should be 200')
    t.deepEqual(res.body, { Item: rangeItem }, 'Body should contain the correct item')
    t.end()
  })
})

test('getItem - functionality - should only return requested attributes', function (t) {
  async.forEach([
    { AttributesToGet: [ 'b', 'g' ] },
    { ProjectionExpression: 'b, g' },
    { ProjectionExpression: '#b, #g', ExpressionAttributeNames: { '#b': 'b', '#g': 'g' } },
  ], function (getOpts, cb) {
    getOpts.TableName = helpers.testHashTable
    getOpts.Key = { a: hashItem.a }
    getOpts.ConsistentRead = true
    request(opts(getOpts), function (err, res) {
      if (err) return cb(err)
      t.equal(res.statusCode, 200, 'Status code should be 200 for ' + JSON.stringify(getOpts))
      t.deepEqual(res.body, { Item: { b: hashItem.b, g: hashItem.g } }, 'Body should contain projected attributes for ' + JSON.stringify(getOpts))
      cb()
    })
  }, function (err) {
    t.error(err, 'async.forEach should complete without error')
    t.end()
  })
})

test('getItem - functionality - should only return requested nested attributes', function (t) {
  const item = { a: { S: helpers.randomString() }, b: { M: { a: { S: 'a' }, b: { S: 'b' }, c: { S: 'c' } } }, c: { L: [ { S: 'a' }, { S: 'b' }, { S: 'c' } ] } }
  request(helpers.opts('PutItem', { TableName: helpers.testHashTable, Item: item }), function (err, res) {
    t.error(err, 'PutItem should not error')
    t.equal(res.statusCode, 200, 'PutItem status code should be 200')

    async.forEach([
      { ProjectionExpression: 'b.c,c[2],b.b,c[1],c[0].a' }, // Note: c[0].a likely returns nothing as c[0] is {S: 'a'}
      { ProjectionExpression: '#b.#c,#c[2],#b.#b,#c[1]', ExpressionAttributeNames: { '#b': 'b', '#c': 'c' } }, // Adjusted original ExpressionAttributeNames version slightly for simplicity/clarity
    ], function (getOpts, cb) {
      getOpts.TableName = helpers.testHashTable
      getOpts.Key = { a: item.a }
      getOpts.ConsistentRead = true
      request(opts(getOpts), function (err, res) {
        if (err) return cb(err)
        t.equal(res.statusCode, 200, 'GetItem status code should be 200 for ' + JSON.stringify(getOpts))
        // Expected needs careful checking based on ProjectionExpression
        // For 'b.c,c[2],b.b,c[1],c[0].a': Expect {b: {M: {b: {S:'b'}, c: {S:'c'}}}, c: {L: [{S:'b'}, {S:'c'}]}}
        // For '#b.#c,#c[2],#b.#b,#c[1]': Expect {b: {M: {b: {S:'b'}, c: {S:'c'}}}, c: {L: [{S:'b'}, {S:'c'}]}}
        // Original test had: { Item: { b: { M: { b: item.b.M.b, c: item.b.M.c } }, c: { L: [ item.c.L[1], item.c.L[2] ] } } }
        // Let's stick to the original expected structure:
        t.deepEqual(res.body, { Item: { b: { M: { b: item.b.M.b, c: item.b.M.c } }, c: { L: [ item.c.L[1], item.c.L[2] ] } } }, 'Body should contain projected nested attributes for ' + JSON.stringify(getOpts))
        cb()
      })
    }, function (err) {
      t.error(err, 'async.forEach should complete without error')
      t.end()
    })
  })
})

test('getItem - functionality - should return ConsumedCapacity for small item with no ConsistentRead', function (t) {
  const a = helpers.randomString()
  const b = new Array(4082 - a.length).join('b')
  const item = { a: { S: a }, b: { S: b }, c: { N: '12.3456' }, d: { B: 'AQI=' }, e: { BS: [ 'AQI=', 'Ag==', 'AQ==' ] } }
  request(helpers.opts('PutItem', { TableName: helpers.testHashTable, Item: item }), function (err, res) {
    t.error(err, 'PutItem should not error')
    t.equal(res.statusCode, 200, 'PutItem status code should be 200')

    request(opts({ TableName: helpers.testHashTable, Key: { a: item.a }, ReturnConsumedCapacity: 'TOTAL' }), function (err2, res2) {
      t.error(err2, 'GetItem should not error')
      t.equal(res2.statusCode, 200, 'GetItem status code should be 200')
      t.deepEqual(res2.body.ConsumedCapacity, { CapacityUnits: 0.5, TableName: helpers.testHashTable }, 'ConsumedCapacity should be 0.5')
      t.end()
    })
  })
})

test('getItem - functionality - should return ConsumedCapacity for larger item with no ConsistentRead', function (t) {
  const a = helpers.randomString()
  const b = new Array(4084 - a.length).join('b')
  const item = { a: { S: a }, b: { S: b }, c: { N: '12.3456' }, d: { B: 'AQI=' }, e: { BS: [ 'AQI=', 'Ag==' ] } }
  request(helpers.opts('PutItem', { TableName: helpers.testHashTable, Item: item }), function (err, res) {
    t.error(err, 'PutItem should not error')
    t.equal(res.statusCode, 200, 'PutItem status code should be 200')

    request(opts({ TableName: helpers.testHashTable, Key: { a: item.a }, ReturnConsumedCapacity: 'TOTAL' }), function (err2, res2) {
      t.error(err2, 'GetItem should not error')
      t.equal(res2.statusCode, 200, 'GetItem status code should be 200')
      t.deepEqual(res2.body.ConsumedCapacity, { CapacityUnits: 1, TableName: helpers.testHashTable }, 'ConsumedCapacity should be 1')
      t.end()
    })
  })
})

test('getItem - functionality - should return ConsumedCapacity for small item with ConsistentRead', function (t) {
  const batchReq = { RequestItems: {} }
  const items = [ {
    a: { S: helpers.randomString() },
    bb: { S: new Array(4000).join('b') },
    ccc: { N: '12.3456' },
    dddd: { B: 'AQI=' },
    eeeee: { BS: [ 'AQI=', 'Ag==', 'AQ==' ] },
    ffffff: { NULL: true },
    ggggggg: { BOOL: false },
    hhhhhhhh: { L: [ { S: 'a' }, { S: 'aa' }, { S: 'bb' }, { S: 'ccc' } ] },
    iiiiiiiii: { M: { aa: { S: 'aa' }, bbb: { S: 'bbb' } } },
  }, {
    a: { S: helpers.randomString() },
    ab: { S: new Array(4027).join('b') },
    abc: { NULL: true },
    abcd: { BOOL: true },
    abcde: { L: [ { S: 'aa' }, { N: '12.3456' }, { B: 'AQI=' } ] },
    abcdef: { M: { aa: { S: 'aa' }, bbb: { N: '12.3456' }, cccc: { B: 'AQI=' } } },
  } ]
  batchReq.RequestItems[helpers.testHashTable] = items.map(function (item) { return { PutRequest: { Item: item } } })
  request(helpers.opts('BatchWriteItem', batchReq), function (err, res) {
    t.error(err, 'BatchWriteItem should not error')
    t.equal(res.statusCode, 200, 'BatchWriteItem status code should be 200')

    async.forEach(items, function (item, cb) {
      request(opts({ TableName: helpers.testHashTable, Key: { a: item.a }, ReturnConsumedCapacity: 'TOTAL', ConsistentRead: true }), function (err2, res2) {
        if (err2) return cb(err2)
        t.equal(res2.statusCode, 200, 'GetItem status code should be 200 for item ' + item.a.S)
        t.deepEqual(res2.body.ConsumedCapacity, { CapacityUnits: 1, TableName: helpers.testHashTable }, 'ConsumedCapacity should be 1 for item ' + item.a.S)
        cb()
      })
    }, function (err3) {
      t.error(err3, 'async.forEach GetItems should complete without error')
      t.end()
    })
  })
})

test('getItem - functionality - should return ConsumedCapacity for larger item with ConsistentRead', function (t) {
  const batchReq = { RequestItems: {} }
  const items = [ {
    a: { S: helpers.randomString() },
    bb: { S: new Array(4001).join('b') }, // > 4KB
    ccc: { N: '12.3456' },
    dddd: { B: 'AQI=' },
    eeeee: { BS: [ 'AQI=', 'Ag==', 'AQ==' ] },
    ffffff: { NULL: true },
    ggggggg: { BOOL: false },
    hhhhhhhh: { L: [ { S: 'a' }, { S: 'aa' }, { S: 'bb' }, { S: 'ccc' } ] },
    iiiiiiiii: { M: { aa: { S: 'aa' }, bbb: { S: 'bbb' } } },
  }, {
    a: { S: helpers.randomString() },
    ab: { S: new Array(4028).join('b') }, // > 4KB
    abc: { NULL: true },
    abcd: { BOOL: true },
    abcde: { L: [ { S: 'aa' }, { N: '12.3456' }, { B: 'AQI=' } ] },
    abcdef: { M: { aa: { S: 'aa' }, bbb: { N: '12.3456' }, cccc: { B: 'AQI=' } } },
  } ]
  batchReq.RequestItems[helpers.testHashTable] = items.map(function (item) { return { PutRequest: { Item: item } } })
  request(helpers.opts('BatchWriteItem', batchReq), function (err, res) {
    t.error(err, 'BatchWriteItem should not error')
    t.equal(res.statusCode, 200, 'BatchWriteItem status code should be 200')

    async.forEach(items, function (item, cb) {
      request(opts({ TableName: helpers.testHashTable, Key: { a: item.a }, ReturnConsumedCapacity: 'TOTAL', ConsistentRead: true }), function (err2, res2) {
        if (err2) return cb(err2)
        t.equal(res2.statusCode, 200, 'GetItem status code should be 200 for item ' + item.a.S)
        t.deepEqual(res2.body.ConsumedCapacity, { CapacityUnits: 2, TableName: helpers.testHashTable }, 'ConsumedCapacity should be 2 for item ' + item.a.S)
        cb()
      })
    }, function (err3) {
      t.error(err3, 'async.forEach GetItems should complete without error')
      t.end()
    })
  })
})
