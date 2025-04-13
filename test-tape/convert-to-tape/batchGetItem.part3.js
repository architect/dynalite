const test = require('tape')
const async = require('async')
const helpers = require('./helpers')

const target = 'BatchGetItem'
const request = helpers.request
// const randomName = helpers.randomName // Removed unused variable
const opts = helpers.opts.bind(null, target)
// const assertType = helpers.assertType.bind(null, target)
// const assertValidation = helpers.assertValidation.bind(null, target)
// const assertNotFound = helpers.assertNotFound.bind(null, target)
const runSlowTests = helpers.runSlowTests

test('batchGetItem - functionality - should return empty responses if keys do not exist', function (t) {
  const batchReq = { RequestItems: {} }
  batchReq.RequestItems[helpers.testHashTable] = { Keys: [ { a: { S: helpers.randomString() } } ] }
  batchReq.RequestItems[helpers.testRangeTable] = { Keys: [ { a: { S: helpers.randomString() }, b: { S: helpers.randomString() } } ] }
  request(opts(batchReq), function (err, res) {
    t.error(err, 'request should not return error')
    t.equal(res.statusCode, 200, 'should return status code 200')
    t.deepEqual(res.body.Responses[helpers.testHashTable], [], 'should return empty array for testHashTable')
    t.deepEqual(res.body.Responses[helpers.testRangeTable], [], 'should return empty array for testRangeTable')
    t.deepEqual(res.body.UnprocessedKeys, {}, 'should return empty UnprocessedKeys')
    t.end()
  })
})

test('batchGetItem - functionality - should return only items that do exist', function (t) {
  const item = { a: { S: helpers.randomString() }, b: { N: helpers.randomNumber() } }
  const item2 = { a: { S: helpers.randomString() }, b: item.b }
  const item3 = { a: { S: helpers.randomString() }, b: { N: helpers.randomNumber() } }
  let batchReq = { RequestItems: {} }
  batchReq.RequestItems[helpers.testHashTable] = [
    { PutRequest: { Item: item } },
    { PutRequest: { Item: item2 } },
    { PutRequest: { Item: item3 } },
  ]
  request(helpers.opts('BatchWriteItem', batchReq), function (err, res) {
    t.error(err, 'BatchWriteItem request should not return error')
    t.equal(res.statusCode, 200, 'BatchWriteItem should return status code 200')
    batchReq = { RequestItems: {} }
    batchReq.RequestItems[helpers.testHashTable] = { Keys: [
      { a: item.a },
      { a: { S: helpers.randomString() } },
      { a: item3.a },
      { a: { S: helpers.randomString() } },
    ], ConsistentRead: true }
    request(opts(batchReq), function (err, res) {
      t.error(err, 'BatchGetItem request should not return error')
      t.equal(res.statusCode, 200, 'BatchGetItem should return status code 200')
      t.ok(res.body.Responses[helpers.testHashTable].find(resItem => JSON.stringify(resItem) === JSON.stringify(item)), 'Responses should contain item')
      t.ok(res.body.Responses[helpers.testHashTable].find(resItem => JSON.stringify(resItem) === JSON.stringify(item3)), 'Responses should contain item3')
      t.equal(res.body.Responses[helpers.testHashTable].length, 2, 'Responses should have length 2')
      t.deepEqual(res.body.UnprocessedKeys, {}, 'should return empty UnprocessedKeys')
      t.end()
    })
  })
})

test('batchGetItem - functionality - should return only requested attributes of items that do exist', function (t) {
  const item = { a: { S: helpers.randomString() }, b: { N: helpers.randomNumber() }, c: { S: 'c' } }
  const item2 = { a: { S: helpers.randomString() }, b: item.b }
  const item3 = { a: { S: helpers.randomString() }, b: { N: helpers.randomNumber() } }
  const item4 = { a: { S: helpers.randomString() } }
  let batchReq = { RequestItems: {} }
  batchReq.RequestItems[helpers.testHashTable] = [
    { PutRequest: { Item: item } },
    { PutRequest: { Item: item2 } },
    { PutRequest: { Item: item3 } },
    { PutRequest: { Item: item4 } },
  ]
  request(helpers.opts('BatchWriteItem', batchReq), function (err, res) {
    t.error(err, 'BatchWriteItem request should not return error')
    t.equal(res.statusCode, 200, 'BatchWriteItem should return status code 200')
    async.forEach([
      { AttributesToGet: [ 'b', 'c' ] },
      { ProjectionExpression: 'b, c' },
      { ProjectionExpression: '#b, #c', ExpressionAttributeNames: { '#b': 'b', '#c': 'c' } },
    ], function (batchOpts, cb) {
      batchReq = { RequestItems: {} }
      batchReq.RequestItems[helpers.testHashTable] = batchOpts
      batchOpts.Keys = [
        { a: item.a },
        { a: { S: helpers.randomString() } },
        { a: item3.a },
        { a: { S: helpers.randomString() } },
        { a: item4.a },
      ]
      batchOpts.ConsistentRead = true
      request(opts(batchReq), function (err, res) {
        if (err) return cb(err)
        t.equal(res.statusCode, 200, 'BatchGetItem should return status code 200')
        const responses = res.body.Responses[helpers.testHashTable]
        t.ok(responses.find(resItem => JSON.stringify(resItem) === JSON.stringify({ b: item.b, c: item.c })), 'Responses should contain projected item')
        t.ok(responses.find(resItem => JSON.stringify(resItem) === JSON.stringify({ b: item3.b })), 'Responses should contain projected item3')
        t.ok(responses.find(resItem => JSON.stringify(resItem) === JSON.stringify({})), 'Responses should contain empty object for item4')
        t.equal(responses.length, 3, 'Responses should have length 3')
        t.deepEqual(res.body.UnprocessedKeys, {}, 'should return empty UnprocessedKeys')
        cb()
      })
    }, function (err) {
      t.error(err, 'async.forEach should not return error')
      t.end()
    })
  })
})

test('batchGetItem - functionality - should return ConsumedCapacity from each specified table with no consistent read and small item', function (t) {
  const a = helpers.randomString()
  const b = new Array(4082 - a.length).join('b')
  const item = { a: { S: a }, b: { S: b }, c: { N: '12.3456' }, d: { B: 'AQI=' }, e: { BS: [ 'AQI=', 'Ag==', 'AQ==' ] } }
  const item2 = { a: { S: helpers.randomString() } }
  let batchReq = { RequestItems: {} }
  batchReq.RequestItems[helpers.testHashTable] = [ { PutRequest: { Item: item } }, { PutRequest: { Item: item2 } } ]
  request(helpers.opts('BatchWriteItem', batchReq), function (err, res) {
    t.error(err, 'BatchWriteItem request should not return error')
    t.equal(res.statusCode, 200, 'BatchWriteItem should return status code 200')
    batchReq = { RequestItems: {}, ReturnConsumedCapacity: 'TOTAL' }
    batchReq.RequestItems[helpers.testHashTable] = { Keys: [ { a: item.a }, { a: item2.a }, { a: { S: helpers.randomString() } } ] }
    batchReq.RequestItems[helpers.testHashNTable] = { Keys: [ { a: { N: helpers.randomNumber() } } ] }
    request(opts(batchReq), function (err, res) {
      t.error(err, 'BatchGetItem request (TOTAL) should not return error')
      t.equal(res.statusCode, 200, 'BatchGetItem (TOTAL) should return status code 200')
      t.ok(res.body.ConsumedCapacity.find(cc => cc.TableName === helpers.testHashTable && cc.CapacityUnits === 1.5), 'ConsumedCapacity should contain 1.5 for testHashTable')
      t.ok(res.body.ConsumedCapacity.find(cc => cc.TableName === helpers.testHashNTable && cc.CapacityUnits === 0.5), 'ConsumedCapacity should contain 0.5 for testHashNTable')
      t.equal(res.body.Responses[helpers.testHashTable].length, 2, 'Responses for testHashTable should have length 2')
      t.equal(res.body.Responses[helpers.testHashNTable].length, 0, 'Responses for testHashNTable should have length 0')
      batchReq.ReturnConsumedCapacity = 'INDEXES'
      request(opts(batchReq), function (err, res) {
        t.error(err, 'BatchGetItem request (INDEXES) should not return error')
        t.equal(res.statusCode, 200, 'BatchGetItem (INDEXES) should return status code 200')
        t.ok(res.body.ConsumedCapacity.find(cc => cc.TableName === helpers.testHashTable && cc.CapacityUnits === 1.5 && cc.Table.CapacityUnits === 1.5), 'ConsumedCapacity (INDEXES) should contain 1.5 for testHashTable')
        t.ok(res.body.ConsumedCapacity.find(cc => cc.TableName === helpers.testHashNTable && cc.CapacityUnits === 0.5 && cc.Table.CapacityUnits === 0.5), 'ConsumedCapacity (INDEXES) should contain 0.5 for testHashNTable')
        t.end()
      })
    })
  })
})

test('batchGetItem - functionality - should return ConsumedCapacity from each specified table with no consistent read and larger item', function (t) {
  const a = helpers.randomString()
  const b = new Array(4084 - a.length).join('b')
  const item = { a: { S: a }, b: { S: b }, c: { N: '12.3456' }, d: { B: 'AQI=' }, e: { BS: [ 'AQI=', 'Ag==' ] } }
  const item2 = { a: { S: helpers.randomString() } }
  let batchReq = { RequestItems: {} }
  batchReq.RequestItems[helpers.testHashTable] = [ { PutRequest: { Item: item } }, { PutRequest: { Item: item2 } } ]
  request(helpers.opts('BatchWriteItem', batchReq), function (err, res) {
    t.error(err, 'BatchWriteItem request should not return error')
    t.equal(res.statusCode, 200, 'BatchWriteItem should return status code 200')
    batchReq = { RequestItems: {}, ReturnConsumedCapacity: 'TOTAL' }
    batchReq.RequestItems[helpers.testHashTable] = { Keys: [ { a: item.a }, { a: item2.a }, { a: { S: helpers.randomString() } } ] }
    batchReq.RequestItems[helpers.testHashNTable] = { Keys: [ { a: { N: helpers.randomNumber() } } ] }
    request(opts(batchReq), function (err, res) {
      t.error(err, 'BatchGetItem request (TOTAL) should not return error')
      t.equal(res.statusCode, 200, 'BatchGetItem (TOTAL) should return status code 200')
      t.ok(res.body.ConsumedCapacity.find(cc => cc.TableName === helpers.testHashTable && cc.CapacityUnits === 2), 'ConsumedCapacity should contain 2 for testHashTable')
      t.ok(res.body.ConsumedCapacity.find(cc => cc.TableName === helpers.testHashNTable && cc.CapacityUnits === 0.5), 'ConsumedCapacity should contain 0.5 for testHashNTable')
      t.equal(res.body.Responses[helpers.testHashTable].length, 2, 'Responses for testHashTable should have length 2')
      t.equal(res.body.Responses[helpers.testHashNTable].length, 0, 'Responses for testHashNTable should have length 0')
      batchReq.ReturnConsumedCapacity = 'INDEXES'
      request(opts(batchReq), function (err, res) {
        t.error(err, 'BatchGetItem request (INDEXES) should not return error')
        t.equal(res.statusCode, 200, 'BatchGetItem (INDEXES) should return status code 200')
        t.ok(res.body.ConsumedCapacity.find(cc => cc.TableName === helpers.testHashTable && cc.CapacityUnits === 2 && cc.Table.CapacityUnits === 2), 'ConsumedCapacity (INDEXES) should contain 2 for testHashTable')
        t.ok(res.body.ConsumedCapacity.find(cc => cc.TableName === helpers.testHashNTable && cc.CapacityUnits === 0.5 && cc.Table.CapacityUnits === 0.5), 'ConsumedCapacity (INDEXES) should contain 0.5 for testHashNTable')
        t.end()
      })
    })
  })
})

test('batchGetItem - functionality - should return ConsumedCapacity from each specified table with consistent read and small item', function (t) {
  const a = helpers.randomString()
  const b = new Array(4082 - a.length).join('b')
  const item = { a: { S: a }, b: { S: b }, c: { N: '12.3456' }, d: { B: 'AQI=' }, e: { BS: [ 'AQI=', 'Ag==', 'AQ==' ] } }
  const item2 = { a: { S: helpers.randomString() } }
  let batchReq = { RequestItems: {} }
  batchReq.RequestItems[helpers.testHashTable] = [ { PutRequest: { Item: item } }, { PutRequest: { Item: item2 } } ]
  request(helpers.opts('BatchWriteItem', batchReq), function (err, res) {
    t.error(err, 'BatchWriteItem request should not return error')
    t.equal(res.statusCode, 200, 'BatchWriteItem should return status code 200')
    batchReq = { RequestItems: {}, ReturnConsumedCapacity: 'TOTAL' }
    batchReq.RequestItems[helpers.testHashTable] = { Keys: [ { a: item.a }, { a: item2.a }, { a: { S: helpers.randomString() } } ], ConsistentRead: true }
    batchReq.RequestItems[helpers.testHashNTable] = { Keys: [ { a: { N: helpers.randomNumber() } } ], ConsistentRead: true }
    request(opts(batchReq), function (err, res) {
      t.error(err, 'BatchGetItem request (TOTAL) should not return error')
      t.equal(res.statusCode, 200, 'BatchGetItem (TOTAL) should return status code 200')
      t.ok(res.body.ConsumedCapacity.find(cc => cc.TableName === helpers.testHashTable && cc.CapacityUnits === 3), 'ConsumedCapacity should contain 3 for testHashTable')
      t.ok(res.body.ConsumedCapacity.find(cc => cc.TableName === helpers.testHashNTable && cc.CapacityUnits === 1), 'ConsumedCapacity should contain 1 for testHashNTable')
      t.equal(res.body.Responses[helpers.testHashTable].length, 2, 'Responses for testHashTable should have length 2')
      t.equal(res.body.Responses[helpers.testHashNTable].length, 0, 'Responses for testHashNTable should have length 0')
      batchReq.ReturnConsumedCapacity = 'INDEXES'
      request(opts(batchReq), function (err, res) {
        t.error(err, 'BatchGetItem request (INDEXES) should not return error')
        t.equal(res.statusCode, 200, 'BatchGetItem (INDEXES) should return status code 200')
        t.ok(res.body.ConsumedCapacity.find(cc => cc.TableName === helpers.testHashTable && cc.CapacityUnits === 3 && cc.Table.CapacityUnits === 3), 'ConsumedCapacity (INDEXES) should contain 3 for testHashTable')
        t.ok(res.body.ConsumedCapacity.find(cc => cc.TableName === helpers.testHashNTable && cc.CapacityUnits === 1 && cc.Table.CapacityUnits === 1), 'ConsumedCapacity (INDEXES) should contain 1 for testHashNTable')
        t.end()
      })
    })
  })
})

test('batchGetItem - functionality - should return ConsumedCapacity from each specified table with consistent read and larger item', function (t) {
  const a = helpers.randomString()
  const b = new Array(4084 - a.length).join('b')
  const item = { a: { S: a }, b: { S: b }, c: { N: '12.3456' }, d: { B: 'AQI=' }, e: { BS: [ 'AQI=', 'Ag==' ] } }
  const item2 = { a: { S: helpers.randomString() } }
  let batchReq = { RequestItems: {} }
  batchReq.RequestItems[helpers.testHashTable] = [ { PutRequest: { Item: item } }, { PutRequest: { Item: item2 } } ]
  request(helpers.opts('BatchWriteItem', batchReq), function (err, res) {
    t.error(err, 'BatchWriteItem request should not return error')
    t.equal(res.statusCode, 200, 'BatchWriteItem should return status code 200')
    batchReq = { RequestItems: {}, ReturnConsumedCapacity: 'TOTAL' }
    batchReq.RequestItems[helpers.testHashTable] = { Keys: [ { a: item.a }, { a: item2.a }, { a: { S: helpers.randomString() } } ], ConsistentRead: true }
    batchReq.RequestItems[helpers.testHashNTable] = { Keys: [ { a: { N: helpers.randomNumber() } } ], ConsistentRead: true }
    request(opts(batchReq), function (err, res) {
      t.error(err, 'BatchGetItem request (TOTAL) should not return error')
      t.equal(res.statusCode, 200, 'BatchGetItem (TOTAL) should return status code 200')
      t.ok(res.body.ConsumedCapacity.find(cc => cc.TableName === helpers.testHashTable && cc.CapacityUnits === 4), 'ConsumedCapacity should contain 4 for testHashTable')
      t.ok(res.body.ConsumedCapacity.find(cc => cc.TableName === helpers.testHashNTable && cc.CapacityUnits === 1), 'ConsumedCapacity should contain 1 for testHashNTable')
      t.equal(res.body.Responses[helpers.testHashTable].length, 2, 'Responses for testHashTable should have length 2')
      t.equal(res.body.Responses[helpers.testHashNTable].length, 0, 'Responses for testHashNTable should have length 0')
      batchReq.ReturnConsumedCapacity = 'INDEXES'
      request(opts(batchReq), function (err, res) {
        t.error(err, 'BatchGetItem request (INDEXES) should not return error')
        t.equal(res.statusCode, 200, 'BatchGetItem (INDEXES) should return status code 200')
        t.ok(res.body.ConsumedCapacity.find(cc => cc.TableName === helpers.testHashTable && cc.CapacityUnits === 4 && cc.Table.CapacityUnits === 4), 'ConsumedCapacity (INDEXES) should contain 4 for testHashTable')
        t.ok(res.body.ConsumedCapacity.find(cc => cc.TableName === helpers.testHashNTable && cc.CapacityUnits === 1 && cc.Table.CapacityUnits === 1), 'ConsumedCapacity (INDEXES) should contain 1 for testHashNTable')
        t.end()
      })
    })
  })
})

// High capacity (~100 or more) needed to run this quickly
if (runSlowTests) {
  test('batchGetItem - functionality - should return all items if just under limit', function (t) {
    // Timeout logic removed for Tape

    let i, item
    const items = []
    const b = new Array(helpers.MAX_SIZE - 6).join('b')
    const batchReq = { RequestItems: {}, ReturnConsumedCapacity: 'TOTAL' }
    for (i = 0; i < 4; i++) {
      if (i < 3) {
        item = { a: { S: ('0' + i).slice(-2) }, b: { S: b } }
      }
      else {
        item = { a: { S: ('0' + i).slice(-2) }, b: { S: b.slice(0, 229353) }, c: { N: '12.3456' }, d: { B: 'AQI=' },
          e: { SS: [ 'a', 'bc' ] }, f: { NS: [ '1.23', '12.3' ] }, g: { BS: [ 'AQI=', 'Ag==', 'AQ==' ] } }
      }
      items.push(item)
    }
    helpers.clearTable(helpers.testHashTable, 'a', function (err) {
      if (err) {
        t.error(err, 'clearTable should not return error')
        return t.end()
      }
      helpers.batchWriteUntilDone(helpers.testHashTable, { puts: items }, function (err) {
        if (err) {
          t.error(err, 'batchWriteUntilDone should not return error')
          return t.end()
        }
        batchReq.RequestItems[helpers.testHashTable] = { Keys: items.map(function (item) { return { a: item.a } }), ConsistentRead: true }
        request(opts(batchReq), function (err, res) {
          if (err) {
            t.error(err, 'BatchGetItem request should not return error')
            return t.end()
          }
          t.equal(res.statusCode, 200, 'BatchGetItem should return status code 200')
          t.deepEqual(res.body.ConsumedCapacity, [ { CapacityUnits: 357, TableName: helpers.testHashTable } ], 'ConsumedCapacity should be correct')
          t.deepEqual(res.body.UnprocessedKeys, {}, 'should return empty UnprocessedKeys')
          t.equal(res.body.Responses[helpers.testHashTable].length, 4, 'Responses should have length 4')
          helpers.clearTable(helpers.testHashTable, 'a', function (err) {
            t.error(err, 'final clearTable should not return error')
            t.end()
          })
        })
      })
    })
  })

  // TODO: test fails!
  test.skip('batchGetItem - functionality - should return an unprocessed item if just over limit', function (t) {
    // Timeout logic removed for Tape

    let i, item
    const items = []
    const b = new Array(helpers.MAX_SIZE - 6).join('b')
    const batchReq = { RequestItems: {}, ReturnConsumedCapacity: 'TOTAL' }
    for (i = 0; i < 4; i++) {
      if (i < 3) {
        item = { a: { S: ('0' + i).slice(-2) }, b: { S: b } }
      }
      else {
        item = { a: { S: ('0' + i).slice(-2) }, b: { S: b.slice(0, 229354) }, c: { N: '12.3456' }, d: { B: 'AQI=' },
          e: { SS: [ 'a', 'bc' ] }, f: { NS: [ '1.23', '12.3' ] }, g: { BS: [ 'AQI=', 'Ag==', 'AQ==' ] } }
      }
      items.push(item)
    }
    helpers.batchWriteUntilDone(helpers.testHashTable, { puts: items }, function (err) {
      if (err) {
        t.error(err, 'batchWriteUntilDone should not return error')
        return t.end()
      }
      batchReq.RequestItems[helpers.testHashTable] = { Keys: items.map(function (item) { return { a: item.a } }), ConsistentRead: true }
      request(opts(batchReq), function (err, res) {
        if (err) {
          t.error(err, 'BatchGetItem request should not return error')
          return t.end()
        }
        t.equal(res.statusCode, 200, 'BatchGetItem should return status code 200')
        t.equal(res.body.UnprocessedKeys[helpers.testHashTable].ConsistentRead, true, 'UnprocessedKeys ConsistentRead should be true')
        t.equal(res.body.UnprocessedKeys[helpers.testHashTable].Keys.length, 1, 'UnprocessedKeys should have length 1')
        t.equal(Object.keys(res.body.UnprocessedKeys[helpers.testHashTable].Keys[0]).length, 1, 'Unprocessed key should have 1 attribute')
        if (res.body.UnprocessedKeys[helpers.testHashTable].Keys[0].a.S == '03') {
          t.deepEqual(res.body.ConsumedCapacity, [ { CapacityUnits: 301, TableName: helpers.testHashTable } ], 'ConsumedCapacity should be 301 if key 03 is unprocessed')
        }
        else {
          const keyVal = parseInt(res.body.UnprocessedKeys[helpers.testHashTable].Keys[0].a.S, 10)
          t.ok(keyVal > -1, 'Unprocessed key should be >= 0')
          t.ok(keyVal < 4, 'Unprocessed key should be < 4')
          t.deepEqual(res.body.ConsumedCapacity, [ { CapacityUnits: 258, TableName: helpers.testHashTable } ], 'ConsumedCapacity should be 258 if key < 3 is unprocessed')
        }
        t.equal(res.body.Responses[helpers.testHashTable].length, 3, 'Responses should have length 3')
        helpers.clearTable(helpers.testHashTable, 'a', function (err) {
          t.error(err, 'final clearTable should not return error')
          t.end()
        })
      })
    })
  })

  test('batchGetItem - functionality - should return many unprocessed items if very over the limit', function (t) {
    // Timeout logic removed for Tape

    let i, item
    const items = []
    const b = new Array(helpers.MAX_SIZE - 3).join('b')
    const batchReq = { RequestItems: {}, ReturnConsumedCapacity: 'TOTAL' }
    for (i = 0; i < 20; i++) {
      if (i < 3) {
        item = { a: { S: ('0' + i).slice(-2) }, b: { S: b } }
      }
      else {
        item = { a: { S: ('0' + i).slice(-2) }, b: { S: b.slice(0, 20000) } }
      }
      items.push(item)
    }
    helpers.batchBulkPut(helpers.testHashTable, items, function (err) {
      if (err) {
        t.error(err, 'batchBulkPut should not return error')
        return t.end()
      }
      batchReq.RequestItems[helpers.testHashTable] = { Keys: items.map(function (item) { return { a: item.a } }), ConsistentRead: true }
      request(opts(batchReq), function (err, res) {
        if (err) {
          t.error(err, 'BatchGetItem request should not return error')
          return t.end()
        }
        t.equal(res.statusCode, 200, 'BatchGetItem should return status code 200')
        t.equal(res.body.UnprocessedKeys[helpers.testHashTable].ConsistentRead, true, 'UnprocessedKeys ConsistentRead should be true')
        t.ok(res.body.UnprocessedKeys[helpers.testHashTable].Keys.length > 0, 'UnprocessedKeys length should be > 0')
        t.ok(res.body.Responses[helpers.testHashTable].length > 0, 'Responses length should be > 0')

        let totalLength, totalCapacity

        totalLength = res.body.Responses[helpers.testHashTable].length +
          res.body.UnprocessedKeys[helpers.testHashTable].Keys.length
        t.equal(totalLength, 20, 'Total length (responses + unprocessed) should be 20')

        totalCapacity = res.body.ConsumedCapacity[0].CapacityUnits
        for (i = 0; i < res.body.UnprocessedKeys[helpers.testHashTable].Keys.length; i++)
          totalCapacity += parseInt(res.body.UnprocessedKeys[helpers.testHashTable].Keys[i].a.S, 10) < 3 ? 99 : 4
        t.equal(totalCapacity, 385, 'Total calculated capacity should be 385')

        helpers.clearTable(helpers.testHashTable, 'a', function (err) {
          t.error(err, 'final clearTable should not return error')
          t.end()
        })
      })
    })
  })
}
else {
  test.skip('batchGetItem - functionality - SKIPPING SLOW TESTS', function (t) {
    t.end()
  })
}
