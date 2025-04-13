var async = require('async'),
  helpers = require('./helpers')

var target = 'GetItem',
  request = helpers.request,
  randomName = helpers.randomName,
  opts = helpers.opts.bind(null, target),
  assertType = helpers.assertType.bind(null, target),
  assertValidation = helpers.assertValidation.bind(null, target),
  assertNotFound = helpers.assertNotFound.bind(null, target)

describe('getItem', function () {
  describe('functionality', function () {

    var hashItem = { a: { S: helpers.randomString() }, b: { S: 'a' }, g: { N: '23' } },
      rangeItem = { a: { S: helpers.randomString() }, b: { S: helpers.randomString() }, g: { N: '23' } }

    before(function (done) {
      var putItems = [
        { TableName: helpers.testHashTable, Item: hashItem },
        { TableName: helpers.testRangeTable, Item: rangeItem },
      ]
      async.forEach(putItems, function (putItem, cb) { request(helpers.opts('PutItem', putItem), cb) }, done)
    })

    it('should return empty response if key does not exist', function (done) {
      request(opts({ TableName: helpers.testHashTable, Key: { a: { S: helpers.randomString() } } }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({})
        done()
      })
    })

    it('should return ConsumedCapacity if specified', function (done) {
      var req = { TableName: helpers.testHashTable, Key: { a: { S: helpers.randomString() } }, ReturnConsumedCapacity: 'TOTAL' }
      request(opts(req), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({ ConsumedCapacity: { CapacityUnits: 0.5, TableName: helpers.testHashTable } })
        req.ReturnConsumedCapacity = 'INDEXES'
        request(opts(req), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ ConsumedCapacity: { CapacityUnits: 0.5, Table: { CapacityUnits: 0.5 }, TableName: helpers.testHashTable } })
          done()
        })
      })
    })

    it('should return full ConsumedCapacity if specified', function (done) {
      var req = { TableName: helpers.testHashTable, Key: { a: { S: helpers.randomString() } }, ReturnConsumedCapacity: 'TOTAL', ConsistentRead: true }
      request(opts(req), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({ ConsumedCapacity: { CapacityUnits: 1, TableName: helpers.testHashTable } })
        req.ReturnConsumedCapacity = 'INDEXES'
        request(opts(req), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ ConsumedCapacity: { CapacityUnits: 1, Table: { CapacityUnits: 1 }, TableName: helpers.testHashTable } })
          done()
        })
      })
    })

    it('should return object by hash key', function (done) {
      request(opts({ TableName: helpers.testHashTable, Key: { a: hashItem.a }, ConsistentRead: true }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({ Item: hashItem })
        done()
      })
    })

    it('should return object by range key', function (done) {
      request(opts({ TableName: helpers.testRangeTable, Key: { a: rangeItem.a, b: rangeItem.b }, ConsistentRead: true }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({ Item: rangeItem })
        done()
      })
    })

    it('should only return requested attributes', function (done) {
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
          res.statusCode.should.equal(200)
          res.body.should.eql({ Item: { b: hashItem.b, g: hashItem.g } })
          cb()
        })
      }, done)
    })

    it('should only return requested nested attributes', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { M: { a: { S: 'a' }, b: { S: 'b' }, c: { S: 'c' } } }, c: { L: [ { S: 'a' }, { S: 'b' }, { S: 'c' } ] } }
      request(helpers.opts('PutItem', { TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          { ProjectionExpression: 'b.c,c[2],b.b,c[1],c[0].a' },
          { ProjectionExpression: '#b.#c,#c[2],#b.#b,#c[1],#c[0][1]', ExpressionAttributeNames: { '#b': 'b', '#c': 'c' } },
        ], function (getOpts, cb) {
          getOpts.TableName = helpers.testHashTable
          getOpts.Key = { a: item.a }
          getOpts.ConsistentRead = true
          request(opts(getOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({ Item: { b: { M: { b: item.b.M.b, c: item.b.M.c } }, c: { L: [ item.c.L[1], item.c.L[2] ] } } })
            cb()
          })
        }, done)
      })
    })

    it('should return ConsumedCapacity for small item with no ConsistentRead', function (done) {
      var a = helpers.randomString(), b = new Array(4082 - a.length).join('b'),
        item = { a: { S: a }, b: { S: b }, c: { N: '12.3456' }, d: { B: 'AQI=' }, e: { BS: [ 'AQI=', 'Ag==', 'AQ==' ] } }
      request(helpers.opts('PutItem', { TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({ TableName: helpers.testHashTable, Key: { a: item.a }, ReturnConsumedCapacity: 'TOTAL' }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.ConsumedCapacity.should.eql({ CapacityUnits: 0.5, TableName: helpers.testHashTable })
          done()
        })
      })
    })

    it('should return ConsumedCapacity for larger item with no ConsistentRead', function (done) {
      var a = helpers.randomString(), b = new Array(4084 - a.length).join('b'),
        item = { a: { S: a }, b: { S: b }, c: { N: '12.3456' }, d: { B: 'AQI=' }, e: { BS: [ 'AQI=', 'Ag==' ] } }
      request(helpers.opts('PutItem', { TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({ TableName: helpers.testHashTable, Key: { a: item.a }, ReturnConsumedCapacity: 'TOTAL' }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.ConsumedCapacity.should.eql({ CapacityUnits: 1, TableName: helpers.testHashTable })
          done()
        })
      })
    })

    it('should return ConsumedCapacity for small item with ConsistentRead', function (done) {
      var batchReq = { RequestItems: {} }
      var items = [ {
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
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach(items, function (item, cb) {
          request(opts({ TableName: helpers.testHashTable, Key: { a: item.a }, ReturnConsumedCapacity: 'TOTAL', ConsistentRead: true }), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.ConsumedCapacity.should.eql({ CapacityUnits: 1, TableName: helpers.testHashTable })
            cb()
          })
        }, done)
      })
    })

    it('should return ConsumedCapacity for larger item with ConsistentRead', function (done) {
      var batchReq = { RequestItems: {} }
      var items = [ {
        a: { S: helpers.randomString() },
        bb: { S: new Array(4001).join('b') },
        ccc: { N: '12.3456' },
        dddd: { B: 'AQI=' },
        eeeee: { BS: [ 'AQI=', 'Ag==', 'AQ==' ] },
        ffffff: { NULL: true },
        ggggggg: { BOOL: false },
        hhhhhhhh: { L: [ { S: 'a' }, { S: 'aa' }, { S: 'bb' }, { S: 'ccc' } ] },
        iiiiiiiii: { M: { aa: { S: 'aa' }, bbb: { S: 'bbb' } } },
      }, {
        a: { S: helpers.randomString() },
        ab: { S: new Array(4028).join('b') },
        abc: { NULL: true },
        abcd: { BOOL: true },
        abcde: { L: [ { S: 'aa' }, { N: '12.3456' }, { B: 'AQI=' } ] },
        abcdef: { M: { aa: { S: 'aa' }, bbb: { N: '12.3456' }, cccc: { B: 'AQI=' } } },
      } ]
      batchReq.RequestItems[helpers.testHashTable] = items.map(function (item) { return { PutRequest: { Item: item } } })
      request(helpers.opts('BatchWriteItem', batchReq), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach(items, function (item, cb) {
          request(opts({ TableName: helpers.testHashTable, Key: { a: item.a }, ReturnConsumedCapacity: 'TOTAL', ConsistentRead: true }), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.ConsumedCapacity.should.eql({ CapacityUnits: 2, TableName: helpers.testHashTable })
            cb()
          })
        }, done)
      })
    })

  })
})