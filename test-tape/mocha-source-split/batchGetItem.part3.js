var async = require('async'),
  helpers = require('./helpers')

var target = 'BatchGetItem',
  request = helpers.request,
  randomName = helpers.randomName,
  opts = helpers.opts.bind(null, target),
  assertType = helpers.assertType.bind(null, target),
  assertValidation = helpers.assertValidation.bind(null, target),
  assertNotFound = helpers.assertNotFound.bind(null, target),
  runSlowTests = helpers.runSlowTests

describe('batchGetItem', function () {
  describe('functionality', function () {

    it('should return empty responses if keys do not exist', function (done) {
      var batchReq = { RequestItems: {} }
      batchReq.RequestItems[helpers.testHashTable] = { Keys: [ { a: { S: helpers.randomString() } } ] }
      batchReq.RequestItems[helpers.testRangeTable] = { Keys: [ { a: { S: helpers.randomString() }, b: { S: helpers.randomString() } } ] }
      request(opts(batchReq), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.Responses[helpers.testHashTable].should.eql([])
        res.body.Responses[helpers.testRangeTable].should.eql([])
        res.body.UnprocessedKeys.should.eql({})
        done()
      })
    })

    it('should return only items that do exist', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { N: helpers.randomNumber() } },
        item2 = { a: { S: helpers.randomString() }, b: item.b },
        item3 = { a: { S: helpers.randomString() }, b: { N: helpers.randomNumber() } },
        batchReq = { RequestItems: {} }
      batchReq.RequestItems[helpers.testHashTable] = [
        { PutRequest: { Item: item } },
        { PutRequest: { Item: item2 } },
        { PutRequest: { Item: item3 } },
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        batchReq = { RequestItems: {} }
        batchReq.RequestItems[helpers.testHashTable] = { Keys: [
          { a: item.a },
          { a: { S: helpers.randomString() } },
          { a: item3.a },
          { a: { S: helpers.randomString() } },
        ], ConsistentRead: true }
        request(opts(batchReq), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Responses[helpers.testHashTable].should.containEql(item)
          res.body.Responses[helpers.testHashTable].should.containEql(item3)
          res.body.Responses[helpers.testHashTable].should.have.length(2)
          res.body.UnprocessedKeys.should.eql({})
          done()
        })
      })
    })

    it('should return only requested attributes of items that do exist', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { N: helpers.randomNumber() }, c: { S: 'c' } },
        item2 = { a: { S: helpers.randomString() }, b: item.b },
        item3 = { a: { S: helpers.randomString() }, b: { N: helpers.randomNumber() } },
        item4 = { a: { S: helpers.randomString() } },
        batchReq = { RequestItems: {} }
      batchReq.RequestItems[helpers.testHashTable] = [
        { PutRequest: { Item: item } },
        { PutRequest: { Item: item2 } },
        { PutRequest: { Item: item3 } },
        { PutRequest: { Item: item4 } },
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
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
            res.statusCode.should.equal(200)
            res.body.Responses[helpers.testHashTable].should.containEql({ b: item.b, c: item.c })
            res.body.Responses[helpers.testHashTable].should.containEql({ b: item3.b })
            res.body.Responses[helpers.testHashTable].should.containEql({})
            res.body.Responses[helpers.testHashTable].should.have.length(3)
            res.body.UnprocessedKeys.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should return ConsumedCapacity from each specified table with no consistent read and small item', function (done) {
      var a = helpers.randomString(), b = new Array(4082 - a.length).join('b'),
        item = { a: { S: a }, b: { S: b }, c: { N: '12.3456' }, d: { B: 'AQI=' }, e: { BS: [ 'AQI=', 'Ag==', 'AQ==' ] } },
        item2 = { a: { S: helpers.randomString() } },
        batchReq = { RequestItems: {} }
      batchReq.RequestItems[helpers.testHashTable] = [ { PutRequest: { Item: item } }, { PutRequest: { Item: item2 } } ]
      request(helpers.opts('BatchWriteItem', batchReq), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        batchReq = { RequestItems: {}, ReturnConsumedCapacity: 'TOTAL' }
        batchReq.RequestItems[helpers.testHashTable] = { Keys: [ { a: item.a }, { a: item2.a }, { a: { S: helpers.randomString() } } ] }
        batchReq.RequestItems[helpers.testHashNTable] = { Keys: [ { a: { N: helpers.randomNumber() } } ] }
        request(opts(batchReq), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.ConsumedCapacity.should.containEql({ CapacityUnits: 1.5, TableName: helpers.testHashTable })
          res.body.ConsumedCapacity.should.containEql({ CapacityUnits: 0.5, TableName: helpers.testHashNTable })
          res.body.Responses[helpers.testHashTable].should.have.length(2)
          res.body.Responses[helpers.testHashNTable].should.have.length(0)
          batchReq.ReturnConsumedCapacity = 'INDEXES'
          request(opts(batchReq), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.ConsumedCapacity.should.containEql({ CapacityUnits: 1.5, Table: { CapacityUnits: 1.5 }, TableName: helpers.testHashTable })
            res.body.ConsumedCapacity.should.containEql({ CapacityUnits: 0.5, Table: { CapacityUnits: 0.5 }, TableName: helpers.testHashNTable })
            done()
          })
        })
      })
    })

    it('should return ConsumedCapacity from each specified table with no consistent read and larger item', function (done) {
      var a = helpers.randomString(), b = new Array(4084 - a.length).join('b'),
        item = { a: { S: a }, b: { S: b }, c: { N: '12.3456' }, d: { B: 'AQI=' }, e: { BS: [ 'AQI=', 'Ag==' ] } },
        item2 = { a: { S: helpers.randomString() } },
        batchReq = { RequestItems: {} }
      batchReq.RequestItems[helpers.testHashTable] = [ { PutRequest: { Item: item } }, { PutRequest: { Item: item2 } } ]
      request(helpers.opts('BatchWriteItem', batchReq), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        batchReq = { RequestItems: {}, ReturnConsumedCapacity: 'TOTAL' }
        batchReq.RequestItems[helpers.testHashTable] = { Keys: [ { a: item.a }, { a: item2.a }, { a: { S: helpers.randomString() } } ] }
        batchReq.RequestItems[helpers.testHashNTable] = { Keys: [ { a: { N: helpers.randomNumber() } } ] }
        request(opts(batchReq), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.ConsumedCapacity.should.containEql({ CapacityUnits: 2, TableName: helpers.testHashTable })
          res.body.ConsumedCapacity.should.containEql({ CapacityUnits: 0.5, TableName: helpers.testHashNTable })
          res.body.Responses[helpers.testHashTable].should.have.length(2)
          res.body.Responses[helpers.testHashNTable].should.have.length(0)
          batchReq.ReturnConsumedCapacity = 'INDEXES'
          request(opts(batchReq), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.ConsumedCapacity.should.containEql({ CapacityUnits: 2, Table: { CapacityUnits: 2 }, TableName: helpers.testHashTable })
            res.body.ConsumedCapacity.should.containEql({ CapacityUnits: 0.5, Table: { CapacityUnits: 0.5 }, TableName: helpers.testHashNTable })
            done()
          })
        })
      })
    })

    it('should return ConsumedCapacity from each specified table with consistent read and small item', function (done) {
      var a = helpers.randomString(), b = new Array(4082 - a.length).join('b'),
        item = { a: { S: a }, b: { S: b }, c: { N: '12.3456' }, d: { B: 'AQI=' }, e: { BS: [ 'AQI=', 'Ag==', 'AQ==' ] } },
        item2 = { a: { S: helpers.randomString() } },
        batchReq = { RequestItems: {} }
      batchReq.RequestItems[helpers.testHashTable] = [ { PutRequest: { Item: item } }, { PutRequest: { Item: item2 } } ]
      request(helpers.opts('BatchWriteItem', batchReq), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        batchReq = { RequestItems: {}, ReturnConsumedCapacity: 'TOTAL' }
        batchReq.RequestItems[helpers.testHashTable] = { Keys: [ { a: item.a }, { a: item2.a }, { a: { S: helpers.randomString() } } ], ConsistentRead: true }
        batchReq.RequestItems[helpers.testHashNTable] = { Keys: [ { a: { N: helpers.randomNumber() } } ], ConsistentRead: true }
        request(opts(batchReq), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.ConsumedCapacity.should.containEql({ CapacityUnits: 3, TableName: helpers.testHashTable })
          res.body.ConsumedCapacity.should.containEql({ CapacityUnits: 1, TableName: helpers.testHashNTable })
          res.body.Responses[helpers.testHashTable].should.have.length(2)
          res.body.Responses[helpers.testHashNTable].should.have.length(0)
          batchReq.ReturnConsumedCapacity = 'INDEXES'
          request(opts(batchReq), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.ConsumedCapacity.should.containEql({ CapacityUnits: 3, Table: { CapacityUnits: 3 }, TableName: helpers.testHashTable })
            res.body.ConsumedCapacity.should.containEql({ CapacityUnits: 1, Table: { CapacityUnits: 1 }, TableName: helpers.testHashNTable })
            done()
          })
        })
      })
    })

    it('should return ConsumedCapacity from each specified table with consistent read and larger item', function (done) {
      var a = helpers.randomString(), b = new Array(4084 - a.length).join('b'),
        item = { a: { S: a }, b: { S: b }, c: { N: '12.3456' }, d: { B: 'AQI=' }, e: { BS: [ 'AQI=', 'Ag==' ] } },
        item2 = { a: { S: helpers.randomString() } },
        batchReq = { RequestItems: {} }
      batchReq.RequestItems[helpers.testHashTable] = [ { PutRequest: { Item: item } }, { PutRequest: { Item: item2 } } ]
      request(helpers.opts('BatchWriteItem', batchReq), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        batchReq = { RequestItems: {}, ReturnConsumedCapacity: 'TOTAL' }
        batchReq.RequestItems[helpers.testHashTable] = { Keys: [ { a: item.a }, { a: item2.a }, { a: { S: helpers.randomString() } } ], ConsistentRead: true }
        batchReq.RequestItems[helpers.testHashNTable] = { Keys: [ { a: { N: helpers.randomNumber() } } ], ConsistentRead: true }
        request(opts(batchReq), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.ConsumedCapacity.should.containEql({ CapacityUnits: 4, TableName: helpers.testHashTable })
          res.body.ConsumedCapacity.should.containEql({ CapacityUnits: 1, TableName: helpers.testHashNTable })
          res.body.Responses[helpers.testHashTable].should.have.length(2)
          res.body.Responses[helpers.testHashNTable].should.have.length(0)
          batchReq.ReturnConsumedCapacity = 'INDEXES'
          request(opts(batchReq), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.ConsumedCapacity.should.containEql({ CapacityUnits: 4, Table: { CapacityUnits: 4 }, TableName: helpers.testHashTable })
            res.body.ConsumedCapacity.should.containEql({ CapacityUnits: 1, Table: { CapacityUnits: 1 }, TableName: helpers.testHashNTable })
            done()
          })
        })
      })
    })

    // High capacity (~100 or more) needed to run this quickly
    if (runSlowTests) {
      it('should return all items if just under limit', function (done) {
        this.timeout(200000)

        var i, item, items = [], b = new Array(helpers.MAX_SIZE - 6).join('b'),
          batchReq = { RequestItems: {}, ReturnConsumedCapacity: 'TOTAL' }
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
          if (err) return done(err)
          helpers.batchWriteUntilDone(helpers.testHashTable, { puts: items }, function (err) {
            if (err) return done(err)
            batchReq.RequestItems[helpers.testHashTable] = { Keys: items.map(function (item) { return { a: item.a } }), ConsistentRead: true }
            request(opts(batchReq), function (err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)
              res.body.ConsumedCapacity.should.eql([ { CapacityUnits: 357, TableName: helpers.testHashTable } ])
              res.body.UnprocessedKeys.should.eql({})
              res.body.Responses[helpers.testHashTable].should.have.length(4)
              helpers.clearTable(helpers.testHashTable, 'a', done)
            })
          })
        })
      })

      // TODO: test fails!
      it.skip('should return an unprocessed item if just over limit', function (done) {
        this.timeout(200000)

        var i, item, items = [], b = new Array(helpers.MAX_SIZE - 6).join('b'),
          batchReq = { RequestItems: {}, ReturnConsumedCapacity: 'TOTAL' }
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
          if (err) return done(err)
          batchReq.RequestItems[helpers.testHashTable] = { Keys: items.map(function (item) { return { a: item.a } }), ConsistentRead: true }
          request(opts(batchReq), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.UnprocessedKeys[helpers.testHashTable].ConsistentRead.should.equal(true)
            res.body.UnprocessedKeys[helpers.testHashTable].Keys.should.have.length(1)
            Object.keys(res.body.UnprocessedKeys[helpers.testHashTable].Keys[0]).should.have.length(1)
            if (res.body.UnprocessedKeys[helpers.testHashTable].Keys[0].a.S == '03') {
              res.body.ConsumedCapacity.should.eql([ { CapacityUnits: 301, TableName: helpers.testHashTable } ])
            }
            else {
              res.body.UnprocessedKeys[helpers.testHashTable].Keys[0].a.S.should.be.above(-1)
              res.body.UnprocessedKeys[helpers.testHashTable].Keys[0].a.S.should.be.below(4)
              res.body.ConsumedCapacity.should.eql([ { CapacityUnits: 258, TableName: helpers.testHashTable } ])
            }
            res.body.Responses[helpers.testHashTable].should.have.length(3)
            helpers.clearTable(helpers.testHashTable, 'a', done)
          })
        })
      })

      it('should return many unprocessed items if very over the limit', function (done) {
        this.timeout(200000)

        var i, item, items = [], b = new Array(helpers.MAX_SIZE - 3).join('b'),
          batchReq = { RequestItems: {}, ReturnConsumedCapacity: 'TOTAL' }
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
          if (err) return done(err)
          batchReq.RequestItems[helpers.testHashTable] = { Keys: items.map(function (item) { return { a: item.a } }), ConsistentRead: true }
          request(opts(batchReq), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.UnprocessedKeys[helpers.testHashTable].ConsistentRead.should.equal(true)
            res.body.UnprocessedKeys[helpers.testHashTable].Keys.length.should.be.above(0)
            res.body.Responses[helpers.testHashTable].length.should.be.above(0)

            var totalLength, totalCapacity

            totalLength = res.body.Responses[helpers.testHashTable].length +
              res.body.UnprocessedKeys[helpers.testHashTable].Keys.length
            totalLength.should.equal(20)

            totalCapacity = res.body.ConsumedCapacity[0].CapacityUnits
            for (i = 0; i < res.body.UnprocessedKeys[helpers.testHashTable].Keys.length; i++)
              totalCapacity += res.body.UnprocessedKeys[helpers.testHashTable].Keys[i].a.S < 3 ? 99 : 4
            totalCapacity.should.equal(385)

            helpers.clearTable(helpers.testHashTable, 'a', done)
          })
        })
      })
    }
  })
})