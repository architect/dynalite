var helpers = require('./helpers'),
  should = require('should'),
  async = require('async')

var target = 'Query',
  request = helpers.request,
  opts = helpers.opts.bind(null, target),
  assertType = helpers.assertType.bind(null, target),
  assertValidation = helpers.assertValidation.bind(null, target),
  assertNotFound = helpers.assertNotFound.bind(null, target),
  runSlowTests = helpers.runSlowTests

describe('query', function () {
  describe('functionality', function () {

    it('should query a hash table when empty', function (done) {
      async.forEach([ {
        KeyConditions: { a: { ComparisonOperator: 'EQ', AttributeValueList: [ { S: helpers.randomString() } ] } },
      }, {
        KeyConditionExpression: 'a = :a',
        ExpressionAttributeValues: { ':a': { S: helpers.randomString() } },
      } ], function (queryOpts, cb) {
        queryOpts.TableName = helpers.testHashTable
        queryOpts.ConsistentRead = false
        queryOpts.ReturnConsumedCapacity = 'NONE'
        queryOpts.ScanIndexForward = true
        queryOpts.Select = 'ALL_ATTRIBUTES'
        request(opts(queryOpts), function (err, res) {
          if (err) return cb(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ Count: 0, ScannedCount: 0, Items: [] })
          cb()
        })
      }, done)
    })

    it('should query a hash table with items', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { N: helpers.randomNumber() } },
        item2 = { a: { S: helpers.randomString() }, b: item.b },
        item3 = { a: { S: helpers.randomString() }, b: { N: helpers.randomNumber() } },
        items = [ item, item2, item3 ]
      helpers.batchBulkPut(helpers.testHashTable, items, function (err) {
        if (err) return done(err)
        async.forEach([ {
          QueryFilter: {},
          KeyConditions: { a: { ComparisonOperator: 'EQ', AttributeValueList: [ item2.a ] } },
        }, {
          KeyConditionExpression: 'a = :a',
          ExpressionAttributeValues: { ':a': item2.a },
        } ], function (queryOpts, cb) {
          queryOpts.TableName = helpers.testHashTable
          queryOpts.ConsistentRead = true
          request(opts(queryOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({ Count: 1, ScannedCount: 1, Items: [ item2 ] })
            cb()
          })
        }, done)
      })
    })

    it('should query a range table with EQ on just hash key', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: '1' } },
        item2 = { a: item.a, b: { S: '2' } },
        item3 = { a: item.a, b: { S: '3' } },
        items = [ item, item2, item3 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        async.forEach([ {
          KeyConditions: { a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] } },
        }, {
          KeyConditionExpression: 'a = :a',
          ExpressionAttributeValues: { ':a': item.a },
        } ], function (queryOpts, cb) {
          queryOpts.TableName = helpers.testRangeTable
          queryOpts.ConsistentRead = true
          request(opts(queryOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({ Count: 3, ScannedCount: 3, Items: [ item, item2, item3 ] })
            cb()
          })
        }, done)
      })
    })

    it('should query a range table with EQ', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: '1' } },
        item2 = { a: item.a, b: { S: '2' } },
        item3 = { a: item.a, b: { S: '3' } },
        items = [ item, item2, item3 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        async.forEach([ {
          KeyConditions: {
            a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] },
            b: { ComparisonOperator: 'EQ', AttributeValueList: [ item2.b ] },
          },
        }, {
          KeyConditionExpression: 'a = :a AND b = :b',
          ExpressionAttributeValues: { ':a': item.a, ':b': item2.b },
        } ], function (queryOpts, cb) {
          queryOpts.TableName = helpers.testRangeTable
          queryOpts.ConsistentRead = true
          request(opts(queryOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({ Count: 1, ScannedCount: 1, Items: [ item2 ] })
            cb()
          })
        }, done)
      })
    })

    it('should query a range table with LE', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: '1' } },
        item2 = { a: item.a, b: { S: '2' } },
        item3 = { a: item.a, b: { S: '3' } },
        items = [ item, item2, item3 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        async.forEach([ {
          KeyConditions: {
            a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] },
            b: { ComparisonOperator: 'LE', AttributeValueList: [ item2.b ] },
          },
        }, {
          KeyConditionExpression: 'a = :a AND b <= :b',
          ExpressionAttributeValues: { ':a': item.a, ':b': item2.b },
        } ], function (queryOpts, cb) {
          queryOpts.TableName = helpers.testRangeTable
          queryOpts.ConsistentRead = true
          request(opts(queryOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({ Count: 2, ScannedCount: 2, Items: [ item, item2 ] })
            cb()
          })
        }, done)
      })
    })

    it('should query a range table with LT', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: '1' } },
        item2 = { a: item.a, b: { S: '2' } },
        item3 = { a: item.a, b: { S: '3' } },
        items = [ item, item2, item3 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        async.forEach([ {
          KeyConditions: {
            a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] },
            b: { ComparisonOperator: 'LT', AttributeValueList: [ item2.b ] },
          },
        }, {
          KeyConditionExpression: 'a = :a AND b < :b',
          ExpressionAttributeValues: { ':a': item.a, ':b': item2.b },
        } ], function (queryOpts, cb) {
          queryOpts.TableName = helpers.testRangeTable
          queryOpts.ConsistentRead = true
          request(opts(queryOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({ Count: 1, ScannedCount: 1, Items: [ item ] })
            cb()
          })
        }, done)
      })
    })

    it('should query a range table with GE', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: '1' } },
        item2 = { a: item.a, b: { S: '2' } },
        item3 = { a: item.a, b: { S: '3' } },
        items = [ item, item2, item3 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        async.forEach([ {
          KeyConditions: {
            a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] },
            b: { ComparisonOperator: 'GE', AttributeValueList: [ item2.b ] },
          },
        }, {
          KeyConditionExpression: 'a = :a AND b >= :b',
          ExpressionAttributeValues: { ':a': item.a, ':b': item2.b },
        } ], function (queryOpts, cb) {
          queryOpts.TableName = helpers.testRangeTable
          queryOpts.ConsistentRead = true
          request(opts(queryOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({ Count: 2, ScannedCount: 2, Items: [ item2, item3 ] })
            cb()
          })
        }, done)
      })
    })

    it('should query a range table with GT', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: '1' } },
        item2 = { a: item.a, b: { S: '2' } },
        item3 = { a: item.a, b: { S: '3' } },
        items = [ item, item2, item3 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        async.forEach([ {
          KeyConditions: {
            a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] },
            b: { ComparisonOperator: 'GT', AttributeValueList: [ item2.b ] },
          },
        }, {
          KeyConditionExpression: 'a = :a AND b > :b',
          ExpressionAttributeValues: { ':a': item.a, ':b': item2.b },
        } ], function (queryOpts, cb) {
          queryOpts.TableName = helpers.testRangeTable
          queryOpts.ConsistentRead = true
          request(opts(queryOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({ Count: 1, ScannedCount: 1, Items: [ item3 ] })
            cb()
          })
        }, done)
      })
    })

    it('should query a range table with BEGINS_WITH', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: 'aaa' } },
        item2 = { a: item.a, b: { S: 'aab' } },
        item3 = { a: item.a, b: { S: 'abc' } },
        items = [ item, item2, item3 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        async.forEach([ {
          KeyConditions: {
            a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] },
            b: { ComparisonOperator: 'BEGINS_WITH', AttributeValueList: [ { S: 'aa' } ] },
          },
        }, {
          KeyConditionExpression: 'a = :a AND begins_with(b, :b)',
          ExpressionAttributeValues: { ':a': item.a, ':b': { S: 'aa' } },
        } ], function (queryOpts, cb) {
          queryOpts.TableName = helpers.testRangeTable
          queryOpts.ConsistentRead = true
          request(opts(queryOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({ Count: 2, ScannedCount: 2, Items: [ item, item2 ] })
            cb()
          })
        }, done)
      })
    })

    it('should query a range table with BETWEEN', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: 'aa' } },
        item2 = { a: item.a, b: { S: 'ab' } },
        item3 = { a: item.a, b: { S: 'abc' } },
        item4 = { a: item.a, b: { S: 'ac' } },
        item5 = { a: item.a, b: { S: 'aca' } },
        items = [ item, item2, item3, item4, item5 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        async.forEach([ {
          KeyConditions: {
            a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] },
            b: { ComparisonOperator: 'BETWEEN', AttributeValueList: [ { S: 'ab' }, { S: 'ac' } ] },
          },
        }, {
          KeyConditionExpression: 'a = :a AND b BETWEEN :b AND :c',
          ExpressionAttributeValues: { ':a': item.a, ':b': { S: 'ab' }, ':c': { S: 'ac' } },
        } ], function (queryOpts, cb) {
          queryOpts.TableName = helpers.testRangeTable
          queryOpts.ConsistentRead = true
          request(opts(queryOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({ Count: 3, ScannedCount: 3, Items: [ item2, item3, item4 ] })
            cb()
          })
        }, done)
      })
    })

    it('should only return requested attributes', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: 'b1' }, d: { S: 'd1' } },
        item2 = { a: item.a, b: { S: 'b2' } },
        item3 = { a: item.a, b: { S: 'b3' }, d: { S: 'd3' }, e: { S: 'e3' } },
        items = [ item, item2, item3 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        async.forEach([ {
          KeyConditions: {
            a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] },
          },
          AttributesToGet: [ 'b', 'd' ],
        }, {
          KeyConditions: {
            a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] },
          },
          ProjectionExpression: 'b, d',
        }, {
          KeyConditionExpression: 'a = :a',
          ExpressionAttributeValues: { ':a': item.a },
          ProjectionExpression: 'b, d',
        }, {
          KeyConditionExpression: 'a = :a',
          ExpressionAttributeValues: { ':a': item.a },
          ExpressionAttributeNames: { '#b': 'b', '#d': 'd' },
          ProjectionExpression: '#b, #d',
        } ], function (queryOpts, cb) {
          queryOpts.TableName = helpers.testRangeTable
          queryOpts.ConsistentRead = true
          request(opts(queryOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({ Count: 3, ScannedCount: 3, Items: [
              { b: { S: 'b1' }, d: { S: 'd1' } },
              { b: { S: 'b2' } },
              { b: { S: 'b3' }, d: { S: 'd3' } },
            ] })
            cb()
          })
        }, done)
      })
    })

    it('should only return requested nested attributes', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: 'b1' }, e: { M: { a: { S: 'b1' }, d: { S: 'b1' } } }, f: { L: [ { S: 'd1' }, { S: 'd2' }, { S: 'd3' } ] } },
        item2 = { a: item.a, b: { S: 'b2' } },
        item3 = { a: item.a, b: { S: 'b3' }, d: { S: 'd3' }, e: { S: 'e3' } },
        items = [ item, item2, item3 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        async.forEach([ {
          KeyConditions: {
            a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] },
          },
          ProjectionExpression: 'f[2], f[0], e.d, e.a, d',
        }, {
          KeyConditionExpression: 'a = :a',
          ExpressionAttributeValues: { ':a': item.a },
          ProjectionExpression: 'f[2], f[0], e.d, e.a, d',
        }, {
          KeyConditionExpression: 'a = :a',
          ExpressionAttributeValues: { ':a': item.a },
          ExpressionAttributeNames: { '#f': 'f', '#e': 'e', '#a': 'a' },
          ProjectionExpression: '#f[2],#f[0],#e.d,e.#a,d',
        } ], function (queryOpts, cb) {
          queryOpts.TableName = helpers.testRangeTable
          queryOpts.ConsistentRead = true
          request(opts(queryOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({ Count: 3, ScannedCount: 3, Items: [
              { e: { M: { a: { S: 'b1' }, d: { S: 'b1' } } }, f: { L: [ { S: 'd1' }, { S: 'd3' } ] } },
              {},
              { d: { S: 'd3' } },
            ] })
            cb()
          })
        }, done)
      })
    })

    it('should filter items by query filter', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: 'b1' }, d: { S: '1' } },
        item2 = { a: item.a, b: { S: 'b2' } },
        item3 = { a: item.a, b: { S: 'b3' }, d: { S: 'd3' }, e: { S: 'e3' } },
        items = [ item, item2, item3 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        async.forEach([ {
          KeyConditions: {
            a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] },
          },
          QueryFilter: {
            e: { ComparisonOperator: 'NOT_NULL' },
          },
        }, {
          KeyConditionExpression: 'a = :a',
          ExpressionAttributeValues: { ':a': item.a },
          FilterExpression: 'attribute_exists(e)',
        } ], function (queryOpts, cb) {
          queryOpts.TableName = helpers.testRangeTable
          queryOpts.ConsistentRead = true
          request(opts(queryOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({ Count: 1, ScannedCount: 3, Items: [
              { a: item.a, b: { S: 'b3' }, d: { S: 'd3' }, e: { S: 'e3' } },
            ] })
            cb()
          })
        }, done)
      })
    })

    it('should only return projected attributes by default for secondary indexes', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: 'b1' }, c: { S: 'c1' }, d: { S: 'd1' } },
        item2 = { a: item.a, b: { S: 'b2' } },
        item3 = { a: item.a, b: { S: 'b3' }, d: { S: 'd3' }, e: { S: 'e3' }, f: { S: 'f3' } },
        item4 = { a: item.a, b: { S: 'b4' }, c: { S: 'c4' }, d: { S: 'd4' }, e: { S: 'e4' } },
        items = [ item, item2, item3, item4 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        var req = { TableName: helpers.testRangeTable, ConsistentRead: true, IndexName: 'index2',
          KeyConditions: { a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] } },
          ReturnConsumedCapacity: 'TOTAL' }
        request(opts(req), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          delete item3.e
          delete item3.f
          delete item4.e
          res.body.should.eql({
            Count: 3,
            ScannedCount: 3,
            Items: [ item, item3, item4 ],
            ConsumedCapacity: { CapacityUnits: 1, TableName: helpers.testRangeTable },
          })
          req.ReturnConsumedCapacity = 'INDEXES'
          request(opts(req), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({
              Count: 3,
              ScannedCount: 3,
              Items: [ item, item3, item4 ],
              ConsumedCapacity: {
                CapacityUnits: 1,
                TableName: helpers.testRangeTable,
                Table: { CapacityUnits: 0 },
                LocalSecondaryIndexes: { index2: { CapacityUnits: 1 } },
              },
            })
            done()
          })
        })
      })
    })

    it('should return all attributes when specified for secondary indexes', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: 'b1' }, c: { S: 'c1' }, d: { S: 'd1' } },
        item2 = { a: item.a, b: { S: 'b2' } },
        item3 = { a: item.a, b: { S: 'b3' }, d: { S: 'd3' }, e: { M: { e3: { S: new Array(4062).join('e') } } }, f: { L: [ { S: 'f3' }, { S: 'ff3' } ] } },
        item4 = { a: item.a, b: { S: 'b4' }, c: { S: 'c4' }, d: { S: 'd4' }, e: { M: { ee4: { S: 'e4' }, eee4: { S: new Array(4062).join('e') } } } },
        items = [ item, item2, item3, item4 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        var req = { TableName: helpers.testRangeTable, ConsistentRead: true, IndexName: 'index2',
          KeyConditions: { a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] } },
          Select: 'ALL_ATTRIBUTES', ReturnConsumedCapacity: 'TOTAL' }
        request(opts(req), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Count: 3,
            ScannedCount: 3,
            Items: [ item, item3, item4 ],
            ConsumedCapacity: { CapacityUnits: 4, TableName: helpers.testRangeTable },
          })
          req.ReturnConsumedCapacity = 'INDEXES'
          request(opts(req), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({
              Count: 3,
              ScannedCount: 3,
              Items: [ item, item3, item4 ],
              ConsumedCapacity: {
                CapacityUnits: 4,
                TableName: helpers.testRangeTable,
                Table: { CapacityUnits: 3 },
                LocalSecondaryIndexes: { index2: { CapacityUnits: 1 } },
              },
            })
            done()
          })
        })
      })
    })

    it('should return COUNT if requested', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: '2' } },
        item2 = { a: item.a, b: { S: '1' } },
        item3 = { a: item.a, b: { S: '3' } },
        item4 = { a: item.a, b: { S: '4' } },
        item5 = { a: item.a, b: { S: '5' } },
        items = [ item, item2, item3, item4, item5 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        request(opts({ TableName: helpers.testRangeTable, ConsistentRead: true, KeyConditions: {
          a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] },
          b: { ComparisonOperator: 'GE', AttributeValueList: [ item.b ] },
        }, Select: 'COUNT' }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          should.not.exist(res.body.Items)
          res.body.should.eql({ Count: 4, ScannedCount: 4 })
          done()
        })
      })
    })

    it('should only return Limit items if requested', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: '2' }, c: { S: 'c' } },
        item2 = { a: item.a, b: { S: '1' }, c: { S: 'c' } },
        item3 = { a: item.a, b: { S: '3' }, c: { S: 'c' } },
        item4 = { a: item.a, b: { S: '4' }, c: { S: 'c' } },
        item5 = { a: item.a, b: { S: '5' }, c: { S: 'c' } },
        items = [ item, item2, item3, item4, item5 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        request(opts({ TableName: helpers.testRangeTable, ConsistentRead: true, KeyConditions: {
          a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] },
          b: { ComparisonOperator: 'GE', AttributeValueList: [ item.b ] },
        }, Limit: 2 }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ Count: 2, ScannedCount: 2, Items: [ item, item3 ], LastEvaluatedKey: { a: item3.a, b: item3.b } })
          done()
        })
      })
    })

    it('should only return Limit items if requested and QueryFilter', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: '2' }, c: { S: 'c' } },
        item2 = { a: item.a, b: { S: '1' }, c: { S: 'c' } },
        item3 = { a: item.a, b: { S: '3' }, c: { S: 'c' }, d: { S: 'd' } },
        item4 = { a: item.a, b: { S: '4' }, c: { S: 'c' } },
        item5 = { a: item.a, b: { S: '5' }, c: { S: 'c' } },
        items = [ item, item2, item3, item4, item5 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        async.forEach([ {
          KeyConditions: {
            a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] },
            b: { ComparisonOperator: 'GE', AttributeValueList: [ item.b ] },
          },
          QueryFilter: {
            d: { ComparisonOperator: 'EQ', AttributeValueList: [ item3.d ] },
          },
        }, {
          KeyConditionExpression: 'a = :a AND b >= :b',
          ExpressionAttributeValues: { ':a': item.a, ':b': item.b, ':d': item3.d },
          FilterExpression: 'd = :d',
        } ], function (queryOpts, cb) {
          queryOpts.TableName = helpers.testRangeTable
          queryOpts.ConsistentRead = true
          queryOpts.Limit = 2
          request(opts(queryOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({ Count: 1, ScannedCount: 2, Items: [ item3 ], LastEvaluatedKey: { a: item3.a, b: item3.b } })
            cb()
          })
        }, done)
      })
    })

    it('should return LastEvaluatedKey even if only Count is selected', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: '2' }, c: { S: 'c' } },
        item2 = { a: item.a, b: { S: '1' }, c: { S: 'c' } },
        item3 = { a: item.a, b: { S: '3' }, c: { S: 'c' } },
        item4 = { a: item.a, b: { S: '4' }, c: { S: 'c' } },
        item5 = { a: item.a, b: { S: '5' }, c: { S: 'c' } },
        items = [ item, item2, item3, item4, item5 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        request(opts({ TableName: helpers.testRangeTable, ConsistentRead: true, KeyConditions: {
          a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] },
          b: { ComparisonOperator: 'GE', AttributeValueList: [ item.b ] },
        }, Limit: 2, Select: 'COUNT' }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ Count: 2, ScannedCount: 2, LastEvaluatedKey: { a: item3.a, b: item3.b } })
          done()
        })
      })
    })

    it('should return LastEvaluatedKey even if only Count is selected and QueryFilter', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: '2' }, c: { S: 'c' } },
        item2 = { a: item.a, b: { S: '1' }, c: { S: 'c' } },
        item3 = { a: item.a, b: { S: '3' }, c: { S: 'c' }, d: { S: 'd' } },
        item4 = { a: item.a, b: { S: '4' }, c: { S: 'c' } },
        item5 = { a: item.a, b: { S: '5' }, c: { S: 'c' } },
        items = [ item, item2, item3, item4, item5 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        request(opts({ TableName: helpers.testRangeTable, ConsistentRead: true, KeyConditions: {
          a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] },
          b: { ComparisonOperator: 'GE', AttributeValueList: [ item.b ] },
        }, QueryFilter: {
          d: { ComparisonOperator: 'EQ', AttributeValueList: [ item3.d ] },
        }, Limit: 2, Select: 'COUNT' }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ Count: 1, ScannedCount: 2, LastEvaluatedKey: { a: item3.a, b: item3.b } })
          done()
        })
      })
    })

    it('should not return LastEvaluatedKey if Limit is at least size of response', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: '1' }, c: { S: 'c' } },
        item2 = { a: item.a, b: { S: '2' }, c: { S: 'c' } },
        item3 = { a: { S: helpers.randomString() }, b: { S: '1' }, c: { S: 'c' } },
        item4 = { a: item3.a, b: { S: '2' }, c: { S: 'c' } }

      helpers.replaceTable(helpers.testRangeTable, [ 'a', 'b' ], [ item, item2, item3, item4 ], function (err) {
        if (err) return done(err)

        request(helpers.opts('Scan', { TableName: helpers.testRangeTable }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          var lastHashItem = res.body.Items[res.body.Items.length - 1],
            lastHashItems = res.body.Items.filter(function (item) { return item.a.S == lastHashItem.a.S }),
            otherHashItem = lastHashItem.a.S == item.a.S ? item3 : item,
            otherHashItems = res.body.Items.filter(function (item) { return item.a.S == otherHashItem.a.S })
          otherHashItems.length.should.equal(2)
          request(opts({ TableName: helpers.testRangeTable, ConsistentRead: true, KeyConditions: {
            a: { ComparisonOperator: 'EQ', AttributeValueList: [ lastHashItem.a ] },
          } }), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({ Count: lastHashItems.length, ScannedCount: lastHashItems.length, Items: lastHashItems })
            request(opts({ TableName: helpers.testRangeTable, ConsistentRead: true, KeyConditions: {
              a: { ComparisonOperator: 'EQ', AttributeValueList: [ lastHashItem.a ] },
            }, Limit: lastHashItems.length }), function (err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)
              res.body.should.eql({ Count: lastHashItems.length, ScannedCount: lastHashItems.length, Items: lastHashItems, LastEvaluatedKey: { a: lastHashItem.a, b: lastHashItem.b } })
              request(opts({ TableName: helpers.testRangeTable, ConsistentRead: true, KeyConditions: {
                a: { ComparisonOperator: 'EQ', AttributeValueList: [ otherHashItem.a ] },
              }, Limit: 2 }), function (err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)

                // TODO: Technically there shouldn't be a LastEvaluatedKey here,
                //       but the logic is very complicated, so for now, just leave it
                // res.body.should.eql({Count: 2, Items: otherHashItems})

                res.body.Count.should.equal(2)
                res.body.ScannedCount.should.equal(2)
                res.body.Items.should.eql(otherHashItems)
                done()
              })
            })
          })
        })
      })
    })

    it('should return items in order for strings', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: '1' } },
        item2 = { a: item.a, b: { S: '2' } },
        item3 = { a: item.a, b: { S: '10' } },
        item4 = { a: item.a, b: { S: 'a' } },
        item5 = { a: item.a, b: { S: 'b' } },
        item6 = { a: item.a, b: { S: 'aa' } },
        item7 = { a: item.a, b: { S: 'ab' } },
        item8 = { a: item.a, b: { S: 'A' } },
        item9 = { a: item.a, b: { S: 'B' } },
        items = [ item, item2, item3, item4, item5, item6, item7, item8, item9 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        request(opts({ TableName: helpers.testRangeTable, ConsistentRead: true, KeyConditions: {
          a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] },
        } }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ Count: 9, ScannedCount: 9, Items: [ item, item3, item2, item8, item9, item4, item6, item7, item5 ] })
          done()
        })
      })
    })

    it('should return items in order for secondary index strings', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: '1' }, c: { S: '1' }, d: { S: '1' } },
        item2 = { a: item.a, b: { S: '2' }, c: { S: '2' } },
        item3 = { a: item.a, b: { S: '3' }, c: { S: '10' } },
        item4 = { a: item.a, b: { S: '4' }, c: { S: 'a' } },
        item5 = { a: item.a, b: { S: '5' }, c: { S: 'b' } },
        item6 = { a: item.a, b: { S: '6' }, c: { S: 'aa' }, e: { S: '6' } },
        item7 = { a: item.a, b: { S: '7' }, c: { S: 'ab' } },
        item8 = { a: item.a, b: { S: '8' }, c: { S: 'A' } },
        item9 = { a: item.a, b: { S: '9' }, c: { S: 'B' } },
        items = [ item, item2, item3, item4, item5, item6, item7, item8, item9 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        var req = { TableName: helpers.testRangeTable, IndexName: 'index1',
          KeyConditions: { a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] } }, ReturnConsumedCapacity: 'TOTAL' }
        request(opts(req), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Count: 9,
            ScannedCount: 9,
            Items: [ item, item3, item2, item8, item9, item4, item6, item7, item5 ],
            ConsumedCapacity: {
              CapacityUnits: 0.5,
              TableName: helpers.testRangeTable,
            },
          })
          req.ReturnConsumedCapacity = 'INDEXES'
          request(opts(req), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({
              Count: 9,
              ScannedCount: 9,
              Items: [ item, item3, item2, item8, item9, item4, item6, item7, item5 ],
              ConsumedCapacity: {
                CapacityUnits: 0.5,
                TableName: helpers.testRangeTable,
                Table: { CapacityUnits: 0 },
                LocalSecondaryIndexes: { index1: { CapacityUnits: 0.5 } },
              },
            })
            done()
          })
        })
      })
    })

    it('should calculate comparisons correctly for secondary indexes', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: '1' }, c: { S: '1' }, d: { S: '1' } },
        item2 = { a: item.a, b: { S: '2' }, c: { S: '2' } },
        item3 = { a: item.a, b: { S: '3' }, c: { S: '10' } },
        item4 = { a: item.a, b: { S: '4' }, c: { S: 'a' } },
        item5 = { a: item.a, b: { S: '5' }, c: { S: 'b' } },
        item6 = { a: item.a, b: { S: '6' }, c: { S: 'aa' }, e: { S: '6' } },
        item7 = { a: item.a, b: { S: '7' }, c: { S: 'ab' } },
        item8 = { a: item.a, b: { S: '8' }, c: { S: 'A' } },
        item9 = { a: item.a, b: { S: '9' }, c: { S: 'B' } },
        items = [ item, item2, item3, item4, item5, item6, item7, item8, item9 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        var req = {
          TableName: helpers.testRangeTable,
          IndexName: 'index1',
          KeyConditionExpression: 'a = :a AND c <= :c',
          ExpressionAttributeValues: { ':a': item.a, ':c': item4.c },
        }
        request(opts(req), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Count: 6,
            ScannedCount: 6,
            Items: [ item, item3, item2, item8, item9, item4 ],
          })
          req.KeyConditionExpression = 'a = :a AND c = :c'
          request(opts(req), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({
              Count: 1,
              ScannedCount: 1,
              Items: [ item4 ],
            })
            req.KeyConditionExpression = 'a = :a AND c >= :c'
            request(opts(req), function (err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)
              res.body.should.eql({
                Count: 4,
                ScannedCount: 4,
                Items: [ item4, item6, item7, item5 ],
              })
              req.KeyConditionExpression = 'a = :a AND c > :c'
              request(opts(req), function (err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)
                res.body.should.eql({
                  Count: 3,
                  ScannedCount: 3,
                  Items: [ item6, item7, item5 ],
                })
                req.KeyConditionExpression = 'a = :a AND c < :c'
                request(opts(req), function (err, res) {
                  if (err) return done(err)
                  res.statusCode.should.equal(200)
                  res.body.should.eql({
                    Count: 5,
                    ScannedCount: 5,
                    Items: [ item, item3, item2, item8, item9 ],
                  })
                  req.KeyConditionExpression = 'a = :a AND c BETWEEN :c AND :d'
                  req.ExpressionAttributeValues[':d'] = item7.c
                  request(opts(req), function (err, res) {
                    if (err) return done(err)
                    res.statusCode.should.equal(200)
                    res.body.should.eql({
                      Count: 3,
                      ScannedCount: 3,
                      Items: [ item4, item6, item7 ],
                    })
                    done()
                  })
                })
              })
            })
          })
        })
      })
    })

    it('should return items in order for numbers', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { N: '0' } },
        item2 = { a: item.a, b: { N: '99.1' } },
        item3 = { a: item.a, b: { N: '10.9' } },
        item4 = { a: item.a, b: { N: '10.1' } },
        item5 = { a: item.a, b: { N: '9.1' } },
        item6 = { a: item.a, b: { N: '9' } },
        item7 = { a: item.a, b: { N: '1.9' } },
        item8 = { a: item.a, b: { N: '1.1' } },
        item9 = { a: item.a, b: { N: '1' } },
        item10 = { a: item.a, b: { N: '0.9' } },
        item11 = { a: item.a, b: { N: '0.1' } },
        item12 = { a: item.a, b: { N: '0.09' } },
        item13 = { a: item.a, b: { N: '0.01' } },
        item14 = { a: item.a, b: { N: '-0.01' } },
        item15 = { a: item.a, b: { N: '-0.09' } },
        item16 = { a: item.a, b: { N: '-0.1' } },
        item17 = { a: item.a, b: { N: '-0.9' } },
        item18 = { a: item.a, b: { N: '-1' } },
        item19 = { a: item.a, b: { N: '-1.01' } },
        item20 = { a: item.a, b: { N: '-9' } },
        item21 = { a: item.a, b: { N: '-9.9' } },
        item22 = { a: item.a, b: { N: '-10.1' } },
        item23 = { a: item.a, b: { N: '-99.1' } },
        items = [ item, item2, item3, item4, item5, item6, item7, item8, item9, item10, item11, item12,
          item13, item14, item15, item16, item17, item18, item19, item20, item21, item22, item23 ]
      helpers.batchBulkPut(helpers.testRangeNTable, items, function (err) {
        if (err) return done(err)
        request(opts({ TableName: helpers.testRangeNTable, ConsistentRead: true, KeyConditions: {
          a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] },
        } }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ Count: 23, ScannedCount: 23, Items: [ item23, item22, item21, item20, item19, item18, item17, item16, item15,
            item14, item, item13, item12, item11, item10, item9, item8, item7, item6, item5, item4, item3, item2 ] })
          done()
        })
      })
    })

    it('should return items in order for binary', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { B: '1Py5xA==' } },
        item2 = { a: item.a, b: { B: 'JA==' } },
        item3 = { a: item.a, b: { B: '2w==' } },
        item4 = { a: item.a, b: { B: 'cAeRhZE=' } },
        item5 = { a: item.a, b: { B: '6piVtA==' } },
        item6 = { a: item.a, b: { B: 'MjA0' } },
        item7 = { a: item.a, b: { B: '1g==' } },
        item8 = { a: item.a, b: { B: 'ER/jLQ==' } },
        item9 = { a: item.a, b: { B: 'T7MzEUw=' } },
        item10 = { a: item.a, b: { B: '9FkiOH0=' } },
        item11 = { a: item.a, b: { B: 'Iv/a' } },
        item12 = { a: item.a, b: { B: '9V0=' } },
        items = [ item, item2, item3, item4, item5, item6, item7, item8, item9, item10, item11, item12 ]
      helpers.batchBulkPut(helpers.testRangeBTable, items, function (err) {
        if (err) return done(err)
        request(opts({ TableName: helpers.testRangeBTable, ConsistentRead: true, KeyConditions: {
          a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] },
        } }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ Count: 12, ScannedCount: 12, Items: [ item8, item11, item2, item6, item9, item4,
            item, item7, item3, item5, item10, item12 ] })
          done()
        })
      })
    })

    it('should return items in reverse order for strings', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: '1' } },
        item2 = { a: item.a, b: { S: '2' } },
        item3 = { a: item.a, b: { S: '10' } },
        items = [ item, item2, item3 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        request(opts({ TableName: helpers.testRangeTable, ConsistentRead: true, KeyConditions: {
          a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] },
        }, ScanIndexForward: false }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ Count: 3, ScannedCount: 3, Items: [ item2, item3, item ] })
          done()
        })
      })
    })

    it('should return items in reverse order with Limit for strings', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: '1' } },
        item2 = { a: item.a, b: { S: '2' } },
        item3 = { a: item.a, b: { S: '10' } },
        items = [ item, item2, item3 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        request(opts({ TableName: helpers.testRangeTable, ConsistentRead: true, KeyConditions: {
          a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] },
        }, ScanIndexForward: false, Limit: 2 }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ Count: 2, ScannedCount: 2, Items: [ item2, item3 ], LastEvaluatedKey: item3 })
          done()
        })
      })
    })

    it('should return items in reverse order with ExclusiveStartKey for strings', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: '1' } },
        item2 = { a: item.a, b: { S: '2' } },
        item3 = { a: item.a, b: { S: '10' } },
        items = [ item, item2, item3 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        request(opts({ TableName: helpers.testRangeTable, ConsistentRead: true, KeyConditions: {
          a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] },
        }, ScanIndexForward: false, ExclusiveStartKey: item2 }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ Count: 2, ScannedCount: 2, Items: [ item3, item ] })
          done()
        })
      })
    })

    it('should return items in reverse order for numbers', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { N: '0' } },
        item2 = { a: item.a, b: { N: '99.1' } },
        item3 = { a: item.a, b: { N: '10.9' } },
        item4 = { a: item.a, b: { N: '9.1' } },
        item5 = { a: item.a, b: { N: '0.9' } },
        item6 = { a: item.a, b: { N: '-0.01' } },
        item7 = { a: item.a, b: { N: '-0.1' } },
        item8 = { a: item.a, b: { N: '-1' } },
        item9 = { a: item.a, b: { N: '-99.1' } },
        items = [ item, item2, item3, item4, item5, item6, item7, item8, item9 ]
      helpers.batchBulkPut(helpers.testRangeNTable, items, function (err) {
        if (err) return done(err)
        request(opts({ TableName: helpers.testRangeNTable, ConsistentRead: true, KeyConditions: {
          a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] },
        }, ScanIndexForward: false }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ Count: 9, ScannedCount: 9, Items: [ item2, item3, item4, item5, item, item6, item7, item8, item9 ] })
          done()
        })
      })
    })

    it('should return items in reverse order with Limit for numbers', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { N: '0' } },
        item2 = { a: item.a, b: { N: '99.1' }, c: { S: 'c' } },
        item3 = { a: item.a, b: { N: '10.9' }, c: { S: 'c' } },
        item4 = { a: item.a, b: { N: '9.1' }, c: { S: 'c' } },
        item5 = { a: item.a, b: { N: '0.9' }, c: { S: 'c' } },
        items = [ item, item2, item3, item4, item5 ]
      helpers.batchBulkPut(helpers.testRangeNTable, items, function (err) {
        if (err) return done(err)
        request(opts({ TableName: helpers.testRangeNTable, ConsistentRead: true, KeyConditions: {
          a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] },
        }, ScanIndexForward: false, Limit: 3 }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ Count: 3, ScannedCount: 3, Items: [ item2, item3, item4 ], LastEvaluatedKey: { a: item4.a, b: item4.b } })
          done()
        })
      })
    })

    it('should return items in reverse order for binary', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { B: '1Py5xA==' } },
        item2 = { a: item.a, b: { B: 'JA==' } },
        item3 = { a: item.a, b: { B: '2w==' } },
        item4 = { a: item.a, b: { B: 'cAeRhZE=' } },
        item5 = { a: item.a, b: { B: '6piVtA==' } },
        item6 = { a: item.a, b: { B: 'MjA0' } },
        item7 = { a: item.a, b: { B: '1g==' } },
        item8 = { a: item.a, b: { B: 'ER/jLQ==' } },
        item9 = { a: item.a, b: { B: 'T7MzEUw=' } },
        items = [ item, item2, item3, item4, item5, item6, item7, item8, item9 ]
      helpers.batchBulkPut(helpers.testRangeBTable, items, function (err) {
        if (err) return done(err)
        request(opts({ TableName: helpers.testRangeBTable, ConsistentRead: true, KeyConditions: {
          a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] },
        }, ScanIndexForward: false }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ Count: 9, ScannedCount: 9, Items: [ item5, item3, item7, item, item4, item9,
            item6, item2, item8 ] })
          done()
        })
      })
    })

    it('should return items in reverse order with Limit for binary', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { B: '1Py5xA==' } },
        item2 = { a: item.a, b: { B: 'JA==' } },
        item3 = { a: item.a, b: { B: '2w==' } },
        item4 = { a: item.a, b: { B: 'cAeRhZE=' } },
        item5 = { a: item.a, b: { B: '6piVtA==' } },
        items = [ item, item2, item3, item4, item5 ]
      helpers.batchBulkPut(helpers.testRangeBTable, items, function (err) {
        if (err) return done(err)
        request(opts({ TableName: helpers.testRangeBTable, ConsistentRead: true, KeyConditions: {
          a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] },
        }, ScanIndexForward: false, Limit: 3 }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ Count: 3, ScannedCount: 3, Items: [ item5, item3, item ], LastEvaluatedKey: { a: item.a, b: item.b } })
          done()
        })
      })
    })

    it('should query on basic hash global index', function (done) {
      var item = { a: { S: 'a' }, b: { S: 'a' }, c: { S: helpers.randomString() }, d: { S: 'a' } },
        item2 = { a: { S: 'b' }, b: { S: 'b' }, c: item.c, d: { S: 'a' } },
        item3 = { a: { S: 'c' }, b: { S: 'e' }, c: item.c, d: { S: 'a' } },
        item4 = { a: { S: 'c' }, b: { S: 'd' }, c: item.c, d: { S: 'a' } },
        item5 = { a: { S: 'c' }, b: { S: 'c' }, c: { S: 'c' }, d: { S: 'a' } },
        item6 = { a: { S: 'd' }, b: { S: 'a' }, c: item.c, d: { S: 'a' } },
        item7 = { a: { S: 'e' }, b: { S: 'a' }, c: item.c, d: { S: 'a' } },
        items = [ item, item2, item3, item4, item5, item6, item7 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        var req = { TableName: helpers.testRangeTable,
          KeyConditions: { c: { ComparisonOperator: 'EQ', AttributeValueList: [ item.c ] } },
          IndexName: 'index3', Limit: 4, ReturnConsumedCapacity: 'TOTAL' }
        request(opts(req), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Count: 4,
            ScannedCount: 4,
            Items: [ item2, item, item3, item7 ],
            LastEvaluatedKey: { a: item7.a, b: item7.b, c: item7.c },
            ConsumedCapacity: { CapacityUnits: 0.5, TableName: helpers.testRangeTable },
          })
          req.ReturnConsumedCapacity = 'INDEXES'
          request(opts(req), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({
              Count: 4,
              ScannedCount: 4,
              Items: [ item2, item, item3, item7 ],
              LastEvaluatedKey: { a: item7.a, b: item7.b, c: item7.c },
              ConsumedCapacity: {
                CapacityUnits: 0.5,
                TableName: helpers.testRangeTable,
                Table: { CapacityUnits: 0 },
                GlobalSecondaryIndexes: { index3: { CapacityUnits: 0.5 } },
              },
            })
            done()
          })
        })
      })
    })

    it('should query in reverse on basic hash global index', function (done) {
      var item = { a: { S: 'a' }, b: { S: 'a' }, c: { S: helpers.randomString() } },
        item2 = { a: { S: 'b' }, b: { S: 'b' }, c: item.c },
        item3 = { a: { S: 'c' }, b: { S: 'e' }, c: item.c },
        item4 = { a: { S: 'c' }, b: { S: 'd' }, c: item.c },
        item5 = { a: { S: 'c' }, b: { S: 'c' }, c: { S: 'c' } },
        item6 = { a: { S: 'd' }, b: { S: 'a' }, c: item.c },
        item7 = { a: { S: 'e' }, b: { S: 'a' }, c: item.c },
        items = [ item, item2, item3, item4, item5, item6, item7 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        var req = { TableName: helpers.testRangeTable,
          KeyConditions: { c: { ComparisonOperator: 'EQ', AttributeValueList: [ item.c ] } },
          IndexName: 'index3', ScanIndexForward: false, Limit: 4, ReturnConsumedCapacity: 'INDEXES' }
        request(opts(req), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Count: 4,
            ScannedCount: 4,
            Items: [ item4, item6, item7, item3 ],
            LastEvaluatedKey: { a: item3.a, b: item3.b, c: item3.c },
            ConsumedCapacity: {
              CapacityUnits: 0.5,
              TableName: helpers.testRangeTable,
              Table: { CapacityUnits: 0 },
              GlobalSecondaryIndexes: { index3: { CapacityUnits: 0.5 } },
            },
          })
          done()
        })
      })
    })

    it('should query on range global index', function (done) {
      var item = { a: { S: 'a' }, b: { S: 'a' }, c: { S: helpers.randomString() }, d: { S: 'f' }, e: { S: 'a' }, f: { S: 'a' } },
        item2 = { a: { S: 'b' }, b: { S: 'b' }, c: item.c, d: { S: 'a' }, e: { S: 'a' }, f: { S: 'a' } },
        item3 = { a: { S: 'c' }, b: { S: 'e' }, c: item.c, d: { S: 'b' }, e: { S: 'a' }, f: { S: 'a' } },
        item4 = { a: { S: 'c' }, b: { S: 'd' }, c: item.c, d: { S: 'c' }, e: { S: 'a' }, f: { S: 'a' } },
        item5 = { a: { S: 'c' }, b: { S: 'c' }, c: { S: 'c' }, d: { S: 'd' }, e: { S: 'a' }, f: { S: 'a' } },
        item6 = { a: { S: 'd' }, b: { S: 'a' }, c: item.c, d: { S: 'e' }, e: { S: 'a' }, f: { S: 'a' } },
        item7 = { a: { S: 'e' }, b: { S: 'a' }, c: item.c, d: { S: 'f' }, e: { S: 'a' }, f: { S: 'a' } },
        items = [ item, item2, item3, item4, item5, item6, item7 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        request(opts({ TableName: helpers.testRangeTable, KeyConditions: {
          c: { ComparisonOperator: 'EQ', AttributeValueList: [ item.c ] },
          d: { ComparisonOperator: 'LT', AttributeValueList: [ item.d ] },
        }, IndexName: 'index4', Limit: 3, ReturnConsumedCapacity: 'INDEXES' }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          delete item2.f
          delete item3.f
          delete item4.f
          res.body.should.eql({
            Count: 3,
            ScannedCount: 3,
            Items: [ item2, item3, item4 ],
            LastEvaluatedKey: { a: item4.a, b: item4.b, c: item4.c, d: item4.d },
            ConsumedCapacity: {
              CapacityUnits: 0.5,
              TableName: helpers.testRangeTable,
              Table: { CapacityUnits: 0 },
              GlobalSecondaryIndexes: { index4: { CapacityUnits: 0.5 } },
            },
          })
          done()
        })
      })
    })

    it('should query in reverse on range global index', function (done) {
      var item = { a: { S: 'a' }, b: { S: 'a' }, c: { S: helpers.randomString() }, d: { S: 'f' } },
        item2 = { a: { S: 'b' }, b: { S: 'b' }, c: item.c, d: { S: 'a' } },
        item3 = { a: { S: 'c' }, b: { S: 'e' }, c: item.c, d: { S: 'b' } },
        item4 = { a: { S: 'c' }, b: { S: 'd' }, c: item.c, d: { S: 'c' } },
        item5 = { a: { S: 'c' }, b: { S: 'c' }, c: { S: 'c' }, d: { S: 'd' } },
        item6 = { a: { S: 'd' }, b: { S: 'a' }, c: item.c, d: { S: 'e' } },
        item7 = { a: { S: 'e' }, b: { S: 'a' }, c: item.c, d: { S: 'f' } },
        items = [ item, item2, item3, item4, item5, item6, item7 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        request(opts({ TableName: helpers.testRangeTable, KeyConditions: {
          c: { ComparisonOperator: 'EQ', AttributeValueList: [ item.c ] },
          d: { ComparisonOperator: 'LT', AttributeValueList: [ item.d ] },
        }, IndexName: 'index4', ScanIndexForward: false, Limit: 3, ReturnConsumedCapacity: 'INDEXES' }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Count: 3,
            ScannedCount: 3,
            Items: [ item6, item4, item3 ],
            LastEvaluatedKey: { a: item3.a, b: item3.b, c: item3.c, d: item3.d },
            ConsumedCapacity: {
              CapacityUnits: 0.5,
              TableName: helpers.testRangeTable,
              Table: { CapacityUnits: 0 },
              GlobalSecondaryIndexes: { index4: { CapacityUnits: 0.5 } },
            },
          })
          done()
        })
      })
    })

    it('should query with ExclusiveStartKey on basic hash global index', function (done) {
      var item = { a: { S: 'a' }, b: { S: 'a' }, c: { S: helpers.randomString() }, d: { S: 'a' } },
        item2 = { a: { S: 'b' }, b: { S: 'b' }, c: item.c, d: { S: 'a' } },
        item3 = { a: { S: 'c' }, b: { S: 'e' }, c: item.c, d: { S: 'a' } },
        item4 = { a: { S: 'c' }, b: { S: 'd' }, c: item.c, d: { S: 'a' } },
        item5 = { a: { S: 'c' }, b: { S: 'c' }, c: { S: 'c' }, d: { S: 'a' } },
        item6 = { a: { S: 'd' }, b: { S: 'a' }, c: item.c, d: { S: 'a' } },
        item7 = { a: { S: 'e' }, b: { S: 'a' }, c: item.c, d: { S: 'a' } },
        items = [ item, item2, item3, item4, item5, item6, item7 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        delete item3.d
        request(opts({ TableName: helpers.testRangeTable, KeyConditions: {
          c: { ComparisonOperator: 'EQ', AttributeValueList: [ item.c ] },
        }, IndexName: 'index3', Limit: 2, ExclusiveStartKey: item3, ReturnConsumedCapacity: 'INDEXES' }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Count: 2,
            ScannedCount: 2,
            Items: [ item7, item6 ],
            LastEvaluatedKey: { a: item6.a, b: item6.b, c: item6.c },
            ConsumedCapacity: {
              CapacityUnits: 0.5,
              TableName: helpers.testRangeTable,
              Table: { CapacityUnits: 0 },
              GlobalSecondaryIndexes: { index3: { CapacityUnits: 0.5 } },
            },
          })
          done()
        })
      })
    })

    it('should query in reverse with ExclusiveStartKey on basic hash global index', function (done) {
      var item = { a: { S: 'a' }, b: { S: 'a' }, c: { S: helpers.randomString() } },
        item2 = { a: { S: 'b' }, b: { S: 'b' }, c: item.c },
        item3 = { a: { S: 'c' }, b: { S: 'e' }, c: item.c },
        item4 = { a: { S: 'c' }, b: { S: 'd' }, c: item.c },
        item5 = { a: { S: 'c' }, b: { S: 'c' }, c: { S: 'c' } },
        item6 = { a: { S: 'd' }, b: { S: 'a' }, c: item.c },
        item7 = { a: { S: 'e' }, b: { S: 'a' }, c: item.c },
        items = [ item, item2, item3, item4, item5, item6, item7 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        delete item7.d
        request(opts({ TableName: helpers.testRangeTable, KeyConditions: {
          c: { ComparisonOperator: 'EQ', AttributeValueList: [ item.c ] },
        }, IndexName: 'index3', ScanIndexForward: false, Limit: 2, ExclusiveStartKey: item7, ReturnConsumedCapacity: 'INDEXES' }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Count: 2,
            ScannedCount: 2,
            Items: [ item3, item ],
            LastEvaluatedKey: { a: item.a, b: item.b, c: item.c },
            ConsumedCapacity: {
              CapacityUnits: 0.5,
              TableName: helpers.testRangeTable,
              Table: { CapacityUnits: 0 },
              GlobalSecondaryIndexes: { index3: { CapacityUnits: 0.5 } },
            },
          })
          done()
        })
      })
    })

    it('should query with ExclusiveStartKey on range global index', function (done) {
      var item = { a: { S: 'a' }, b: { S: 'a' }, c: { S: helpers.randomString() }, d: { S: 'f' }, e: { S: 'a' }, f: { S: 'a' } },
        item2 = { a: { S: 'b' }, b: { S: 'b' }, c: item.c, d: { S: 'a' }, e: { S: 'a' }, f: { S: 'a' } },
        item3 = { a: { S: 'c' }, b: { S: 'e' }, c: item.c, d: { S: 'b' }, e: { S: 'a' }, f: { S: 'a' } },
        item4 = { a: { S: 'c' }, b: { S: 'd' }, c: item.c, d: { S: 'c' }, e: { S: 'a' }, f: { S: 'a' } },
        item5 = { a: { S: 'c' }, b: { S: 'c' }, c: { S: 'c' }, d: { S: 'd' }, e: { S: 'a' }, f: { S: 'a' } },
        item6 = { a: { S: 'd' }, b: { S: 'a' }, c: item.c, d: { S: 'e' }, e: { S: 'a' }, f: { S: 'a' } },
        item7 = { a: { S: 'e' }, b: { S: 'a' }, c: item.c, d: { S: 'f' }, e: { S: 'a' }, f: { S: 'a' } },
        items = [ item, item2, item3, item4, item5, item6, item7 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        delete item3.e
        delete item3.f
        delete item4.f
        request(opts({ TableName: helpers.testRangeTable, KeyConditions: {
          c: { ComparisonOperator: 'EQ', AttributeValueList: [ item.c ] },
          d: { ComparisonOperator: 'LT', AttributeValueList: [ item.d ] },
        }, IndexName: 'index4', Limit: 1, ExclusiveStartKey: item3, ReturnConsumedCapacity: 'INDEXES' }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Count: 1,
            ScannedCount: 1,
            Items: [ item4 ],
            LastEvaluatedKey: { a: item4.a, b: item4.b, c: item4.c, d: item4.d },
            ConsumedCapacity: {
              CapacityUnits: 0.5,
              TableName: helpers.testRangeTable,
              Table: { CapacityUnits: 0 },
              GlobalSecondaryIndexes: { index4: { CapacityUnits: 0.5 } },
            },
          })
          done()
        })
      })
    })

    it('should query in reverse with ExclusiveStartKey on range global index', function (done) {
      var item = { a: { S: 'a' }, b: { S: 'a' }, c: { S: helpers.randomString() }, d: { S: 'f' }, e: { S: 'a' }, f: { S: 'a' } },
        item2 = { a: { S: 'b' }, b: { S: 'b' }, c: item.c, d: { S: 'a' }, e: { S: 'a' }, f: { S: 'a' } },
        item3 = { a: { S: 'c' }, b: { S: 'e' }, c: item.c, d: { S: 'b' }, e: { S: 'a' }, f: { S: 'a' } },
        item4 = { a: { S: 'c' }, b: { S: 'd' }, c: item.c, d: { S: 'c' }, e: { S: 'a' }, f: { S: 'a' } },
        item5 = { a: { S: 'c' }, b: { S: 'c' }, c: { S: 'c' }, d: { S: 'd' }, e: { S: 'a' }, f: { S: 'a' } },
        item6 = { a: { S: 'd' }, b: { S: 'a' }, c: item.c, d: { S: 'e' }, e: { S: 'a' }, f: { S: 'a' } },
        item7 = { a: { S: 'e' }, b: { S: 'a' }, c: item.c, d: { S: 'f' }, e: { S: 'a' }, f: { S: 'a' } },
        items = [ item, item2, item3, item4, item5, item6, item7 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        delete item4.e
        delete item4.f
        delete item3.f
        request(opts({ TableName: helpers.testRangeTable, KeyConditions: {
          c: { ComparisonOperator: 'EQ', AttributeValueList: [ item.c ] },
          d: { ComparisonOperator: 'LT', AttributeValueList: [ item.d ] },
        }, IndexName: 'index4', Limit: 1, ScanIndexForward: false, ExclusiveStartKey: item4, ReturnConsumedCapacity: 'INDEXES' }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Count: 1,
            ScannedCount: 1,
            Items: [ item3 ],
            LastEvaluatedKey: { a: item3.a, b: item3.b, c: item3.c, d: item3.d },
            ConsumedCapacity: {
              CapacityUnits: 0.5,
              TableName: helpers.testRangeTable,
              Table: { CapacityUnits: 0 },
              GlobalSecondaryIndexes: { index4: { CapacityUnits: 0.5 } },
            },
          })
          done()
        })
      })
    })

    it('should query on a global index if values are equal', function (done) {
      var item = { a: { S: 'a' }, b: { S: 'a' }, c: { S: helpers.randomString() }, d: { S: 'a' } },
        item2 = { a: { S: 'b' }, b: { S: 'a' }, c: item.c, d: { S: 'a' } },
        item3 = { a: { S: 'c' }, b: { S: 'a' }, c: item.c, d: { S: 'a' } },
        item4 = { a: { S: 'c' }, b: { S: 'b' }, c: item.c, d: { S: 'a' } },
        item5 = { a: { S: 'd' }, b: { S: 'a' }, c: item.c, d: { S: 'a' } },
        item6 = { a: { S: 'd' }, b: { S: 'b' }, c: item.c, d: { S: 'a' } },
        items = [ item, item2, item3, item4, item5, item6 ]
      helpers.batchBulkPut(helpers.testRangeTable, items, function (err) {
        if (err) return done(err)
        request(opts({ TableName: helpers.testRangeTable, KeyConditions: {
          c: { ComparisonOperator: 'EQ', AttributeValueList: [ item.c ] },
        }, IndexName: 'index4', ExclusiveStartKey: item, ReturnConsumedCapacity: 'INDEXES' }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Count: 5,
            ScannedCount: 5,
            Items: [ item5, item2, item3, item6, item4 ],
            ConsumedCapacity: {
              CapacityUnits: 0.5,
              TableName: helpers.testRangeTable,
              Table: { CapacityUnits: 0 },
              GlobalSecondaryIndexes: { index4: { CapacityUnits: 0.5 } },
            },
          })
          done()
        })
      })
    })

    // High capacity (~100 or more) needed to run this quickly
    if (runSlowTests) {
      it('should not return LastEvaluatedKey if just under limit', function (done) {
        this.timeout(200000)

        var i, items = [], id = helpers.randomString(), e = new Array(41646).join('e'), eAttr = e.slice(0, 255)
        for (i = 0; i < 25; i++) {
          var item = { a: { S: id }, b: { S: ('0' + i).slice(-2) } }
          item[eAttr] = { S: e }
          items.push(item)
        }

        helpers.replaceTable(helpers.testRangeTable, [ 'a', 'b' ], items, function (err) {
          if (err) return done(err)

          request(opts({
            TableName: helpers.testRangeTable,
            KeyConditions: { a: { ComparisonOperator: 'EQ', AttributeValueList: [ { S: id } ] } },
            Select: 'COUNT',
            ReturnConsumedCapacity: 'INDEXES',
            Limit: 26, // Limit of 25 includes LastEvaluatedKey, leaving this out does not
          }), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({
              Count: 25,
              ScannedCount: 25,
              ConsumedCapacity: {
                CapacityUnits: 128,
                Table: { CapacityUnits: 128 },
                TableName: helpers.testRangeTable,
              },
            })
            helpers.clearTable(helpers.testRangeTable, [ 'a', 'b' ], done)
          })
        })
      })

      it('should return LastEvaluatedKey if just over limit', function (done) {
        this.timeout(200000)

        var i, items = [], id = helpers.randomString(), e = new Array(41646).join('e')
        for (i = 0; i < 25; i++)
          items.push({ a: { S: id }, b: { S: ('0' + i).slice(-2) }, e: { S: e } })
        items[24].e.S = new Array(41647).join('e')

        helpers.replaceTable(helpers.testRangeTable, [ 'a', 'b' ], items, function (err) {
          if (err) return done(err)

          request(opts({
            TableName: helpers.testRangeTable,
            KeyConditions: { a: { ComparisonOperator: 'EQ', AttributeValueList: [ { S: id } ] } },
            Select: 'COUNT',
            ReturnConsumedCapacity: 'INDEXES',
          }), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({
              Count: 25,
              ScannedCount: 25,
              ConsumedCapacity: {
                CapacityUnits: 127.5,
                Table: { CapacityUnits: 127.5 },
                TableName: helpers.testRangeTable,
              },
              LastEvaluatedKey: { a: items[24].a, b: items[24].b },
            })
            helpers.clearTable(helpers.testRangeTable, [ 'a', 'b' ], done)
          })
        })
      })

      it('should return all if just under limit', function (done) {
        this.timeout(200000)

        var i, items = [], id = helpers.randomString(), e = new Array(43373).join('e'), eAttr = e.slice(0, 255)
        for (i = 0; i < 25; i++) {
          var item = { a: { S: id }, b: { S: ('0' + i).slice(-2) } }
          item[eAttr] = { S: e }
          items.push(item)
        }
        items[23][eAttr].S = new Array(43388).join('e')
        items[24][eAttr].S = new Array(45000).join('e')

        helpers.replaceTable(helpers.testRangeTable, [ 'a', 'b' ], items, function (err) {
          if (err) return done(err)

          request(opts({
            TableName: helpers.testRangeTable,
            KeyConditions: { a: { ComparisonOperator: 'EQ', AttributeValueList: [ { S: id } ] } },
            Select: 'COUNT',
            ReturnConsumedCapacity: 'TOTAL',
          }), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({
              Count: 25,
              ScannedCount: 25,
              ConsumedCapacity: { CapacityUnits: 133.5, TableName: helpers.testRangeTable },
              LastEvaluatedKey: { a: items[24].a, b: items[24].b },
            })
            helpers.clearTable(helpers.testRangeTable, [ 'a', 'b' ], done)
          })
        })
      })

      it('should return one less than all if just over limit', function (done) {
        this.timeout(200000)

        var i, items = [], id = helpers.randomString(), e = new Array(43373).join('e')
        for (i = 0; i < 25; i++)
          items.push({ a: { S: id }, b: { S: ('0' + i).slice(-2) }, e: { S: e } })
        items[23].e.S = new Array(43389).join('e')
        items[24].e.S = new Array(45000).join('e')

        helpers.replaceTable(helpers.testRangeTable, [ 'a', 'b' ], items, function (err) {
          if (err) return done(err)

          request(opts({
            TableName: helpers.testRangeTable,
            KeyConditions: { a: { ComparisonOperator: 'EQ', AttributeValueList: [ { S: id } ] } },
            Select: 'COUNT',
            ReturnConsumedCapacity: 'TOTAL',
          }), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({
              Count: 24,
              ScannedCount: 24,
              ConsumedCapacity: { CapacityUnits: 127.5, TableName: helpers.testRangeTable },
              LastEvaluatedKey: { a: items[23].a, b: items[23].b },
            })
            helpers.clearTable(helpers.testRangeTable, [ 'a', 'b' ], done)
          })
        })
      })
    }

  })
})