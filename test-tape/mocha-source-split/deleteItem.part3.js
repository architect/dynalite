var async = require('async'),
  helpers = require('./helpers')

var target = 'DeleteItem',
  request = helpers.request,
  opts = helpers.opts.bind(null, target),
  assertType = helpers.assertType.bind(null, target),
  assertValidation = helpers.assertValidation.bind(null, target),
  assertConditional = helpers.assertConditional.bind(null, target)

describe('deleteItem', function () {
  describe('functionality', function () {

    it('should return nothing if item does not exist', function (done) {
      request(opts({ TableName: helpers.testHashTable, Key: { a: { S: helpers.randomString() } } }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({})
        done()
      })
    })

    it('should return ConsumedCapacity if specified and item does not exist', function (done) {
      var req = { TableName: helpers.testHashTable, Key: { a: { S: helpers.randomString() } }, ReturnConsumedCapacity: 'TOTAL' }
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

    it('should delete item successfully', function (done) {
      var item = { a: { S: helpers.randomString() } }
      request(helpers.opts('PutItem', { TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({ TableName: helpers.testHashTable, Key: { a: item.a } }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({})
          request(helpers.opts('GetItem', { TableName: helpers.testHashTable, Key: { a: item.a }, ConsistentRead: true }), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            done()
          })
        })
      })
    })

    it('should delete item successfully and return old values', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: 'b' } }
      request(helpers.opts('PutItem', { TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({ TableName: helpers.testHashTable, Key: { a: item.a }, ReturnValues: 'ALL_OLD' }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ Attributes: item })
          done()
        })
      })
    })

    it('should return ConditionalCheckFailedException if expecting non-existent key to exist', function (done) {
      async.forEach([
        { Expected: { a: { Value: { S: helpers.randomString() } } } },
        { ConditionExpression: 'a = :a', ExpressionAttributeValues: { ':a': { S: helpers.randomString() } } },
        { ConditionExpression: '#a = :a', ExpressionAttributeNames: { '#a': 'a' }, ExpressionAttributeValues: { ':a': { S: helpers.randomString() } } },
      ], function (deleteOpts, cb) {
        deleteOpts.TableName = helpers.testHashTable
        deleteOpts.Key = { a: { S: helpers.randomString() } }
        assertConditional(deleteOpts, cb)
      }, done)
    })

    it('should return ConditionalCheckFailedException if expecting existing key to not exist', function (done) {
      var item = { a: { S: helpers.randomString() } }
      request(helpers.opts('PutItem', { TableName: helpers.testHashTable, Item: item }), function (err) {
        if (err) return done(err)
        async.forEach([
          { Expected: { a: { Exists: false } } },
          { ConditionExpression: 'attribute_not_exists(a)' },
        ], function (deleteOpts, cb) {
          deleteOpts.TableName = helpers.testHashTable
          deleteOpts.Key = { a: item.a }
          assertConditional(deleteOpts, cb)
        }, done)
      })
    })

    it('should succeed if conditional key is different and exists is false', function (done) {
      var item = { a: { S: helpers.randomString() } }
      request(helpers.opts('PutItem', { TableName: helpers.testHashTable, Item: item }), function (err) {
        if (err) return done(err)
        async.forEach([
          { Expected: { a: { Exists: false } } },
          { ConditionExpression: 'attribute_not_exists(a)' },
        ], function (deleteOpts, cb) {
          deleteOpts.TableName = helpers.testHashTable
          deleteOpts.Key = { a: { S: helpers.randomString() } }
          request(opts(deleteOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should succeed if conditional key is same and exists is true', function (done) {
      async.forEach([
        { Expected: { a: { Value: { S: helpers.randomString() } } } },
        { ConditionExpression: 'a = :a', ExpressionAttributeValues: { ':a': { S: helpers.randomString() } } },
        { ConditionExpression: '#a = :a', ExpressionAttributeNames: { '#a': 'a' }, ExpressionAttributeValues: { ':a': { S: helpers.randomString() } } },
      ], function (deleteOpts, cb) {
        var item = { a: deleteOpts.Expected ? deleteOpts.Expected.a.Value : deleteOpts.ExpressionAttributeValues[':a'] }
        request(helpers.opts('PutItem', { TableName: helpers.testHashTable, Item: item }), function (err) {
          if (err) return cb(err)
          deleteOpts.TableName = helpers.testHashTable
          deleteOpts.Key = item
          request(opts(deleteOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        })
      }, done)
    })

    it('should succeed if expecting non-existant value to not exist', function (done) {
      async.forEach([
        { Expected: { b: { Exists: false } }, Key: { a: { S: helpers.randomString() } } },
        { ConditionExpression: 'attribute_not_exists(b)', Key: { a: { S: helpers.randomString() } } },
        { ConditionExpression: 'attribute_not_exists(#b)', ExpressionAttributeNames: { '#b': 'b' }, Key: { a: { S: helpers.randomString() } } },
      ], function (deleteOpts, cb) {
        var item = deleteOpts.Key
        request(helpers.opts('PutItem', { TableName: helpers.testHashTable, Item: item }), function (err) {
          if (err) return cb(err)
          deleteOpts.TableName = helpers.testHashTable
          request(opts(deleteOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        })
      }, done)
    })

    it('should return ConditionalCheckFailedException if expecting existing value to not exist', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: helpers.randomString() } }
      request(helpers.opts('PutItem', { TableName: helpers.testHashTable, Item: item }), function (err) {
        if (err) return done(err)
        async.forEach([
          { Expected: { b: { Exists: false } } },
          { ConditionExpression: 'attribute_not_exists(b)' },
          { ConditionExpression: 'attribute_not_exists(#b)', ExpressionAttributeNames: { '#b': 'b' } },
        ], function (deleteOpts, cb) {
          deleteOpts.TableName = helpers.testHashTable
          deleteOpts.Key = { a: item.a }
          assertConditional(deleteOpts, cb)
        }, done)
      })
    })

    it('should succeed for multiple conditional checks if all are valid', function (done) {
      async.forEach([
        { Expected: { a: { Value: { S: helpers.randomString() } }, b: { Exists: false }, c: { Value: { S: helpers.randomString() } } } },
        { ConditionExpression: 'a = :a AND attribute_not_exists(b) AND c = :c', ExpressionAttributeValues: { ':a': { S: helpers.randomString() }, ':c': { S: helpers.randomString() } } },
        { ConditionExpression: '#a = :a AND attribute_not_exists(#b) AND #c = :c', ExpressionAttributeNames: { '#a': 'a', '#b': 'b', '#c': 'c' }, ExpressionAttributeValues: { ':a': { S: helpers.randomString() }, ':c': { S: helpers.randomString() } } },
      ], function (deleteOpts, cb) {
        var item = deleteOpts.Expected ? { a: deleteOpts.Expected.a.Value, c: deleteOpts.Expected.c.Value } :
          { a: deleteOpts.ExpressionAttributeValues[':a'], c: deleteOpts.ExpressionAttributeValues[':c'] }
        request(helpers.opts('PutItem', { TableName: helpers.testHashTable, Item: item }), function (err) {
          if (err) return cb(err)
          deleteOpts.TableName = helpers.testHashTable
          deleteOpts.Key = { a: item.a }
          request(opts(deleteOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        })
      }, done)
    })

    it('should return ConditionalCheckFailedException for multiple conditional checks if one is invalid', function (done) {
      var item = { a: { S: helpers.randomString() }, c: { S: helpers.randomString() } }
      request(helpers.opts('PutItem', { TableName: helpers.testHashTable, Item: item }), function (err) {
        if (err) return done(err)
        async.forEach([
          { Expected: { a: { Value: item.a }, b: { Exists: false }, c: { Value: { S: helpers.randomString() } } } },
          { ConditionExpression: 'a = :a AND attribute_not_exists(b) AND c = :c', ExpressionAttributeValues: { ':a': item.a, ':c': { S: helpers.randomString() } } },
          { ConditionExpression: '#a = :a AND attribute_not_exists(#b) AND #c = :c', ExpressionAttributeNames: { '#a': 'a', '#b': 'b', '#c': 'c' }, ExpressionAttributeValues: { ':a': item.a, ':c': { S: helpers.randomString() } } },
        ], function (deleteOpts, cb) {
          deleteOpts.TableName = helpers.testHashTable
          deleteOpts.Key = { a: item.a }
          assertConditional(deleteOpts, cb)
        }, done)
      })
    })

    it('should return ConsumedCapacity for small item', function (done) {
      var a = helpers.randomString(), b = new Array(1010 - a.length).join('b'),
        item = { a: { S: a }, b: { S: b }, c: { N: '12.3456' }, d: { B: 'AQI=' }, e: { BS: [ 'AQI=', 'Ag==', 'AQ==' ] } }
      request(helpers.opts('PutItem', { TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({ TableName: helpers.testHashTable, Key: { a: item.a }, ReturnConsumedCapacity: 'TOTAL' }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ ConsumedCapacity: { CapacityUnits: 1, TableName: helpers.testHashTable } })
          done()
        })
      })
    })

    it('should return ConsumedCapacity for larger item', function (done) {
      var a = helpers.randomString(), b = new Array(1012 - a.length).join('b'),
        item = { a: { S: a }, b: { S: b }, c: { N: '12.3456' }, d: { B: 'AQI=' }, e: { BS: [ 'AQI=', 'Ag==' ] } }
      request(helpers.opts('PutItem', { TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({ TableName: helpers.testHashTable, Key: { a: item.a }, ReturnConsumedCapacity: 'TOTAL' }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ ConsumedCapacity: { CapacityUnits: 2, TableName: helpers.testHashTable } })
          done()
        })
      })
    })

  })
})