var async = require('async'),
  helpers = require('./helpers')

var target = 'PutItem',
  request = helpers.request,
  randomName = helpers.randomName,
  opts = helpers.opts.bind(null, target),
  assertType = helpers.assertType.bind(null, target),
  assertValidation = helpers.assertValidation.bind(null, target),
  assertNotFound = helpers.assertNotFound.bind(null, target),
  assertConditional = helpers.assertConditional.bind(null, target)

describe('putItem', function () {
  // A number can have up to 38 digits precision and can be between 10^-128 to 10^+126

  describe('functionality', function () {

    it('should put basic item', function (done) {
      var item = { a: { S: helpers.randomString() } }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({})
        request(helpers.opts('GetItem', { TableName: helpers.testHashTable, Key: { a: item.a }, ConsistentRead: true }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ Item: item })
          done()
        })
      })
    })

    it('should put empty values', function (done) {
      var item = {
        a: { S: helpers.randomString() },
        b: { S: '' },
        c: { B: '' },
        d: { SS: [ '' ] },
        e: { BS: [ '' ] },
      }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.body.should.eql({})
        res.statusCode.should.equal(200)
        request(helpers.opts('GetItem', { TableName: helpers.testHashTable, Key: { a: item.a }, ConsistentRead: true }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          item.b = { S: '' }
          item.c = { B: '' }
          item.d = { SS: [ '' ] }
          item.e = { BS: [ '' ] }
          res.body.should.eql({ Item: item })
          done()
        })
      })
    })

    it('should put really long numbers', function (done) {
      var item = {
        a: { S: helpers.randomString() },
        b: { N: '0000012345678901234567890123456789012345678' },
        c: { N: '-00001.23456789012345678901234567890123456780000' },
        d: { N: '0009.99999999999999999999999999999999999990000e125' },
        e: { N: '-0009.99999999999999999999999999999999999990000e125' },
        f: { N: '0001.000e-130' },
        g: { N: '-0001.000e-130' },
      }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({})
        request(helpers.opts('GetItem', { TableName: helpers.testHashTable, Key: { a: item.a }, ConsistentRead: true }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          item.b = { N: '12345678901234567890123456789012345678' }
          item.c = { N: '-1.2345678901234567890123456789012345678' }
          item.d = { N: Array(39).join('9') + Array(89).join('0') }
          item.e = { N: '-' + Array(39).join('9') + Array(89).join('0') }
          item.f = { N: '0.' + Array(130).join('0') + '1' }
          item.g = { N: '-0.' + Array(130).join('0') + '1' }
          res.body.should.eql({ Item: item })
          done()
        })
      })
    })

    it('should put multi attribute item', function (done) {
      var item = {
        a: { S: helpers.randomString() },
        b: { N: '-56.789' },
        c: { B: 'Yg==' },
        d: { BOOL: false },
        e: { NULL: true },
        f: { SS: [ 'a' ] },
        g: { NS: [ '-56.789' ] },
        h: { BS: [ 'Yg==' ] },
        i: { L: [
          { S: 'a' },
          { N: '-56.789' },
          { B: 'Yg==' },
          { BOOL: true },
          { NULL: true },
          { SS: [ 'a' ] },
          { NS: [ '-56.789' ] },
          { BS: [ 'Yg==' ] },
          { L: [] },
          { M: {} },
        ] },
        j: { M: {
          a: { S: 'a' },
          b: { N: '-56.789' },
          c: { B: 'Yg==' },
          d: { BOOL: true },
          e: { NULL: true },
          f: { SS: [ 'a' ] },
          g: { NS: [ '-56.789' ] },
          h: { BS: [ 'Yg==' ] },
          i: { L: [] },
          j: { M: { a: { M: {} }, b: { L: [] } } },
        } },
      }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({})
        request(helpers.opts('GetItem', { TableName: helpers.testHashTable, Key: { a: item.a }, ConsistentRead: true }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ Item: item })
          done()
        })
      })
    })

    it('should return empty when there are no old values', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: 'a' }, c: { S: 'a' } }
      request(opts({ TableName: helpers.testHashTable, Item: item, ReturnValues: 'ALL_OLD' }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({})
        done()
      })
    })

    it('should return correct old values when they exist', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { N: '-0015.789e6' }, c: { S: 'a' } }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        item.b = { S: 'b' }
        request(opts({ TableName: helpers.testHashTable, Item: item, ReturnValues: 'ALL_OLD' }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          item.b = { N: '-15789000' }
          res.body.should.eql({ Attributes: item })
          done()
        })
      })
    })

    it('should put basic range item', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: 'a' }, c: { S: 'a' } }
      request(opts({ TableName: helpers.testRangeTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({})
        // Put another item with the same hash key to prove we're retrieving the correct one
        request(opts({ TableName: helpers.testRangeTable, Item: { a: item.a, b: { S: 'b' } } }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          request(helpers.opts('GetItem', { TableName: helpers.testRangeTable, Key: { a: item.a, b: item.b }, ConsistentRead: true }), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({ Item: item })
            done()
          })
        })
      })
    })

    it('should return ConditionalCheckFailedException if expecting non-existent key to exist', function (done) {
      async.forEach([
        { Expected: { a: { Value: { S: helpers.randomString() } } } },
        { Expected: { a: { ComparisonOperator: 'NOT_NULL' } } },
        { ConditionExpression: 'a = :a', ExpressionAttributeValues: { ':a': { S: helpers.randomString() } } },
        { ConditionExpression: '#a = :a', ExpressionAttributeNames: { '#a': 'a' }, ExpressionAttributeValues: { ':a': { S: helpers.randomString() } } },
        { ConditionExpression: 'attribute_exists(a)' },
        { ConditionExpression: 'attribute_exists(#a)', ExpressionAttributeNames: { '#a': 'a' } },
      ], function (putOpts, cb) {
        putOpts.TableName = helpers.testHashTable
        putOpts.Item = { a: { S: helpers.randomString() } }
        assertConditional(putOpts, cb)
      }, done)
    })

    it('should return ConditionalCheckFailedException if expecting existing key to not exist', function (done) {
      var item = { a: { S: helpers.randomString() } }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          { Expected: { a: { Exists: false } } },
          { Expected: { a: { ComparisonOperator: 'NULL' } } },
          { ConditionExpression: 'attribute_not_exists(a)' },
          { ConditionExpression: 'attribute_not_exists(#a)', ExpressionAttributeNames: { '#a': 'a' } },
        ], function (putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          assertConditional(putOpts, cb)
        }, done)
      })
    })

    it('should succeed if conditional key is different and exists is false', function (done) {
      var item = { a: { S: helpers.randomString() } }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          { Expected: { a: { Exists: false } } },
          { Expected: { a: { ComparisonOperator: 'NULL' } } },
          { ConditionExpression: 'attribute_not_exists(a)' },
          { ConditionExpression: 'attribute_not_exists(#a)', ExpressionAttributeNames: { '#a': 'a' } },
        ], function (putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = { a: { S: helpers.randomString() } }
          request(opts(putOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should succeed if conditional key is same', function (done) {
      var item = { a: { S: helpers.randomString() } }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          { Expected: { a: { Value: item.a } } },
          { Expected: { a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] } } },
          { Expected: { a: { Value: item.a, ComparisonOperator: 'EQ' } } },
          { Expected: { b: { Exists: false } } },
          { Expected: { b: { ComparisonOperator: 'NULL' } } },
          { ConditionExpression: 'a = :a', ExpressionAttributeValues: { ':a': item.a } },
          { ConditionExpression: '#a = :a', ExpressionAttributeNames: { '#a': 'a' }, ExpressionAttributeValues: { ':a': item.a } },
          { ConditionExpression: 'attribute_not_exists(b)' },
          { ConditionExpression: 'attribute_not_exists(#b)', ExpressionAttributeNames: { '#b': 'b' } },
        ], function (putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          request(opts(putOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should return ConditionalCheckFailedException if expecting existing value to not exist if different value specified', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: helpers.randomString() } }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          { Expected: { b: { Exists: false } } },
          { Expected: { b: { ComparisonOperator: 'NULL' } } },
          { ConditionExpression: 'attribute_not_exists(b)' },
          { ConditionExpression: 'attribute_not_exists(#b)', ExpressionAttributeNames: { '#b': 'b' } },
        ], function (putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = { a: item.a, b: { S: helpers.randomString() } }
          assertConditional(putOpts, cb)
        }, done)
      })
    })

    it('should return ConditionalCheckFailedException if expecting existing value to not exist if value not specified', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: helpers.randomString() } }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          { Expected: { b: { Exists: false } } },
          { Expected: { b: { ComparisonOperator: 'NULL' } } },
          { ConditionExpression: 'attribute_not_exists(b)' },
          { ConditionExpression: 'attribute_not_exists(#b)', ExpressionAttributeNames: { '#b': 'b' } },
        ], function (putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = { a: item.a }
          assertConditional(putOpts, cb)
        }, done)
      })
    })

    it('should return ConditionalCheckFailedException if expecting existing value to not exist if same value specified', function (done) {
      var item = { a: { S: helpers.randomString() }, b: { S: helpers.randomString() } }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          { Expected: { b: { Exists: false } } },
          { Expected: { b: { ComparisonOperator: 'NULL' } } },
          { ConditionExpression: 'attribute_not_exists(b)' },
          { ConditionExpression: 'attribute_not_exists(#b)', ExpressionAttributeNames: { '#b': 'b' } },
        ], function (putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          assertConditional(putOpts, cb)
        }, done)
      })
    })

    it('should succeed for multiple conditional checks if all are valid', function (done) {
      var item = { a: { S: helpers.randomString() }, c: { S: helpers.randomString() } }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          { Expected: { a: { Value: item.a }, b: { Exists: false }, c: { ComparisonOperator: 'GE', Value: item.c } } },
          { Expected: {
            a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] },
            b: { ComparisonOperator: 'NULL' },
            c: { ComparisonOperator: 'GE', AttributeValueList: [ item.c ] },
          } },
          {
            ConditionExpression: 'a = :a AND attribute_not_exists(#b) AND c >= :c',
            ExpressionAttributeValues: { ':a': item.a, ':c': item.c },
            ExpressionAttributeNames: { '#b': 'b' },
          },
        ], function (putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          request(opts(putOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should return ConditionalCheckFailedException for multiple conditional checks if one is invalid', function (done) {
      var item = { a: { S: helpers.randomString() }, c: { S: helpers.randomString() } }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          { Expected: { a: { Value: item.a }, b: { Exists: false }, c: { Value: { S: helpers.randomString() } } } },
          { Expected: {
            a: { AttributeValueList: [ item.a ], ComparisonOperator: 'EQ' },
            b: { ComparisonOperator: 'NULL' },
            c: { AttributeValueList: [ { S: helpers.randomString() } ], ComparisonOperator: 'EQ' },
          } },
          {
            ConditionExpression: 'a = :a AND attribute_not_exists(#b) AND c = :c',
            ExpressionAttributeValues: { ':a': item.a, ':c': { S: helpers.randomString() } },
            ExpressionAttributeNames: { '#b': 'b' },
          },
        ], function (putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          assertConditional(putOpts, cb)
        }, done)
      })
    })

    it('should succeed for multiple conditional checks if one is invalid and OR is specified', function (done) {
      var item = { a: { S: helpers.randomString() }, c: { S: helpers.randomString() } }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          { Expected: {
            a: { ComparisonOperator: 'EQ', AttributeValueList: [ item.a ] },
            b: { ComparisonOperator: 'NULL' },
            c: { ComparisonOperator: 'EQ', AttributeValueList: [ { S: helpers.randomString() } ] },
          }, ConditionalOperator: 'OR' },
          {
            ConditionExpression: 'a = :a OR attribute_not_exists(#b) OR c = :c',
            ExpressionAttributeValues: { ':a': item.a, ':c': { S: helpers.randomString() } },
            ExpressionAttributeNames: { '#b': 'b' },
          },
        ], function (putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          request(opts(putOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should succeed if condition is valid: NE', function (done) {
      var item = { a: { S: helpers.randomString() } }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          { Expected: { a: { ComparisonOperator: 'NE', AttributeValueList: [ { S: helpers.randomString() } ] } } },
          {
            ConditionExpression: 'a <> :a',
            ExpressionAttributeValues: { ':a': { S: helpers.randomString() } },
          },
          {
            ConditionExpression: '#a <> :a',
            ExpressionAttributeValues: { ':a': { S: helpers.randomString() } },
            ExpressionAttributeNames: { '#a': 'a' },
          },
        ], function (putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          request(opts(putOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should fail if condition is invalid: NE', function (done) {
      var item = { a: { S: helpers.randomString() } }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          { Expected: { a: { ComparisonOperator: 'NE', AttributeValueList: [ item.a ] } } },
          {
            ConditionExpression: 'a <> :a',
            ExpressionAttributeValues: { ':a': item.a },
          },
          {
            ConditionExpression: '#a <> :a',
            ExpressionAttributeValues: { ':a': item.a },
            ExpressionAttributeNames: { '#a': 'a' },
          },
        ], function (putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          assertConditional(putOpts, cb)
        }, done)
      })
    })

    it('should succeed if condition is valid: LE', function (done) {
      var item = { a: { S: 'b' } }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          { Expected: { a: { ComparisonOperator: 'LE', AttributeValueList: [ { S: 'c' } ] } } },
          {
            ConditionExpression: 'a <= :a',
            ExpressionAttributeValues: { ':a': { S: 'c' } },
          },
          {
            ConditionExpression: '#a <= :a',
            ExpressionAttributeValues: { ':a': { S: 'c' } },
            ExpressionAttributeNames: { '#a': 'a' },
          },
        ], function (putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          request(opts(putOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should fail if condition is invalid: LE', function (done) {
      var item = { a: { S: 'd' } }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          { Expected: { a: { ComparisonOperator: 'LE', AttributeValueList: [ { S: 'c' } ] } } },
          {
            ConditionExpression: 'a <= :a',
            ExpressionAttributeValues: { ':a': { S: 'c' } },
          },
          {
            ConditionExpression: '#a <= :a',
            ExpressionAttributeValues: { ':a': { S: 'c' } },
            ExpressionAttributeNames: { '#a': 'a' },
          },
        ], function (putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          assertConditional(putOpts, cb)
        }, done)
      })
    })

    it('should succeed if condition is valid: LT', function (done) {
      var item = { a: { S: 'b' } }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          { Expected: { a: { ComparisonOperator: 'LT', AttributeValueList: [ { S: 'c' } ] } } },
          {
            ConditionExpression: 'a < :a',
            ExpressionAttributeValues: { ':a': { S: 'c' } },
          },
          {
            ConditionExpression: '#a < :a',
            ExpressionAttributeValues: { ':a': { S: 'c' } },
            ExpressionAttributeNames: { '#a': 'a' },
          },
        ], function (putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          request(opts(putOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should fail if condition is invalid: LT', function (done) {
      var item = { a: { S: 'd' } }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          { Expected: { a: { ComparisonOperator: 'LT', AttributeValueList: [ { S: 'c' } ] } } },
          {
            ConditionExpression: 'a < :a',
            ExpressionAttributeValues: { ':a': { S: 'c' } },
          },
          {
            ConditionExpression: '#a < :a',
            ExpressionAttributeValues: { ':a': { S: 'c' } },
            ExpressionAttributeNames: { '#a': 'a' },
          },
        ], function (putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          assertConditional(putOpts, cb)
        }, done)
      })
    })

    it('should succeed if condition is valid: GE', function (done) {
      var item = { a: { S: 'b' } }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          { Expected: { a: { ComparisonOperator: 'GE', AttributeValueList: [ { S: 'a' } ] } } },
          {
            ConditionExpression: 'a >= :a',
            ExpressionAttributeValues: { ':a': { S: 'a' } },
          },
          {
            ConditionExpression: '#a >= :a',
            ExpressionAttributeValues: { ':a': { S: 'a' } },
            ExpressionAttributeNames: { '#a': 'a' },
          },
        ], function (putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          request(opts(putOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should fail if condition is invalid: GE', function (done) {
      var item = { a: { S: 'b' } }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          { Expected: { a: { ComparisonOperator: 'GE', AttributeValueList: [ { S: 'c' } ] } } },
          {
            ConditionExpression: 'a >= :a',
            ExpressionAttributeValues: { ':a': { S: 'c' } },
          },
          {
            ConditionExpression: '#a >= :a',
            ExpressionAttributeValues: { ':a': { S: 'c' } },
            ExpressionAttributeNames: { '#a': 'a' },
          },
        ], function (putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          assertConditional(putOpts, cb)
        }, done)
      })
    })

    it('should succeed if condition is valid: GT', function (done) {
      var item = { a: { S: 'b' } }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          { Expected: { a: { ComparisonOperator: 'GT', AttributeValueList: [ { S: 'a' } ] } } },
          {
            ConditionExpression: 'a > :a',
            ExpressionAttributeValues: { ':a': { S: 'a' } },
          },
          {
            ConditionExpression: '#a > :a',
            ExpressionAttributeValues: { ':a': { S: 'a' } },
            ExpressionAttributeNames: { '#a': 'a' },
          },
        ], function (putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          request(opts(putOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should fail if condition is invalid: GT', function (done) {
      var item = { a: { S: 'a' } }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          { Expected: { a: { ComparisonOperator: 'GT', AttributeValueList: [ { S: 'c' } ] } } },
          {
            ConditionExpression: 'a > :a',
            ExpressionAttributeValues: { ':a': { S: 'c' } },
          },
          {
            ConditionExpression: '#a > :a',
            ExpressionAttributeValues: { ':a': { S: 'c' } },
            ExpressionAttributeNames: { '#a': 'a' },
          },
        ], function (putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          assertConditional(putOpts, cb)
        }, done)
      })
    })

    it('should succeed if condition is valid: CONTAINS', function (done) {
      var item = { a: { S: 'hello' } }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          { Expected: { a: { ComparisonOperator: 'CONTAINS', AttributeValueList: [ { S: 'ell' } ] } } },
          {
            ConditionExpression: 'contains(a, :a)',
            ExpressionAttributeValues: { ':a': { S: 'ell' } },
          },
          {
            ConditionExpression: 'contains(#a, :a)',
            ExpressionAttributeValues: { ':a': { S: 'ell' } },
            ExpressionAttributeNames: { '#a': 'a' },
          },
        ], function (putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          request(opts(putOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should fail if condition is invalid: CONTAINS', function (done) {
      var item = { a: { S: 'hello' } }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          { Expected: { a: { ComparisonOperator: 'CONTAINS', AttributeValueList: [ { S: 'goodbye' } ] } } },
          {
            ConditionExpression: 'contains(a, :a)',
            ExpressionAttributeValues: { ':a': { S: 'goodbye' } },
          },
          {
            ConditionExpression: 'contains(#a, :a)',
            ExpressionAttributeValues: { ':a': { S: 'goodbye' } },
            ExpressionAttributeNames: { '#a': 'a' },
          },
        ], function (putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          assertConditional(putOpts, cb)
        }, done)
      })
    })

    it('should succeed if condition is valid: BEGINS_WITH', function (done) {
      var item = { a: { S: 'hello' } }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          { Expected: { a: { ComparisonOperator: 'BEGINS_WITH', AttributeValueList: [ { S: 'he' } ] } } },
          {
            ConditionExpression: 'begins_with(a, :a)',
            ExpressionAttributeValues: { ':a': { S: 'he' } },
          },
          {
            ConditionExpression: 'begins_with(#a, :a)',
            ExpressionAttributeValues: { ':a': { S: 'he' } },
            ExpressionAttributeNames: { '#a': 'a' },
          },
        ], function (putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          request(opts(putOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should fail if condition is invalid: BEGINS_WITH', function (done) {
      var item = { a: { S: 'hello' } }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          { Expected: { a: { ComparisonOperator: 'BEGINS_WITH', AttributeValueList: [ { S: 'goodbye' } ] } } },
          {
            ConditionExpression: 'begins_with(a, :a)',
            ExpressionAttributeValues: { ':a': { S: 'goodbye' } },
          },
          {
            ConditionExpression: 'begins_with(#a, :a)',
            ExpressionAttributeValues: { ':a': { S: 'goodbye' } },
            ExpressionAttributeNames: { '#a': 'a' },
          },
        ], function (putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          assertConditional(putOpts, cb)
        }, done)
      })
    })

    it('should succeed if condition is valid: NOT_CONTAINS', function (done) {
      var item = { a: { S: 'hello' } }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          { Expected: { a: { ComparisonOperator: 'NOT_CONTAINS', AttributeValueList: [ { S: 'goodbye' } ] } } },
          {
            ConditionExpression: 'not contains(a, :a)',
            ExpressionAttributeValues: { ':a': { S: 'goodbye' } },
          },
          {
            ConditionExpression: 'not contains(#a, :a)',
            ExpressionAttributeValues: { ':a': { S: 'goodbye' } },
            ExpressionAttributeNames: { '#a': 'a' },
          },
        ], function (putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          request(opts(putOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should fail if condition is invalid: NOT_CONTAINS', function (done) {
      var item = { a: { S: 'hello' } }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          { Expected: { a: { ComparisonOperator: 'NOT_CONTAINS', AttributeValueList: [ { S: 'ell' } ] } } },
          {
            ConditionExpression: 'not contains(a, :a)',
            ExpressionAttributeValues: { ':a': { S: 'ell' } },
          },
          {
            ConditionExpression: 'not contains(#a, :a)',
            ExpressionAttributeValues: { ':a': { S: 'ell' } },
            ExpressionAttributeNames: { '#a': 'a' },
          },
        ], function (putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          assertConditional(putOpts, cb)
        }, done)
      })
    })

    it('should succeed if condition is valid: IN', function (done) {
      var item = { a: { S: 'b' } }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          { Expected: { a: { ComparisonOperator: 'IN', AttributeValueList: [ { S: 'c' }, { S: 'b' } ] } } },
          {
            ConditionExpression: 'a in (:a, :b)',
            ExpressionAttributeValues: { ':a': { S: 'c' }, ':b': { S: 'b' } },
          },
          {
            ConditionExpression: '#a in (:a, :b)',
            ExpressionAttributeValues: { ':a': { S: 'c' }, ':b': { S: 'b' } },
            ExpressionAttributeNames: { '#a': 'a' },
          },
        ], function (putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          request(opts(putOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should fail if condition is invalid: IN', function (done) {
      var item = { a: { S: 'd' } }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          { Expected: { a: { ComparisonOperator: 'IN', AttributeValueList: [ { S: 'c' } ] } } },
          {
            ConditionExpression: 'a in (:a)',
            ExpressionAttributeValues: { ':a': { S: 'c' } },
          },
          {
            ConditionExpression: '#a in (:a)',
            ExpressionAttributeValues: { ':a': { S: 'c' } },
            ExpressionAttributeNames: { '#a': 'a' },
          },
        ], function (putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          assertConditional(putOpts, cb)
        }, done)
      })
    })

    it('should succeed if condition is valid: BETWEEN', function (done) {
      var item = { a: { S: 'b' } }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          { Expected: { a: { ComparisonOperator: 'BETWEEN', AttributeValueList: [ { S: 'a' }, { S: 'c' } ] } } },
          {
            ConditionExpression: 'a between :a and :b',
            ExpressionAttributeValues: { ':a': { S: 'a' }, ':b': { S: 'c' } },
          },
          {
            ConditionExpression: '#a between :a and :b',
            ExpressionAttributeValues: { ':a': { S: 'a' }, ':b': { S: 'c' } },
            ExpressionAttributeNames: { '#a': 'a' },
          },
        ], function (putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          request(opts(putOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should fail if condition is invalid: BETWEEN', function (done) {
      var item = { a: { S: 'b' } }
      request(opts({ TableName: helpers.testHashTable, Item: item }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          { Expected: { a: { ComparisonOperator: 'BETWEEN', AttributeValueList: [ { S: 'c' }, { S: 'd' } ] } } },
          {
            ConditionExpression: 'a between :a and :b',
            ExpressionAttributeValues: { ':a': { S: 'c' }, ':b': { S: 'd' } },
          },
          {
            ConditionExpression: '#a between :a and :b',
            ExpressionAttributeValues: { ':a': { S: 'c' }, ':b': { S: 'd' } },
            ExpressionAttributeNames: { '#a': 'a' },
          },
        ], function (putOpts, cb) {
          putOpts.TableName = helpers.testHashTable
          putOpts.Item = item
          assertConditional(putOpts, cb)
        }, done)
      })
    })

    it('should return ConsumedCapacity for small item', function (done) {
      var a = helpers.randomString(), b = new Array(1010 - a.length).join('b'),
        item = { a: { S: a }, b: { S: b }, c: { N: '12.3456' }, d: { B: 'AQI=' }, e: { BS: [ 'AQI=', 'Ag==', 'AQ==' ] } },
        req = { TableName: helpers.testHashTable, Item: item, ReturnConsumedCapacity: 'TOTAL' }
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

    it('should return ConsumedCapacity for larger item', function (done) {
      var a = helpers.randomString(), b = new Array(1012 - a.length).join('b'),
        item = { a: { S: a }, b: { S: b }, c: { N: '12.3456' }, d: { B: 'AQI=' }, e: { BS: [ 'AQI=', 'Ag==' ] } },
        req = { TableName: helpers.testHashTable, Item: item, ReturnConsumedCapacity: 'TOTAL' }
      request(opts(req), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({ ConsumedCapacity: { CapacityUnits: 2, TableName: helpers.testHashTable } })
        req.ReturnConsumedCapacity = 'INDEXES'
        request(opts(req), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ ConsumedCapacity: { CapacityUnits: 2, Table: { CapacityUnits: 2 }, TableName: helpers.testHashTable } })
          req.Item = { a: item.a }
          request(opts(req), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({ ConsumedCapacity: { CapacityUnits: 2, Table: { CapacityUnits: 2 }, TableName: helpers.testHashTable } })
            request(opts(req), function (err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)
              res.body.should.eql({ ConsumedCapacity: { CapacityUnits: 1, Table: { CapacityUnits: 1 }, TableName: helpers.testHashTable } })
              done()
            })
          })
        })
      })
    })

  })
})