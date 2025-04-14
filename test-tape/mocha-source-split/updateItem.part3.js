var async = require('async'),
  helpers = require('./helpers')

var target = 'UpdateItem',
  request = helpers.request,
  randomName = helpers.randomName,
  opts = helpers.opts.bind(null, target),
  assertType = helpers.assertType.bind(null, target),
  assertValidation = helpers.assertValidation.bind(null, target),
  assertNotFound = helpers.assertNotFound.bind(null, target),
  assertConditional = helpers.assertConditional.bind(null, target)

describe('updateItem', function () {
  describe('functionality', function () {
    it('should return ConditionalCheckFailedException if expecting non-existent key to exist', function (done) {
      async.forEach([
        { Expected: { a: { Value: { S: helpers.randomString() } } } },
        { Expected: { a: { ComparisonOperator: 'NOT_NULL' } } },
        { ConditionExpression: 'a = :a', ExpressionAttributeValues: { ':a': { S: helpers.randomString() } } },
        { ConditionExpression: '#a = :a', ExpressionAttributeNames: { '#a': 'a' }, ExpressionAttributeValues: { ':a': { S: helpers.randomString() } } },
        { ConditionExpression: 'attribute_exists(a)' },
        { ConditionExpression: 'attribute_exists(#a)', ExpressionAttributeNames: { '#a': 'a' } },
      ], function (updateOpts, cb) {
        updateOpts.TableName = helpers.testHashTable
        updateOpts.Key = { a: { S: helpers.randomString() } }
        assertConditional(updateOpts, cb)
      }, done)
    })

    it('should just add item with key if no action', function (done) {
      var key = { a: { S: helpers.randomString() } }
      request(opts({ TableName: helpers.testHashTable, Key: key }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({})
        request(helpers.opts('GetItem', { TableName: helpers.testHashTable, Key: key, ConsistentRead: true }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ Item: key })
          done()
        })
      })
    })

    it('should return empty when there are no old values', function (done) {
      var key = { a: { S: helpers.randomString() } }
      request(opts({ TableName: helpers.testHashTable, Key: key, ReturnValues: 'ALL_OLD' }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({})
        done()
      })
    })

    it('should return all old values when they exist', function (done) {
      var key = { a: { S: helpers.randomString() } }, updates = { b: { Value: { S: 'a' } } }
      request(opts({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        updates.b.Value.S = 'b'
        request(opts({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates, ReturnValues: 'ALL_OLD' }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ Attributes: { a: key.a, b: { S: 'a' } } })
          done()
        })
      })
    })

    it('should return updated old values when they exist', function (done) {
      var key = { a: { S: helpers.randomString() } }, updates = { b: { Value: { S: 'a' } }, c: { Value: { S: 'a' } } }
      request(opts({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        updates.b.Value.S = 'b'
        request(opts({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates, ReturnValues: 'UPDATED_OLD' }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ Attributes: { b: { S: 'a' }, c: { S: 'a' } } })
          done()
        })
      })
    })

    it('should return updated old nested values when they exist', function (done) {
      var key = { a: { S: helpers.randomString() } }, updates = {
        b: { Value: { M: { a: { S: 'a' }, b: { L: [] } } } },
        c: { Value: { N: '1' } },
      }
      request(opts({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        updates.b.Value.M.a.S = 'b'
        updates.c.Action = 'ADD'
        request(opts({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates, ReturnValues: 'UPDATED_OLD' }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ Attributes: { b: { M: { a: { S: 'a' }, b: { L: [] } } }, c: { N: '1' } } })
          done()
        })
      })
    })

    it('should return all new values when they exist', function (done) {
      var key = { a: { S: helpers.randomString() } }, updates = { b: { Value: { S: 'a' } } }
      request(opts({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        updates.b.Value.S = 'b'
        request(opts({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates, ReturnValues: 'ALL_NEW' }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ Attributes: { a: key.a, b: { S: 'b' } } })
          done()
        })
      })
    })

    it('should return updated new values when they exist', function (done) {
      var key = { a: { S: helpers.randomString() } }, updates = { b: { Value: { S: 'a' } }, c: { Value: { S: 'a' } } }
      request(opts({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({
          TableName: helpers.testHashTable,
          Key: key,
          UpdateExpression: 'set b=:b,c=:c',
          ExpressionAttributeValues: { ':b': { S: 'b' }, ':c': { S: 'a' } },
          ReturnValues: 'UPDATED_NEW',
        }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ Attributes: { b: { S: 'b' }, c: { S: 'a' } } })
          done()
        })
      })
    })

    it('should just add valid ADD actions if nothing exists', function (done) {
      async.forEach([ {
        AttributeUpdates: {
          b: { Action: 'DELETE' },
          c: { Action: 'DELETE', Value: { SS: [ 'a', 'b' ] } },
          d: { Action: 'ADD', Value: { N: '5' } },
          e: { Action: 'ADD', Value: { SS: [ 'a', 'b' ] } },
          f: { Action: 'ADD', Value: { L: [ { S: 'a' }, { N: '1' } ] } },
        },
      }, {
        UpdateExpression: 'REMOVE b DELETE c :c ADD d :d, e :e SET f = :f',
        ExpressionAttributeValues: { ':c': { SS: [ 'a', 'b' ] }, ':d': { N: '5' }, ':e': { SS: [ 'a', 'b' ] }, ':f': { L: [ { S: 'a' }, { N: '1' } ] } },
      }, {
        UpdateExpression: 'ADD #e :e,#d :d DELETE #c :c REMOVE #b SET #f = :f',
        ExpressionAttributeValues: { ':c': { SS: [ 'a', 'b' ] }, ':d': { N: '5' }, ':e': { SS: [ 'a', 'b' ] }, ':f': { L: [ { S: 'a' }, { N: '1' } ] } },
        ExpressionAttributeNames: { '#b': 'b', '#c': 'c', '#d': 'd', '#e': 'e', '#f': 'f' },
      } ], function (updateOpts, cb) {
        var key = { a: { S: helpers.randomString() } }
        updateOpts.TableName = helpers.testHashTable
        updateOpts.Key = key
        updateOpts.ReturnValues = 'UPDATED_NEW'
        request(opts(updateOpts), function (err, res) {
          if (err) return cb(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ Attributes: { d: { N: '5' }, e: { SS: [ 'a', 'b' ] }, f: { L: [ { S: 'a' }, { N: '1' } ] } } })
          request(helpers.opts('GetItem', { TableName: helpers.testHashTable, Key: key, ConsistentRead: true }), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({ Item: { a: key.a, d: { N: '5' }, e: { SS: [ 'a', 'b' ] }, f: { L: [ { S: 'a' }, { N: '1' } ] } } })
            cb()
          })
        })
      }, done)
    })

    it('should delete normal values and return updated new', function (done) {
      var key = { a: { S: helpers.randomString() } }, updates = { b: { Value: { S: 'a' } }, c: { Value: { S: 'a' } } }
      request(opts({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        updates.b = { Action: 'DELETE' }
        request(opts({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates, ReturnValues: 'UPDATED_NEW' }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ Attributes: { c: { S: 'a' } } })
          request(helpers.opts('GetItem', { TableName: helpers.testHashTable, Key: key, ConsistentRead: true }), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({ Item: { a: key.a, c: { S: 'a' } } })
            done()
          })
        })
      })
    })

    it('should delete normal values and return updated on index table', function (done) {
      var key = { a: { S: helpers.randomString() }, b: { S: helpers.randomString() } }, updates = { c: { Value: { S: 'a' } }, d: { Value: { S: 'a' } } }
      request(opts({ TableName: helpers.testRangeTable, Key: key, AttributeUpdates: updates }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        updates.c = { Action: 'DELETE' }
        request(opts({ TableName: helpers.testRangeTable, Key: key, AttributeUpdates: updates, ReturnValues: 'UPDATED_NEW' }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ Attributes: { d: { S: 'a' } } })
          request(helpers.opts('GetItem', { TableName: helpers.testRangeTable, Key: key, ConsistentRead: true }), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({ Item: { a: key.a, b: key.b, d: { S: 'a' } } })
            done()
          })
        })
      })
    })

    it('should delete set values and return updated new', function (done) {
      var key = { a: { S: helpers.randomString() } }, updates = { b: { Value: { NS: [ '1', '2', '3' ] } }, c: { Value: { S: 'a' } } }
      request(opts({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        updates.b = { Action: 'DELETE', Value: { NS: [ '1', '4' ] } }
        request(opts({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates, ReturnValues: 'UPDATED_NEW' }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Attributes.b.NS.should.containEql('2')
          res.body.Attributes.b.NS.should.containEql('3')
          res.body.Attributes.c.should.eql({ S: 'a' })
          request(helpers.opts('GetItem', { TableName: helpers.testHashTable, Key: key, ConsistentRead: true }), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.Item.b.NS.should.containEql('2')
            res.body.Item.b.NS.should.containEql('3')
            res.body.Item.c.should.eql({ S: 'a' })
            updates.b = { Action: 'DELETE', Value: { NS: [ '2', '3' ] } }
            request(opts({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates, ReturnValues: 'UPDATED_NEW' }), function (err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)
              res.body.Attributes.should.eql({ c: { S: 'a' } })
              request(helpers.opts('GetItem', { TableName: helpers.testHashTable, Key: key, ConsistentRead: true }), function (err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)
                res.body.Item.should.eql({ a: key.a, c: { S: 'a' } })
                done()
              })
            })
          })
        })
      })
    })

    it('should add numerical value and return updated new', function (done) {
      var key = { a: { S: helpers.randomString() } }, updates = { b: { Value: { N: '1' } } }
      request(opts({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        updates.b = { Action: 'ADD', Value: { N: '3' } }
        request(opts({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates, ReturnValues: 'UPDATED_NEW' }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ Attributes: { b: { N: '4' } } })
          request(helpers.opts('GetItem', { TableName: helpers.testHashTable, Key: key, ConsistentRead: true }), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({ Item: { a: key.a, b: { N: '4' } } })
            done()
          })
        })
      })
    })

    it('should add set value and return updated new', function (done) {
      var key = { a: { S: helpers.randomString() } }, updates = { b: { Value: { SS: [ 'a', 'b' ] } } }
      request(opts({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        updates.b = { Action: 'ADD', Value: { SS: [ 'c', 'd' ] } }
        request(opts({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates, ReturnValues: 'UPDATED_NEW' }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ Attributes: { b: { SS: [ 'a', 'b', 'c', 'd' ] } } })
          request(helpers.opts('GetItem', { TableName: helpers.testHashTable, Key: key, ConsistentRead: true }), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({ Item: { a: key.a, b: { SS: [ 'a', 'b', 'c', 'd' ] } } })
            done()
          })
        })
      })
    })

    it('should add list value and return updated new', function (done) {
      var key = { a: { S: helpers.randomString() } }, updates = { b: { Value: { L: [ { S: 'a' }, { N: '1' } ] } } }
      request(opts({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        updates.b = { Action: 'ADD', Value: { L: [ { S: 'b' }, { N: '2' } ] } }
        request(opts({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates, ReturnValues: 'UPDATED_NEW' }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ Attributes: { b: { L: [ { S: 'a' }, { N: '1' }, { S: 'b' }, { N: '2' } ] } } })
          request(helpers.opts('GetItem', { TableName: helpers.testHashTable, Key: key, ConsistentRead: true }), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({ Item: { a: key.a, b: { L: [ { S: 'a' }, { N: '1' }, { S: 'b' }, { N: '2' } ] } } })
            done()
          })
        })
      })
    })

    it('should throw away duplicate string values', function (done) {
      var key = { a: { S: helpers.randomString() } }, updates = { b: { Value: { SS: [ 'a', 'b' ] } } }
      request(opts({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        updates.b = { Action: 'ADD', Value: { SS: [ 'b', 'c', 'd' ] } }
        request(opts({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates, ReturnValues: 'UPDATED_NEW' }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Attributes.b.SS.should.have.lengthOf(4)
          res.body.Attributes.b.SS.should.containEql('a')
          res.body.Attributes.b.SS.should.containEql('b')
          res.body.Attributes.b.SS.should.containEql('c')
          res.body.Attributes.b.SS.should.containEql('d')
          request(helpers.opts('GetItem', { TableName: helpers.testHashTable, Key: key, ConsistentRead: true }), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.Item.b.SS.should.have.lengthOf(4)
            res.body.Item.b.SS.should.containEql('a')
            res.body.Item.b.SS.should.containEql('b')
            res.body.Item.b.SS.should.containEql('c')
            res.body.Item.b.SS.should.containEql('d')
            done()
          })
        })
      })
    })

    it('should throw away duplicate numeric values', function (done) {
      var key = { a: { S: helpers.randomString() } }, updates = { b: { Value: { NS: [ '1', '2' ] } } }
      request(opts({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        updates.b = { Action: 'ADD', Value: { NS: [ '2', '3', '4' ] } }
        request(opts({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates, ReturnValues: 'UPDATED_NEW' }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Attributes.b.NS.should.have.lengthOf(4)
          res.body.Attributes.b.NS.should.containEql('1')
          res.body.Attributes.b.NS.should.containEql('2')
          res.body.Attributes.b.NS.should.containEql('3')
          res.body.Attributes.b.NS.should.containEql('4')
          request(helpers.opts('GetItem', { TableName: helpers.testHashTable, Key: key, ConsistentRead: true }), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.Item.b.NS.should.have.lengthOf(4)
            res.body.Item.b.NS.should.containEql('1')
            res.body.Item.b.NS.should.containEql('2')
            res.body.Item.b.NS.should.containEql('3')
            res.body.Item.b.NS.should.containEql('4')
            done()
          })
        })
      })
    })

    it('should throw away duplicate binary values', function (done) {
      var key = { a: { S: helpers.randomString() } }, updates = { b: { Value: { BS: [ 'AQI=', 'Ag==' ] } } }
      request(opts({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        updates.b = { Action: 'ADD', Value: { BS: [ 'Ag==', 'AQ==' ] } }
        request(opts({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates, ReturnValues: 'UPDATED_NEW' }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Attributes.b.BS.should.have.lengthOf(3)
          res.body.Attributes.b.BS.should.containEql('AQI=')
          res.body.Attributes.b.BS.should.containEql('Ag==')
          res.body.Attributes.b.BS.should.containEql('AQ==')
          request(helpers.opts('GetItem', { TableName: helpers.testHashTable, Key: key, ConsistentRead: true }), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.Item.b.BS.should.have.lengthOf(3)
            res.body.Item.b.BS.should.containEql('AQI=')
            res.body.Item.b.BS.should.containEql('Ag==')
            res.body.Item.b.BS.should.containEql('AQ==')
            done()
          })
        })
      })
    })

    it('should return ConsumedCapacity for creating small item', function (done) {
      var key = { a: { S: helpers.randomString() } }, b = new Array(1010 - key.a.S.length).join('b'),
        updates = { b: { Value: { S: b } }, c: { Value: { N: '12.3456' } }, d: { Value: { B: 'AQI=' } }, e: { Value: { BS: [ 'AQI=', 'Ag==', 'AQ==' ] } } },
        req = { TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates, ReturnConsumedCapacity: 'TOTAL' }
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

    it('should return ConsumedCapacity for creating larger item', function (done) {
      var key = { a: { S: helpers.randomString() } }, b = new Array(1012 - key.a.S.length).join('b'),
        updates = { b: { Value: { S: b } }, c: { Value: { N: '12.3456' } }, d: { Value: { B: 'AQI=' } }, e: { Value: { BS: [ 'AQI=', 'Ag==' ] } } },
        req = { TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates, ReturnConsumedCapacity: 'TOTAL' }
      request(opts(req), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({ ConsumedCapacity: { CapacityUnits: 2, TableName: helpers.testHashTable } })
        req.ReturnConsumedCapacity = 'INDEXES'
        request(opts(req), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ ConsumedCapacity: { CapacityUnits: 2, Table: { CapacityUnits: 2 }, TableName: helpers.testHashTable } })
          done()
        })
      })
    })

    it('should return ConsumedCapacity for creating and updating small item', function (done) {
      var key = { a: { S: helpers.randomString() } }, b = new Array(1009 - key.a.S.length).join('b'),
        updates = { b: { Value: { S: b } }, c: { Value: { N: '12.3456' } }, d: { Value: { B: 'AQI=' } }, e: { Value: { BS: [ 'AQI=', 'Ag==', 'AQ==' ] } } }
      request(opts({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates, ReturnConsumedCapacity: 'TOTAL' }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({ ConsumedCapacity: { CapacityUnits: 1, TableName: helpers.testHashTable } })
        updates = { b: { Value: { S: b + 'b' } } }
        request(opts({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates, ReturnConsumedCapacity: 'TOTAL' }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ ConsumedCapacity: { CapacityUnits: 1, TableName: helpers.testHashTable } })
          done()
        })
      })
    })

    it('should return ConsumedCapacity for creating and updating larger item', function (done) {
      var key = { a: { S: helpers.randomString() } }, b = new Array(1011 - key.a.S.length).join('b'),
        updates = { b: { Value: { S: b } }, c: { Value: { N: '12.3456' } }, d: { Value: { B: 'AQI=' } }, e: { Value: { BS: [ 'AQI=', 'Ag==' ] } } }
      request(opts({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates, ReturnConsumedCapacity: 'TOTAL' }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({ ConsumedCapacity: { CapacityUnits: 1, TableName: helpers.testHashTable } })
        updates = { b: { Value: { S: b + 'b' } } }
        request(opts({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates, ReturnConsumedCapacity: 'TOTAL' }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ ConsumedCapacity: { CapacityUnits: 2, TableName: helpers.testHashTable } })
          done()
        })
      })
    })

    it('should update when boolean value expect matches', function (done) {
      async.forEach([ {
        Expected: { active: { Value: { BOOL: false }, Exists: true } },
        AttributeUpdates: { active: { Action: 'PUT', Value: { BOOL: true } } },
      }, {
        ConditionExpression: 'active = :a',
        UpdateExpression: 'SET active = :b',
        ExpressionAttributeValues: { ':a': { BOOL: false }, ':b': { BOOL: true } },
      }, {
        ConditionExpression: '#a = :a',
        UpdateExpression: 'SET #b = :b',
        ExpressionAttributeNames: { '#a': 'active', '#b': 'active' },
        ExpressionAttributeValues: { ':a': { BOOL: false }, ':b': { BOOL: true } },
      } ], function (updateOpts, cb) {
        var item = { a: { S: helpers.randomString() }, active: { BOOL: false } }
        request(helpers.opts('PutItem', { TableName: helpers.testHashTable, Item: item }), function (err, res) {
          if (err) return cb(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({})
          updateOpts.TableName = helpers.testHashTable
          updateOpts.Key = { a: item.a }
          updateOpts.ReturnValues = 'UPDATED_NEW'
          request(opts(updateOpts), function (err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({ Attributes: { active: { BOOL: true } } })
            cb()
          })
        })
      }, done)
    })

    it('should update values from other attributes', function (done) {
      var key = { a: { S: helpers.randomString() } }
      request(opts({
        TableName: helpers.testHashTable,
        Key: key,
        UpdateExpression: 'set b = if_not_exists(b, a)',
        ReturnValues: 'UPDATED_NEW',
      }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({ Attributes: { b: key.a } })
        done()
      })
    })

    it('should update nested attributes', function (done) {
      var key = { a: { S: helpers.randomString() } }
      request(opts({
        TableName: helpers.testHashTable,
        Key: key,
        UpdateExpression: 'set b = :b, c = :c',
        ExpressionAttributeValues: { ':b': { M: { a: { N: '1' }, b: { N: '2' }, c: { N: '3' } } }, ':c': { L: [ { N: '1' }, { N: '3' } ] } },
      }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({
          TableName: helpers.testHashTable,
          Key: key,
          UpdateExpression: 'set b.c=((c[1])+(b.a)),b.a = a,c[1] = a, c[4] = b.a - b.b, c[2] = b.c add c[8] :b, c[6] :a',
          ExpressionAttributeValues: { ':a': { N: '2' }, ':b': { SS: [ 'a' ] } },
          ReturnValues: 'UPDATED_NEW',
        }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ Attributes: { b: { M: { a: key.a, c: { N: '4' } } }, c: { L: [ key.a, { N: '3' }, { N: '2' } ] } } })
          request(helpers.opts('GetItem', { TableName: helpers.testHashTable, Key: key, ConsistentRead: true }), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.Item.should.eql({
              a: key.a,
              b: { M: { a: key.a, b: { N: '2' },
                c: { N: '4' } } }, c: { L: [ { N: '1' }, key.a, { N: '3' }, { N: '-1' }, { N: '2' }, { SS: [ 'a' ] } ] },
            })
            done()
          })
        })
      })
    })

    it('should update indexed attributes', function (done) {
      var key = { a: { S: helpers.randomString() }, b: { S: helpers.randomString() } }
      request(opts({
        TableName: helpers.testRangeTable,
        Key: key,
        UpdateExpression: 'set c = a, d = b, e = a, f = b',
        ReturnValues: 'UPDATED_NEW',
      }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({ Attributes: { c: key.a, d: key.b, e: key.a, f: key.b } })
        request(helpers.opts('Query', {
          TableName: helpers.testRangeTable,
          ConsistentRead: true,
          IndexName: 'index1',
          KeyConditions: {
            a: { ComparisonOperator: 'EQ', AttributeValueList: [ key.a ] },
            c: { ComparisonOperator: 'EQ', AttributeValueList: [ key.a ] },
          },
        }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Items.should.eql([ { a: key.a, b: key.b, c: key.a, d: key.b, e: key.a, f: key.b } ])
          request(helpers.opts('Query', {
            TableName: helpers.testRangeTable,
            ConsistentRead: true,
            IndexName: 'index2',
            KeyConditions: {
              a: { ComparisonOperator: 'EQ', AttributeValueList: [ key.a ] },
              d: { ComparisonOperator: 'EQ', AttributeValueList: [ key.b ] },
            },
          }), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.eql([ { a: key.a, b: key.b, c: key.a, d: key.b } ])
            request(opts({
              TableName: helpers.testRangeTable,
              Key: key,
              UpdateExpression: 'set c = b, d = a, e = b, f = a',
            }), function (err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)
              request(helpers.opts('Query', {
                TableName: helpers.testRangeTable,
                ConsistentRead: true,
                IndexName: 'index1',
                KeyConditions: {
                  a: { ComparisonOperator: 'EQ', AttributeValueList: [ key.a ] },
                  c: { ComparisonOperator: 'EQ', AttributeValueList: [ key.a ] },
                },
              }), function (err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)
                res.body.Items.should.eql([])
                request(helpers.opts('Query', {
                  TableName: helpers.testRangeTable,
                  ConsistentRead: true,
                  IndexName: 'index2',
                  KeyConditions: {
                    a: { ComparisonOperator: 'EQ', AttributeValueList: [ key.a ] },
                    d: { ComparisonOperator: 'EQ', AttributeValueList: [ key.b ] },
                  },
                }), function (err, res) {
                  if (err) return done(err)
                  res.statusCode.should.equal(200)
                  res.body.Items.should.eql([])
                  request(helpers.opts('Query', {
                    TableName: helpers.testRangeTable,
                    ConsistentRead: true,
                    IndexName: 'index1',
                    KeyConditions: {
                      a: { ComparisonOperator: 'EQ', AttributeValueList: [ key.a ] },
                      c: { ComparisonOperator: 'EQ', AttributeValueList: [ key.b ] },
                    },
                  }), function (err, res) {
                    if (err) return done(err)
                    res.statusCode.should.equal(200)
                    res.body.Items.should.eql([ { a: key.a, b: key.b, c: key.b, d: key.a, e: key.b, f: key.a } ])
                    request(helpers.opts('Query', {
                      TableName: helpers.testRangeTable,
                      ConsistentRead: true,
                      IndexName: 'index2',
                      KeyConditions: {
                        a: { ComparisonOperator: 'EQ', AttributeValueList: [ key.a ] },
                        d: { ComparisonOperator: 'EQ', AttributeValueList: [ key.a ] },
                      },
                    }), function (err, res) {
                      if (err) return done(err)
                      res.statusCode.should.equal(200)
                      res.body.Items.should.eql([ { a: key.a, b: key.b, c: key.b, d: key.a } ])
                      request(helpers.opts('Query', {
                        TableName: helpers.testRangeTable,
                        IndexName: 'index3',
                        KeyConditions: {
                          c: { ComparisonOperator: 'EQ', AttributeValueList: [ key.b ] },
                        },
                      }), function (err, res) {
                        if (err) return done(err)
                        res.statusCode.should.equal(200)
                        res.body.Items.should.eql([ { a: key.a, b: key.b, c: key.b, d: key.a, e: key.b, f: key.a } ])
                        request(helpers.opts('Query', {
                          TableName: helpers.testRangeTable,
                          IndexName: 'index4',
                          KeyConditions: {
                            c: { ComparisonOperator: 'EQ', AttributeValueList: [ key.b ] },
                            d: { ComparisonOperator: 'EQ', AttributeValueList: [ key.a ] },
                          },
                        }), function (err, res) {
                          if (err) return done(err)
                          res.statusCode.should.equal(200)
                          res.body.Items.should.eql([ { a: key.a, b: key.b, c: key.b, d: key.a, e: key.b } ])
                          done()
                        })
                      })
                    })
                  })
                })
              })
            })
          })
        })
      })
    })

  })
})