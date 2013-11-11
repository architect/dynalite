var async = require('async'),
    helpers = require('./helpers'),
    db = require('../db')

var target = 'BatchWriteItem',
    request = helpers.request,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target)

describe('batchWriteItem', function() {

  describe('serializations', function() {

    it('should return SerializationException when RequestItems is not a map', function(done) {
      assertType('RequestItems', 'Map', done)
    })

    it('should return SerializationException when RequestItems.Attr is not a list', function(done) {
      assertType('RequestItems.Attr', 'List', done)
    })

    it('should return SerializationException when RequestItems.Attr.0 is not a struct', function(done) {
      assertType('RequestItems.Attr.0', 'Structure', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.DeleteRequest is not a struct', function(done) {
      assertType('RequestItems.Attr.0.DeleteRequest', 'Structure', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.DeleteRequest.Key is not a map', function(done) {
      assertType('RequestItems.Attr.0.DeleteRequest.Key', 'Map', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.DeleteRequest.Key.Attr is not a struct', function(done) {
      assertType('RequestItems.Attr.0.DeleteRequest.Key.Attr', 'Structure', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.DeleteRequest.Key.Attr.S is not a string', function(done) {
      assertType('RequestItems.Attr.0.DeleteRequest.Key.Attr.S', 'String', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.DeleteRequest.Key.Attr.B is not a blob', function(done) {
      assertType('RequestItems.Attr.0.DeleteRequest.Key.Attr.B', 'Blob', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.DeleteRequest.Key.Attr.N is not a string', function(done) {
      assertType('RequestItems.Attr.0.DeleteRequest.Key.Attr.N', 'String', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.PutRequest is not a struct', function(done) {
      assertType('RequestItems.Attr.0.PutRequest', 'Structure', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.PutRequest.Item is not a map', function(done) {
      assertType('RequestItems.Attr.0.PutRequest.Item', 'Map', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.PutRequest.Item.Attr is not a struct', function(done) {
      assertType('RequestItems.Attr.0.PutRequest.Item.Attr', 'Structure', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.PutRequest.Item.Attr.S is not a string', function(done) {
      assertType('RequestItems.Attr.0.PutRequest.Item.Attr.S', 'String', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.PutRequest.Item.Attr.B is not a blob', function(done) {
      assertType('RequestItems.Attr.0.PutRequest.Item.Attr.B', 'Blob', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.PutRequest.Item.Attr.N is not a string', function(done) {
      assertType('RequestItems.Attr.0.PutRequest.Item.Attr.N', 'String', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.PutRequest.Item.Attr.SS is not a list', function(done) {
      assertType('RequestItems.Attr.0.PutRequest.Item.Attr.SS', 'List', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.PutRequest.Item.Attr.SS.0 is not a string', function(done) {
      assertType('RequestItems.Attr.0.PutRequest.Item.Attr.SS.0', 'String', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.PutRequest.Item.Attr.NS is not a list', function(done) {
      assertType('RequestItems.Attr.0.PutRequest.Item.Attr.NS', 'List', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.PutRequest.Item.Attr.NS.0 is not a string', function(done) {
      assertType('RequestItems.Attr.0.PutRequest.Item.Attr.NS.0', 'String', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.PutRequest.Item.Attr.BS is not a list', function(done) {
      assertType('RequestItems.Attr.0.PutRequest.Item.Attr.BS', 'List', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.PutRequest.Item.Attr.BS.0 is not a blob', function(done) {
      assertType('RequestItems.Attr.0.PutRequest.Item.Attr.BS.0', 'Blob', done)
    })

    it('should return SerializationException when ReturnConsumedCapacity is not a string', function(done) {
      assertType('ReturnConsumedCapacity', 'String', done)
    })

    it('should return SerializationException when ReturnItemCollectionMetrics is not a string', function(done) {
      assertType('ReturnItemCollectionMetrics', 'String', done)
    })

  })

  describe('validations', function() {

    it('should return ValidationException for empty body', function(done) {
      assertValidation({},
        '1 validation error detected: ' +
        'Value null at \'requestItems\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for missing RequestItems', function(done) {
      assertValidation({ReturnConsumedCapacity: 'hi', ReturnItemCollectionMetrics: 'hi'},
        '3 validation errors detected: ' +
        'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [INDEXES, TOTAL, NONE]; ' +
        'Value \'hi\' at \'returnItemCollectionMetrics\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [SIZE, NONE]; ' +
        'Value null at \'requestItems\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for empty RequestItems', function(done) {
      assertValidation({RequestItems: {}},
        '1 validation error detected: ' +
        'Value \'{}\' at \'requestItems\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1', done)
    })

    it('should return ValidationException for empty list in RequestItems', function(done) {
      assertValidation({RequestItems: {a: []}},
        'The batch write request list for a table cannot be null or empty: a', done)
    })

    it('should return ValidationException for empty item in RequestItems', function(done) {
      assertValidation({RequestItems: {a: [{}]}},
        'Supplied AttributeValue has more than one datatypes set, ' +
        'must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException for short table name', function(done) {
      assertValidation({RequestItems: {a: []}, ReturnConsumedCapacity: 'hi', ReturnItemCollectionMetrics: 'hi'},
        '2 validation errors detected: ' +
        'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [INDEXES, TOTAL, NONE]; ' +
        'Value \'hi\' at \'returnItemCollectionMetrics\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [SIZE, NONE]', done)
    })

    it('should return ValidationException for incorrect attributes', function(done) {
      assertValidation({RequestItems: {'aa;': [{PutRequest: {}, DeleteRequest: {}}]},
        ReturnConsumedCapacity: 'hi', ReturnItemCollectionMetrics: 'hi'},
        '4 validation errors detected: ' +
        'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [INDEXES, TOTAL, NONE]; ' +
        'Value \'hi\' at \'returnItemCollectionMetrics\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [SIZE, NONE]; ' +
        'Value null at \'requestItems.aa;.member.1.member.deleteRequest.key\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'requestItems.aa;.member.1.member.putRequest.item\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for puts and deletes of the same item with put first', function(done) {
      var item = {a: {S: helpers.randomString()}, c: {S: 'c'}},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [{PutRequest: {Item: item}}, {DeleteRequest: {Key: {a: item.a}}}]
      assertValidation(batchReq, 'Provided list of item keys contains duplicates', done)
    })

    it('should return ValidationException for puts and deletes of the same item with delete first', function(done) {
      var item = {a: {S: helpers.randomString()}, c: {S: 'c'}},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [{DeleteRequest: {Key: {a: item.a}}}, {PutRequest: {Item: item}}]
      assertValidation(batchReq, 'Provided list of item keys contains duplicates', done)
    })

    it('should return ResourceNotFoundException for short table name', function(done) {
      assertNotFound({RequestItems: {a: [{PutRequest: {Item: {a: {S: 'a'}}}}]}}, 'Requested resource not found', done)
    })

  })

  describe('functionality', function() {

    it('should write a single item to each table', function(done) {
      var item = {a: {S: helpers.randomString()}, c: {S: 'c'}},
          item2 = {a: {S: helpers.randomString()}, b: {S: helpers.randomString()}, c: {S: 'c'}},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [{PutRequest: {Item: item}}]
      batchReq.RequestItems[helpers.testRangeTable] = [{PutRequest: {Item: item2}}]
      request(opts(batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({UnprocessedItems: {}})
        request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item.a}, ConsistentRead: true}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({Item: item})
          request(helpers.opts('GetItem', {TableName: helpers.testRangeTable, Key: {a: item2.a, b: item2.b}, ConsistentRead: true}), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({Item: item2})
            done()
          })
        })
      })
    })

    it('should delete an item from each table', function(done) {
      var item = {a: {S: helpers.randomString()}, c: {S: 'c'}},
          item2 = {a: {S: helpers.randomString()}, b: {S: helpers.randomString()}, c: {S: 'c'}},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [{DeleteRequest: {Key: {a: item.a}}}]
      batchReq.RequestItems[helpers.testRangeTable] = [{DeleteRequest: {Key: {a: item2.a, b: item2.b}}}]
      request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(helpers.opts('PutItem', {TableName: helpers.testRangeTable, Item: item2}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          request(opts(batchReq), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({UnprocessedItems: {}})
            request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item.a}, ConsistentRead: true}), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)
              res.body.should.eql({})
              request(helpers.opts('GetItem', {TableName: helpers.testRangeTable, Key: {a: item2.a, b: item2.b}, ConsistentRead: true}), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)
                res.body.should.eql({})
                done()
              })
            })
          })
        })
      })
    })

    it('should deal with puts and deletes together', function(done) {
      var item = {a: {S: helpers.randomString()}, c: {S: 'c'}},
          item2 = {a: {S: helpers.randomString()}, c: {S: 'c'}},
          batchReq = {RequestItems: {}}
      request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        batchReq.RequestItems[helpers.testHashTable] = [{DeleteRequest: {Key: {a: item.a}}}, {PutRequest: {Item: item2}}]
        request(opts(batchReq), function(err, res) {
          if (err) return done(err)
          res.body.should.eql({UnprocessedItems: {}})
          batchReq.RequestItems[helpers.testHashTable] = [{PutRequest: {Item: item}}, {DeleteRequest: {Key: {a: item2.a}}}]
          request(opts(batchReq), function(err, res) {
            if (err) return done(err)
            res.body.should.eql({UnprocessedItems: {}})
            request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item.a}, ConsistentRead: true}), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)
              res.body.should.eql({Item: item})
              request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item2.a}, ConsistentRead: true}), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)
                res.body.should.eql({})
                done()
              })
            })
          })
        })
      })
    })

    it('should return ConsumedCapacity from each specified table when putting and deleting small item', function(done) {
      var a = helpers.randomString(), b = new Array(1010 - a.length).join('b'),
          item = {a: {S: a}, b: {S: b}, c: {N: '12.3456'}, d: {B: 'AQI='}, e: {BS: ['AQI=', 'Ag==', 'AQ==']}},
          key2 = helpers.randomString(), key3 = helpers.randomString(),
          batchReq = {RequestItems: {}, ReturnConsumedCapacity: 'TOTAL'}
      batchReq.RequestItems[helpers.testHashTable] = [{PutRequest: {Item: item}}, {PutRequest: {Item: {a: {S: key2}}}}]
      batchReq.RequestItems[helpers.testHashNTable] = [{PutRequest: {Item: {a: {N: key3}}}}]
      request(opts(batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.ConsumedCapacity.should.includeEql({CapacityUnits: 2, TableName: helpers.testHashTable})
        res.body.ConsumedCapacity.should.includeEql({CapacityUnits: 1, TableName: helpers.testHashNTable})
        batchReq.RequestItems[helpers.testHashTable] = [{DeleteRequest: {Key: {a: item.a}}}, {DeleteRequest: {Key: {a: {S: key2}}}}]
        batchReq.RequestItems[helpers.testHashNTable] = [{DeleteRequest: {Key: {a: {N: key3}}}}]
        request(opts(batchReq), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.ConsumedCapacity.should.includeEql({CapacityUnits: 2, TableName: helpers.testHashTable})
          res.body.ConsumedCapacity.should.includeEql({CapacityUnits: 1, TableName: helpers.testHashNTable})
          done()
        })
      })
    })

    it('should return ConsumedCapacity from each specified table when putting and deleting larger item', function(done) {
      var a = helpers.randomString(), b = new Array(1012 - a.length).join('b'),
          item = {a: {S: a}, b: {S: b}, c: {N: '12.3456'}, d: {B: 'AQI='}, e: {BS: ['AQI=', 'Ag==']}},
          key2 = helpers.randomString(), key3 = helpers.randomString(),
          batchReq = {RequestItems: {}, ReturnConsumedCapacity: 'TOTAL'}
      batchReq.RequestItems[helpers.testHashTable] = [{PutRequest: {Item: item}}, {PutRequest: {Item: {a: {S: key2}}}}]
      batchReq.RequestItems[helpers.testHashNTable] = [{PutRequest: {Item: {a: {N: key3}}}}]
      request(opts(batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.ConsumedCapacity.should.includeEql({CapacityUnits: 3, TableName: helpers.testHashTable})
        res.body.ConsumedCapacity.should.includeEql({CapacityUnits: 1, TableName: helpers.testHashNTable})
        batchReq.RequestItems[helpers.testHashTable] = [{DeleteRequest: {Key: {a: item.a}}}, {DeleteRequest: {Key: {a: {S: key2}}}}]
        batchReq.RequestItems[helpers.testHashNTable] = [{DeleteRequest: {Key: {a: {N: key3}}}}]
        request(opts(batchReq), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.ConsumedCapacity.should.includeEql({CapacityUnits: 3, TableName: helpers.testHashTable})
          res.body.ConsumedCapacity.should.includeEql({CapacityUnits: 1, TableName: helpers.testHashNTable})
          done()
        })
      })
    })


    // TODO: Need high capacity to run this
    // All capacities seem to have a burst rate of 300x => full recovery is 300sec
    // Max size = 1638400 = 25 * 65536 = 1600 capacity units
    // Will process all if capacity >= 751. Below this value, the algorithm is something like:
    // min(capacity * 300, min(capacity, 336) + 677) + random(mean = 80, stddev = 32)
    it.skip('should return UnprocessedItems if over limit', function(done) {
      this.timeout(1e8)

      var CAPACITY = 3

      async.times(10, createAndWrite, done)

      function createAndWrite(i, cb) {
        var name = helpers.randomName(), table = {
          TableName: name,
          AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}],
          KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}],
          ProvisionedThroughput: {ReadCapacityUnits: CAPACITY, WriteCapacityUnits: CAPACITY},
        }
        helpers.createAndWait(table, function(err) {
          if (err) return cb(err)
          async.timesSeries(50, function(n, cb) { batchWrite(name, n, cb) }, cb)
        })
      }

      function batchWrite(name, n, cb) {
        var i, item, items = [], totalSize = 0, batchReq = {RequestItems: {}, ReturnConsumedCapacity: 'TOTAL'}

        for (i = 0; i < 25; i++) {
          item = {a: {S: ('0' + i).slice(-2)},
            b: {S: new Array(Math.floor((64 - (16 * Math.random())) * 1024) - 3).join('b')}}
          totalSize += db.itemSize(item)
          items.push({PutRequest: {Item: item}})
        }

        batchReq.RequestItems[name] = items
        request(opts(batchReq), function(err, res) {
          //if (err) return cb(err)
          if (err) {
            //console.log('Caught err: ' + err)
            return cb()
          }
          if (/ProvisionedThroughputExceededException$/.test(res.body.__type)) {
            //console.log('ProvisionedThroughputExceededException$')
            return cb()
          } else if (res.body.__type) {
            //return cb(new Error(JSON.stringify(res.body)))
            return cb()
          }
          res.statusCode.should.equal(200)
          console.log([CAPACITY, res.body.ConsumedCapacity[0].CapacityUnits, totalSize].join())
          setTimeout(cb, res.body.ConsumedCapacity[0].CapacityUnits * 1000 / CAPACITY)
        })
      }
    })
  })

})
