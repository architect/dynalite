var helpers = require('./helpers')

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

  })

})


