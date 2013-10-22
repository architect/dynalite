var async = require('async'),
    helpers = require('./helpers'),
    should = require('should'),
    dynalite = require('..')

var target = 'BatchGetItem',
    request = helpers.request,
    opts = helpers.opts.bind(null, target),
    assertSerialization = helpers.assertSerialization.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target)

describe('batchGetItem', function() {

  describe('serializations', function() {

    it('should return SerializationException when RequestItems is not a map', function(done) {
      assertType('RequestItems', 'Map', done)
    })

    it('should return SerializationException when RequestItems.Attr is not a struct', function(done) {
      assertType('RequestItems.Attr', 'Structure', done)
    })

    it('should return SerializationException when RequestItems.Attr.Keys is not a list', function(done) {
      assertType('RequestItems.Attr.Keys', 'List', done)
    })

    it('should return SerializationException when RequestItems.Attr.Keys.0 is not a map', function(done) {
      assertType('RequestItems.Attr.Keys.0', 'Map', done)
    })

    it('should return SerializationException when RequestItems.Attr.Keys.0.Attr is not a struct', function(done) {
      assertType('RequestItems.Attr.Keys.0.Attr', 'Structure', done)
    })

    it('should return SerializationException when RequestItems.Attr.Keys.0.Attr.S is not a string', function(done) {
      assertType('RequestItems.Attr.Keys.0.Attr.S', 'String', done)
    })

    it('should return SerializationException when RequestItems.Attr.Keys.0.Attr.B is not a blob', function(done) {
      assertType('RequestItems.Attr.Keys.0.Attr.B', 'Blob', done)
    })

    it('should return SerializationException when RequestItems.Attr.Keys.0.Attr.N is not a string', function(done) {
      assertType('RequestItems.Attr.Keys.0.Attr.N', 'String', done)
    })

    it('should return SerializationException when RequestItems.Attr.AttributesToGet is not a list', function(done) {
      assertType('RequestItems.Attr.AttributesToGet', 'List', done)
    })

    it('should return SerializationException when RequestItems.Attr.ConsistentRead is not a boolean', function(done) {
      assertType('RequestItems.Attr.ConsistentRead', 'Boolean', done)
    })

    it('should return SerializationException when ReturnConsumedCapacity is not a string', function(done) {
      assertType('ReturnConsumedCapacity', 'String', done)
    })

  })

  describe('validations', function() {

    it('should return ValidationException for missing RequestItems', function(done) {
      assertValidation({ReturnConsumedCapacity: 'hi', ReturnItemCollectionMetrics: 'hi'},
        'The requestItems parameter is required for BatchGetItem', done)
    })

    it('should return ValidationException for empty RequestItems', function(done) {
      assertValidation({RequestItems: {}},
        'The requestItems parameter is required for BatchGetItem', done)
    })

    it('should return ValidationException for short table name', function(done) {
      assertValidation({RequestItems: {a:{}}, ReturnConsumedCapacity: 'hi', ReturnItemCollectionMetrics: 'hi'},
        'TableName must be at least 3 characters long and at most 255 characters long', done)
    })

    it('should return ValidationException for incorrect attributes', function(done) {
      assertValidation({RequestItems: {'aa;': {}}, ReturnConsumedCapacity: 'hi'},
        '2 validation errors detected: ' +
        'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [TOTAL, NONE]; ' +
        'Value null at \'requestItems.aa;.member.keys\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for duplicated keys', function(done) {
      var key = {a: {S: helpers.randomString()}},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = {Keys: [key, key, key]}
      assertValidation(batchReq, 'Provided list of item keys contains duplicates', done)
    })

    it('should return ValidationException for duplicated mixed up keys', function(done) {
      var key = {a: {S: helpers.randomString()}},
          key2 = {a: {S: helpers.randomString()}},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = {Keys: [key, key2, key]}
      assertValidation(batchReq, 'Provided list of item keys contains duplicates', done)
    })
  })

  describe('functionality', function() {

    it('should return empty responses if keys do not exist', function(done) {
      var batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = {Keys: [{a: {S: helpers.randomString()}}]}
      batchReq.RequestItems[helpers.testRangeTable] = {Keys: [{a: {S: helpers.randomString()}, b: {S: helpers.randomString()}}]}
      request(opts(batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.Responses[helpers.testHashTable].should.eql([])
        res.body.Responses[helpers.testRangeTable].should.eql([])
        res.body.UnprocessedKeys.should.eql({})
        done()
      })
    })

    it('should return only items that do exist', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {N: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: item.b},
          item3 = {a: {S: helpers.randomString()}, b: {N: helpers.randomString()}},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        batchReq = {RequestItems: {}}
        batchReq.RequestItems[helpers.testHashTable] = {Keys: [
          {a: item.a},
          {a: {S: helpers.randomString()}},
          {a: item3.a},
          {a: {S: helpers.randomString()}},
        ], ConsistentRead: true}
        request(opts(batchReq), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Responses[helpers.testHashTable].should.includeEql(item)
          res.body.Responses[helpers.testHashTable].should.includeEql(item3)
          res.body.Responses[helpers.testHashTable].should.have.length(2)
          res.body.UnprocessedKeys.should.eql({})
          done()
        })
      })
    })

    it('should return only requested attributes of items that do exist', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {N: helpers.randomString()}, c: {S: 'c'}},
          item2 = {a: {S: helpers.randomString()}, b: item.b},
          item3 = {a: {S: helpers.randomString()}, b: {N: helpers.randomString()}},
          item4 = {a: {S: helpers.randomString()}},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
        {PutRequest: {Item: item4}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        batchReq = {RequestItems: {}}
        batchReq.RequestItems[helpers.testHashTable] = {Keys: [
          {a: item.a},
          {a: {S: helpers.randomString()}},
          {a: item3.a},
          {a: {S: helpers.randomString()}},
          {a: item4.a},
        ], AttributesToGet: ['b', 'c'], ConsistentRead: true}
        request(opts(batchReq), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Responses[helpers.testHashTable].should.includeEql({b: item.b, c: item.c})
          res.body.Responses[helpers.testHashTable].should.includeEql({b: item3.b})
          res.body.Responses[helpers.testHashTable].should.includeEql({})
          res.body.Responses[helpers.testHashTable].should.have.length(3)
          res.body.UnprocessedKeys.should.eql({})
          done()
        })
      })
    })

  })

})


