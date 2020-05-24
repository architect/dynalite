var async = require('async'),
    helpers = require('./helpers')

var target = 'BatchGetItem',
    request = helpers.request,
    randomName = helpers.randomName,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target)

describe('batchGetItem', function() {

  describe('serializations', function() {

    it('should return SerializationException when RequestItems is not a map', function(done) {
      assertType('RequestItems', 'Map<KeysAndAttributes>', done)
    })

    it('should return SerializationException when RequestItems.Attr is not a struct', function(done) {
      assertType('RequestItems.Attr', 'ValueStruct<KeysAndAttributes>', done)
    })

    it('should return SerializationException when RequestItems.Attr.Keys is not a list', function(done) {
      assertType('RequestItems.Attr.Keys', 'List', done)
    })

    it('should return SerializationException when RequestItems.Attr.Keys.0 is not a map', function(done) {
      assertType('RequestItems.Attr.Keys.0', 'ParameterizedMap', done)
    })

    it('should return SerializationException when RequestItems.Attr.Keys.0.Attr is not an attr struct', function(done) {
      this.timeout(60000)
      assertType('RequestItems.Attr.Keys.0.Attr', 'AttrStruct<ValueStruct>', done)
    })

    it('should return SerializationException when RequestItems.Attr.AttributesToGet is not a list', function(done) {
      assertType('RequestItems.Attr.AttributesToGet', 'List', done)
    })

    it('should return SerializationException when RequestItems.Attr.ConsistentRead is not a boolean', function(done) {
      assertType('RequestItems.Attr.ConsistentRead', 'Boolean', done)
    })

    it('should return SerializationException when RequestItems.Attr.ExpressionAttributeNames is not a map', function(done) {
      assertType('RequestItems.Attr.ExpressionAttributeNames', 'Map<java.lang.String>', done)
    })

    it('should return SerializationException when RequestItems.Attr.ExpressionAttributeNames.Attr is not a string', function(done) {
      assertType('RequestItems.Attr.ExpressionAttributeNames.Attr', 'String', done)
    })

    it('should return SerializationException when RequestItems.Attr.ProjectionExpression is not a string', function(done) {
      assertType('RequestItems.Attr.ProjectionExpression', 'String', done)
    })

    it('should return SerializationException when ReturnConsumedCapacity is not a string', function(done) {
      assertType('ReturnConsumedCapacity', 'String', done)
    })

  })

  describe('validations', function() {

    it('should return ValidationException for empty RequestItems', function(done) {
      assertValidation({},
        '1 validation error detected: ' +
        'Value null at \'requestItems\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for missing RequestItems', function(done) {
      assertValidation({ReturnConsumedCapacity: 'hi', ReturnItemCollectionMetrics: 'hi'}, [
        'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [INDEXES, TOTAL, NONE]',
        'Value null at \'requestItems\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for empty RequestItems', function(done) {
      assertValidation({RequestItems: {}},
        '1 validation error detected: ' +
        'Value \'{}\' at \'requestItems\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1', done)
    })

    it('should return ValidationException for short table name with no keys', function(done) {
      assertValidation({RequestItems: {a: {}}, ReturnConsumedCapacity: 'hi', ReturnItemCollectionMetrics: 'hi'}, [
        'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [INDEXES, TOTAL, NONE]',
        new RegExp('Value \'{.+}\' at \'requestItems\' ' +
          'failed to satisfy constraint: Map keys must satisfy constraint: ' +
          '\\[Member must have length less than or equal to 255, ' +
          'Member must have length greater than or equal to 3, ' +
          'Member must satisfy regular expression pattern: \\[a-zA-Z0-9_.-\\]\\+\\]'),
        'Value null at \'requestItems.a.member.keys\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for empty keys', function(done) {
      assertValidation({RequestItems: {a: {Keys: []}}}, [
        new RegExp('Value \'{.+}\' at \'requestItems\' ' +
          'failed to satisfy constraint: Map keys must satisfy constraint: ' +
          '\\[Member must have length less than or equal to 255, ' +
          'Member must have length greater than or equal to 3, ' +
          'Member must satisfy regular expression pattern: \\[a-zA-Z0-9_.-\\]\\+\\]'),
        'Value \'[]\' at \'requestItems.a.member.keys\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1',
      ], done)
    })

    it('should return ValidationException for incorrect attributes', function(done) {
      assertValidation({RequestItems: {'aa;': {}}, ReturnConsumedCapacity: 'hi'}, [
        'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [INDEXES, TOTAL, NONE]',
        new RegExp('Value \'{.+}\' at \'requestItems\' ' +
          'failed to satisfy constraint: Map keys must satisfy constraint: ' +
          '\\[Member must have length less than or equal to 255, ' +
          'Member must have length greater than or equal to 3, ' +
          'Member must satisfy regular expression pattern: \\[a-zA-Z0-9_.-\\]\\+\\]'),
        'Value null at \'requestItems.aa;.member.keys\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for short table name with keys', function(done) {
      assertValidation({RequestItems: {a: {Keys: [{a: {S: 'a'}}]}}},
        new RegExp('1 validation error detected: ' +
          'Value \'{.+}\' at \'requestItems\' ' +
          'failed to satisfy constraint: Map keys must satisfy constraint: ' +
          '\\[Member must have length less than or equal to 255, ' +
          'Member must have length greater than or equal to 3, ' +
          'Member must satisfy regular expression pattern: \\[a-zA-Z0-9_.-\\]\\+\\]'), done)
    })

    it('should return ValidationException when fetching more than 100 keys', function(done) {
      var keys = [], i
      for (i = 0; i < 101; i++) {
        keys.push({a: {S: String(i)}})
      }
      assertValidation({RequestItems: {abc: {Keys: keys}}},
        new RegExp('1 validation error detected: ' +
          'Value \'\\[.+\\]\' at \'requestItems.abc.member.keys\' failed to satisfy constraint: ' +
          'Member must have length less than or equal to 100'), done)
    })

    it('should return ValidationException if filter expression and non-expression', function(done) {
      assertValidation({
        RequestItems: {
          abc: {
            Keys: [{}],
            AttributesToGet: ['a'],
            ExpressionAttributeNames: {},
            ProjectionExpression: '',
          },
        },
      }, 'Can not use both expression and non-expression parameters in the same request: ' +
        'Non-expression parameters: {AttributesToGet} Expression parameters: {ProjectionExpression}', done)
    })

    it('should return ValidationException if ExpressionAttributeNames but no ProjectionExpression', function(done) {
      assertValidation({
        RequestItems: {
          abc: {
            Keys: [{}],
            AttributesToGet: ['a'],
            ExpressionAttributeNames: {},
          },
        },
      }, 'ExpressionAttributeNames can only be specified when using expressions', done)
    })

    it('should return ValidationException for empty ExpressionAttributeNames', function(done) {
      assertValidation({
        RequestItems: {
          abc: {
            Keys: [{}],
            ExpressionAttributeNames: {},
            ProjectionExpression: '',
          },
        },
      }, 'ExpressionAttributeNames must not be empty', done)
    })

    it('should return ValidationException for invalid ExpressionAttributeNames', function(done) {
      assertValidation({
        RequestItems: {
          abc: {
            Keys: [{}],
            ExpressionAttributeNames: {'a': 'a'},
            ProjectionExpression: '',
          },
        },
      }, 'ExpressionAttributeNames contains invalid key: Syntax error; key: "a"', done)
    })

    it('should return ValidationException for empty ProjectionExpression', function(done) {
      assertValidation({
        RequestItems: {
          abc: {
            Keys: [{}],
            ProjectionExpression: '',
          },
        },
      }, 'Invalid ProjectionExpression: The expression can not be empty;', done)
    })

    it('should return ValidationException when fetching more than 100 keys over multiple tables', function(done) {
      var keys = [], i
      for (i = 0; i < 100; i++) {
        keys.push({a: {S: String(i)}})
      }
      assertValidation({RequestItems: {abc: {Keys: keys}, abd: {Keys: [{a: {S: '100'}}]}}},
        'Too many items requested for the BatchGetItem call', done)
    })

    it('should return ResourceNotFoundException when fetching exactly 100 keys and table does not exist', function(done) {
      var keys = [], i
      for (i = 0; i < 100; i++) {
        keys.push({a: {S: String(i)}})
      }
      assertNotFound({RequestItems: {abc: {Keys: keys}}},
        'Requested resource not found', done)
    })

    it('should return ValidationException for unsupported datatype in Key', function(done) {
      async.forEach([
        {},
        {a: ''},
        {M: {a: {}}},
        {L: [{}]},
        {L: [{a: {}}]},
      ], function(expr, cb) {
        assertValidation({RequestItems: {abc: {Keys: [{a: expr}]}}},
          'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', cb)
      }, done)
    })

    it('should return ValidationException for invalid values in Key', function(done) {
      async.forEach([
        [{NULL: 'no'}, 'Null attribute value types must have the value of true'],
        [{SS: []}, 'An string set  may not be empty'],
        [{NS: []}, 'An number set  may not be empty'],
        [{BS: []}, 'Binary sets should not be empty'],
        [{SS: ['a', 'a']}, 'Input collection [a, a] contains duplicates.'],
        [{BS: ['Yg==', 'Yg==']}, 'Input collection [Yg==, Yg==]of type BS contains duplicates.'],
      ], function(expr, cb) {
        assertValidation({RequestItems: {abc: {Keys: [{a: expr[0]}]}}},
          'One or more parameter values were invalid: ' + expr[1], cb)
      }, done)
    })

    it('should return ValidationException for empty/invalid numbers in Key', function(done) {
      async.forEach([
        [{S: '', N: ''}, 'The parameter cannot be converted to a numeric value'],
        [{S: 'a', N: ''}, 'The parameter cannot be converted to a numeric value'],
        [{S: 'a', N: 'b'}, 'The parameter cannot be converted to a numeric value: b'],
        [{NS: ['1', '']}, 'The parameter cannot be converted to a numeric value'],
        [{NS: ['1', 'b']}, 'The parameter cannot be converted to a numeric value: b'],
        [{NS: ['1', '1']}, 'Input collection contains duplicates'],
        [{N: '123456789012345678901234567890123456789'}, 'Attempting to store more than 38 significant digits in a Number'],
        [{N: '-1.23456789012345678901234567890123456789'}, 'Attempting to store more than 38 significant digits in a Number'],
        [{N: '1e126'}, 'Number overflow. Attempting to store a number with magnitude larger than supported range'],
        [{N: '-1e126'}, 'Number overflow. Attempting to store a number with magnitude larger than supported range'],
        [{N: '1e-131'}, 'Number underflow. Attempting to store a number with magnitude smaller than supported range'],
        [{N: '-1e-131'}, 'Number underflow. Attempting to store a number with magnitude smaller than supported range'],
      ], function(expr, cb) {
        assertValidation({RequestItems: {abc: {Keys: [{a: expr[0]}]}}}, expr[1], cb)
      }, done)
    })

    it('should return ValidationException for multiple datatypes in Key', function(done) {
      assertValidation({RequestItems: {abc: {Keys: [{'a': {S: 'a', N: '1'}}]}}},
        'Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException for empty ExpressionAttributeNames', function(done) {
      var key = {a: {S: helpers.randomString()}, b: {N: helpers.randomNumber()}}
      assertValidation({
        RequestItems: {
          abc: {
            Keys: [key, {b: key.b, a: key.a}, key],
            ExpressionAttributeNames: {},
            ProjectionExpression: '',
          },
        },
      }, 'ExpressionAttributeNames must not be empty', done)
    })

    it('should return ValidationException for empty ProjectionExpression', function(done) {
      var key = {a: {S: helpers.randomString()}, b: {N: helpers.randomNumber()}}
      assertValidation({
        RequestItems: {
          abc: {
            Keys: [key, {b: key.b, a: key.a}, key],
            ProjectionExpression: '',
          },
        },
      }, 'Invalid ProjectionExpression: The expression can not be empty;', done)
    })

    it('should return ValidationException for duplicated keys', function(done) {
      var key = {a: {S: helpers.randomString()}, b: {N: helpers.randomNumber()}}
      assertValidation({
        RequestItems: {
          abc: {
            Keys: [key, {b: key.b, a: key.a}, key],
          },
        },
      }, 'Provided list of item keys contains duplicates', done)
    })

    it('should return ValidationException for duplicated mixed up keys', function(done) {
      var key = {a: {S: helpers.randomString()}},
          key2 = {a: {S: helpers.randomString()}}
      assertValidation({
        RequestItems: {
          abc: {
            Keys: [key, key2, key],
            AttributesToGet: ['a', 'a'],
          },
        },
      }, 'One or more parameter values were invalid: Duplicate value in attribute name: a', done)
    })

    it('should return ValidationException duplicate values in AttributesToGet', function(done) {
      assertValidation({RequestItems: {abc: {Keys: [{}], AttributesToGet: ['a', 'a']}}},
        'One or more parameter values were invalid: Duplicate value in attribute name: a', done)
    })

    it('should return ResourceNotFoundException if key is empty and table does not exist', function(done) {
      var batchReq = {RequestItems: {}}
      batchReq.RequestItems[randomName()] = {Keys: [{}]}
      assertNotFound(batchReq,
        'Requested resource not found', done)
    })

    it('should return ValidationException if key does not match schema', function(done) {
      async.forEach([
        {},
        {b: {S: 'a'}},
        {a: {S: 'a'}, b: {S: 'a'}},
        {a: {B: 'abcd'}},
        {a: {N: '1'}},
        {a: {BOOL: true}},
        {a: {NULL: true}},
        {a: {SS: ['a']}},
        {a: {NS: ['1']}},
        {a: {BS: ['aaaa']}},
        {a: {M: {}}},
        {a: {L: []}},
      ], function(expr, cb) {
        var batchReq = {RequestItems: {}}
        batchReq.RequestItems[helpers.testHashTable] = {Keys: [expr]}
        assertValidation(batchReq,
          'The provided key element does not match the schema', cb)
      }, done)
    })

    it('should return ValidationException if range key does not match schema', function(done) {
      var batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testRangeTable] = {Keys: [{a: {S: 'a'}}]}
      assertValidation(batchReq,
        'The provided key element does not match the schema', done)
    })

    it('should return ValidationException if hash key is too big', function(done) {
      var batchReq = {RequestItems: {}}, keyStr = (helpers.randomString() + new Array(2048).join('a')).slice(0, 2049)
      batchReq.RequestItems[helpers.testHashTable] = {Keys: [{a: {S: keyStr}}]}
      assertValidation(batchReq,
        'One or more parameter values were invalid: ' +
        'Size of hashkey has exceeded the maximum size limit of2048 bytes', done)
    })

    it('should return ValidationException if range key is too big', function(done) {
      var batchReq = {RequestItems: {}}, keyStr = (helpers.randomString() + new Array(1024).join('a')).slice(0, 1025)
      batchReq.RequestItems[helpers.testRangeTable] = {Keys: [{a: {S: 'a'}, b: {S: keyStr}}]}
      assertValidation(batchReq,
        'One or more parameter values were invalid: ' +
        'Aggregated size of all range keys has exceeded the size limit of 1024 bytes', done)
    })

    it('should return ResourceNotFoundException if table is being created', function(done) {
      var table = {
        TableName: randomName(),
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1},
      }
      request(helpers.opts('CreateTable', table), function(err) {
        if (err) return done(err)
        var batchReq = {RequestItems: {}}
        batchReq.RequestItems[table.TableName] = {Keys: [{a: {S: 'a'}}]}
        assertNotFound(batchReq, 'Requested resource not found', done)
        helpers.deleteWhenActive(table.TableName)
      })
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
      var item = {a: {S: helpers.randomString()}, b: {N: helpers.randomNumber()}},
          item2 = {a: {S: helpers.randomString()}, b: item.b},
          item3 = {a: {S: helpers.randomString()}, b: {N: helpers.randomNumber()}},
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
          res.body.Responses[helpers.testHashTable].should.containEql(item)
          res.body.Responses[helpers.testHashTable].should.containEql(item3)
          res.body.Responses[helpers.testHashTable].should.have.length(2)
          res.body.UnprocessedKeys.should.eql({})
          done()
        })
      })
    })

    it('should return only requested attributes of items that do exist', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {N: helpers.randomNumber()}, c: {S: 'c'}},
          item2 = {a: {S: helpers.randomString()}, b: item.b},
          item3 = {a: {S: helpers.randomString()}, b: {N: helpers.randomNumber()}},
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
        async.forEach([
          {AttributesToGet: ['b', 'c']},
          {ProjectionExpression: 'b, c'},
          {ProjectionExpression: '#b, #c', ExpressionAttributeNames: {'#b': 'b', '#c': 'c'}},
        ], function(batchOpts, cb) {
          batchReq = {RequestItems: {}}
          batchReq.RequestItems[helpers.testHashTable] = batchOpts
          batchOpts.Keys = [
            {a: item.a},
            {a: {S: helpers.randomString()}},
            {a: item3.a},
            {a: {S: helpers.randomString()}},
            {a: item4.a},
          ]
          batchOpts.ConsistentRead = true
          request(opts(batchReq), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Responses[helpers.testHashTable].should.containEql({b: item.b, c: item.c})
            res.body.Responses[helpers.testHashTable].should.containEql({b: item3.b})
            res.body.Responses[helpers.testHashTable].should.containEql({})
            res.body.Responses[helpers.testHashTable].should.have.length(3)
            res.body.UnprocessedKeys.should.eql({})
            cb()
          })
        }, done)
      })
    })

    it('should return ConsumedCapacity from each specified table with no consistent read and small item', function(done) {
      var a = helpers.randomString(), b = new Array(4082 - a.length).join('b'),
          item = {a: {S: a}, b: {S: b}, c: {N: '12.3456'}, d: {B: 'AQI='}, e: {BS: ['AQI=', 'Ag==', 'AQ==']}},
          item2 = {a: {S: helpers.randomString()}},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [{PutRequest: {Item: item}}, {PutRequest: {Item: item2}}]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        batchReq = {RequestItems: {}, ReturnConsumedCapacity: 'TOTAL'}
        batchReq.RequestItems[helpers.testHashTable] = {Keys: [{a: item.a}, {a: item2.a}, {a: {S: helpers.randomString()}}]}
        batchReq.RequestItems[helpers.testHashNTable] = {Keys: [{a: {N: helpers.randomNumber()}}]}
        request(opts(batchReq), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.ConsumedCapacity.should.containEql({CapacityUnits: 1.5, TableName: helpers.testHashTable})
          res.body.ConsumedCapacity.should.containEql({CapacityUnits: 0.5, TableName: helpers.testHashNTable})
          res.body.Responses[helpers.testHashTable].should.have.length(2)
          res.body.Responses[helpers.testHashNTable].should.have.length(0)
          batchReq.ReturnConsumedCapacity = 'INDEXES'
          request(opts(batchReq), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.ConsumedCapacity.should.containEql({CapacityUnits: 1.5, Table: {CapacityUnits: 1.5}, TableName: helpers.testHashTable})
            res.body.ConsumedCapacity.should.containEql({CapacityUnits: 0.5, Table: {CapacityUnits: 0.5}, TableName: helpers.testHashNTable})
            done()
          })
        })
      })
    })

    it('should return ConsumedCapacity from each specified table with no consistent read and larger item', function(done) {
      var a = helpers.randomString(), b = new Array(4084 - a.length).join('b'),
          item = {a: {S: a}, b: {S: b}, c: {N: '12.3456'}, d: {B: 'AQI='}, e: {BS: ['AQI=', 'Ag==']}},
          item2 = {a: {S: helpers.randomString()}},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [{PutRequest: {Item: item}}, {PutRequest: {Item: item2}}]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        batchReq = {RequestItems: {}, ReturnConsumedCapacity: 'TOTAL'}
        batchReq.RequestItems[helpers.testHashTable] = {Keys: [{a: item.a}, {a: item2.a}, {a: {S: helpers.randomString()}}]}
        batchReq.RequestItems[helpers.testHashNTable] = {Keys: [{a: {N: helpers.randomNumber()}}]}
        request(opts(batchReq), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.ConsumedCapacity.should.containEql({CapacityUnits: 2, TableName: helpers.testHashTable})
          res.body.ConsumedCapacity.should.containEql({CapacityUnits: 0.5, TableName: helpers.testHashNTable})
          res.body.Responses[helpers.testHashTable].should.have.length(2)
          res.body.Responses[helpers.testHashNTable].should.have.length(0)
          batchReq.ReturnConsumedCapacity = 'INDEXES'
          request(opts(batchReq), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.ConsumedCapacity.should.containEql({CapacityUnits: 2, Table: {CapacityUnits: 2}, TableName: helpers.testHashTable})
            res.body.ConsumedCapacity.should.containEql({CapacityUnits: 0.5, Table: {CapacityUnits: 0.5}, TableName: helpers.testHashNTable})
            done()
          })
        })
      })
    })

    it('should return ConsumedCapacity from each specified table with consistent read and small item', function(done) {
      var a = helpers.randomString(), b = new Array(4082 - a.length).join('b'),
          item = {a: {S: a}, b: {S: b}, c: {N: '12.3456'}, d: {B: 'AQI='}, e: {BS: ['AQI=', 'Ag==', 'AQ==']}},
          item2 = {a: {S: helpers.randomString()}},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [{PutRequest: {Item: item}}, {PutRequest: {Item: item2}}]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        batchReq = {RequestItems: {}, ReturnConsumedCapacity: 'TOTAL'}
        batchReq.RequestItems[helpers.testHashTable] = {Keys: [{a: item.a}, {a: item2.a}, {a: {S: helpers.randomString()}}], ConsistentRead: true}
        batchReq.RequestItems[helpers.testHashNTable] = {Keys: [{a: {N: helpers.randomNumber()}}], ConsistentRead: true}
        request(opts(batchReq), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.ConsumedCapacity.should.containEql({CapacityUnits: 3, TableName: helpers.testHashTable})
          res.body.ConsumedCapacity.should.containEql({CapacityUnits: 1, TableName: helpers.testHashNTable})
          res.body.Responses[helpers.testHashTable].should.have.length(2)
          res.body.Responses[helpers.testHashNTable].should.have.length(0)
          batchReq.ReturnConsumedCapacity = 'INDEXES'
          request(opts(batchReq), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.ConsumedCapacity.should.containEql({CapacityUnits: 3, Table: {CapacityUnits: 3}, TableName: helpers.testHashTable})
            res.body.ConsumedCapacity.should.containEql({CapacityUnits: 1, Table: {CapacityUnits: 1}, TableName: helpers.testHashNTable})
            done()
          })
        })
      })
    })

    it('should return ConsumedCapacity from each specified table with consistent read and larger item', function(done) {
      var a = helpers.randomString(), b = new Array(4084 - a.length).join('b'),
          item = {a: {S: a}, b: {S: b}, c: {N: '12.3456'}, d: {B: 'AQI='}, e: {BS: ['AQI=', 'Ag==']}},
          item2 = {a: {S: helpers.randomString()}},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [{PutRequest: {Item: item}}, {PutRequest: {Item: item2}}]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        batchReq = {RequestItems: {}, ReturnConsumedCapacity: 'TOTAL'}
        batchReq.RequestItems[helpers.testHashTable] = {Keys: [{a: item.a}, {a: item2.a}, {a: {S: helpers.randomString()}}], ConsistentRead: true}
        batchReq.RequestItems[helpers.testHashNTable] = {Keys: [{a: {N: helpers.randomNumber()}}], ConsistentRead: true}
        request(opts(batchReq), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.ConsumedCapacity.should.containEql({CapacityUnits: 4, TableName: helpers.testHashTable})
          res.body.ConsumedCapacity.should.containEql({CapacityUnits: 1, TableName: helpers.testHashNTable})
          res.body.Responses[helpers.testHashTable].should.have.length(2)
          res.body.Responses[helpers.testHashNTable].should.have.length(0)
          batchReq.ReturnConsumedCapacity = 'INDEXES'
          request(opts(batchReq), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.ConsumedCapacity.should.containEql({CapacityUnits: 4, Table: {CapacityUnits: 4}, TableName: helpers.testHashTable})
            res.body.ConsumedCapacity.should.containEql({CapacityUnits: 1, Table: {CapacityUnits: 1}, TableName: helpers.testHashNTable})
            done()
          })
        })
      })
    })

    // TODO: Need high capacity to run this (~100 runs quickly)
    it.skip('should return all items if just under limit', function(done) {
      this.timeout(200000)

      var i, item, items = [], b = new Array(helpers.MAX_SIZE - 6).join('b'),
          batchReq = {RequestItems: {}, ReturnConsumedCapacity: 'TOTAL'}
      for (i = 0; i < 4; i++) {
        if (i < 3) {
          item = {a: {S: ('0' + i).slice(-2)}, b: {S: b}}
        } else {
          item = {a: {S: ('0' + i).slice(-2)}, b: {S: b.slice(0, 229353)}, c: {N: '12.3456'}, d: {B: 'AQI='},
            e: {SS: ['a', 'bc']}, f: {NS: ['1.23', '12.3']}, g: {BS: ['AQI=', 'Ag==', 'AQ==']}}
        }
        items.push(item)
      }
      helpers.clearTable(helpers.testHashTable, 'a', function(err) {
        if (err) return done(err)
        helpers.batchWriteUntilDone(helpers.testHashTable, {puts: items}, function(err) {
          if (err) return done(err)
          batchReq.RequestItems[helpers.testHashTable] = {Keys: items.map(function(item) { return {a: item.a} }), ConsistentRead: true}
          request(opts(batchReq), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.ConsumedCapacity.should.eql([{CapacityUnits: 357, TableName: helpers.testHashTable}])
            res.body.UnprocessedKeys.should.eql({})
            res.body.Responses[helpers.testHashTable].should.have.length(4)
            helpers.clearTable(helpers.testHashTable, 'a', done)
          })
        })
      })
    })

    // TODO: Need high capacity to run this (~100 runs quickly)
    it.skip('should return an unprocessed item if just over limit', function(done) {
      this.timeout(200000)

      var i, item, items = [], b = new Array(helpers.MAX_SIZE - 6).join('b'),
          batchReq = {RequestItems: {}, ReturnConsumedCapacity: 'TOTAL'}
      for (i = 0; i < 4; i++) {
        if (i < 3) {
          item = {a: {S: ('0' + i).slice(-2)}, b: {S: b}}
        } else {
          item = {a: {S: ('0' + i).slice(-2)}, b: {S: b.slice(0, 229354)}, c: {N: '12.3456'}, d: {B: 'AQI='},
            e: {SS: ['a', 'bc']}, f: {NS: ['1.23', '12.3']}, g: {BS: ['AQI=', 'Ag==', 'AQ==']}}
        }
        items.push(item)
      }
      helpers.batchWriteUntilDone(helpers.testHashTable, {puts: items}, function(err) {
        if (err) return done(err)
        batchReq.RequestItems[helpers.testHashTable] = {Keys: items.map(function(item) { return {a: item.a} }), ConsistentRead: true}
        request(opts(batchReq), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.UnprocessedKeys[helpers.testHashTable].ConsistentRead.should.equal(true)
          res.body.UnprocessedKeys[helpers.testHashTable].Keys.should.have.length(1)
          Object.keys(res.body.UnprocessedKeys[helpers.testHashTable].Keys[0]).should.have.length(1)
          if (res.body.UnprocessedKeys[helpers.testHashTable].Keys[0].a.S == '03') {
            res.body.ConsumedCapacity.should.eql([{CapacityUnits: 301, TableName: helpers.testHashTable}])
          } else {
            res.body.UnprocessedKeys[helpers.testHashTable].Keys[0].a.S.should.be.above(-1)
            res.body.UnprocessedKeys[helpers.testHashTable].Keys[0].a.S.should.be.below(4)
            res.body.ConsumedCapacity.should.eql([{CapacityUnits: 258, TableName: helpers.testHashTable}])
          }
          res.body.Responses[helpers.testHashTable].should.have.length(3)
          helpers.clearTable(helpers.testHashTable, 'a', done)
        })
      })
    })


    // TODO: Need high capacity to run this (~100 runs quickly)
    it.skip('should return many unprocessed items if very over the limit', function(done) {
      this.timeout(200000)

      var i, item, items = [], b = new Array(helpers.MAX_SIZE - 3).join('b'),
          batchReq = {RequestItems: {}, ReturnConsumedCapacity: 'TOTAL'}
      for (i = 0; i < 20; i++) {
        if (i < 3) {
          item = {a: {S: ('0' + i).slice(-2)}, b: {S: b}}
        } else {
          item = {a: {S: ('0' + i).slice(-2)}, b: {S: b.slice(0, 20000)}}
        }
        items.push(item)
      }
      helpers.batchBulkPut(helpers.testHashTable, items, function(err) {
        if (err) return done(err)
        batchReq.RequestItems[helpers.testHashTable] = {Keys: items.map(function(item) { return {a: item.a} }), ConsistentRead: true}
        request(opts(batchReq), function(err, res) {
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

  })

})
