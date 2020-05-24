var async = require('async'),
    helpers = require('./helpers'),
    db = require('../db')

var target = 'BatchWriteItem',
    request = helpers.request,
    randomName = helpers.randomName,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target)

describe('batchWriteItem', function() {

  describe('serializations', function() {

    it('should return SerializationException when RequestItems is not a map', function(done) {
      assertType('RequestItems', 'Map<java.util.List<com.amazonaws.dynamodb.v20120810.WriteRequest>>', done)
    })

    it('should return SerializationException when RequestItems.Attr is not a list', function(done) {
      assertType('RequestItems.Attr', 'ParameterizedList', done)
    })

    it('should return SerializationException when RequestItems.Attr.0 is not a struct', function(done) {
      assertType('RequestItems.Attr.0', 'ValueStruct<WriteRequest>', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.DeleteRequest is not a struct', function(done) {
      assertType('RequestItems.Attr.0.DeleteRequest', 'FieldStruct<DeleteRequest>', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.DeleteRequest.Key is not a map', function(done) {
      assertType('RequestItems.Attr.0.DeleteRequest.Key', 'Map<AttributeValue>', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.DeleteRequest.Key.Attr is not an attr struct', function(done) {
      this.timeout(60000)
      assertType('RequestItems.Attr.0.DeleteRequest.Key.Attr', 'AttrStruct<ValueStruct>', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.PutRequest is not a struct', function(done) {
      assertType('RequestItems.Attr.0.PutRequest', 'FieldStruct<PutRequest>', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.PutRequest.Item is not a map', function(done) {
      assertType('RequestItems.Attr.0.PutRequest.Item', 'Map<AttributeValue>', done)
    })

    it('should return SerializationException when RequestItems.Attr.0.PutRequest.Item.Attr is not an attr struct', function(done) {
      this.timeout(60000)
      assertType('RequestItems.Attr.0.PutRequest.Item.Attr', 'AttrStruct<ValueStruct>', done)
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
      assertValidation({ReturnConsumedCapacity: 'hi', ReturnItemCollectionMetrics: 'hi'}, [
        'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [INDEXES, TOTAL, NONE]',
        'Value \'hi\' at \'returnItemCollectionMetrics\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [SIZE, NONE]',
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

    it('should return ValidationException for empty list in RequestItems', function(done) {
      assertValidation({RequestItems: {a: []}}, [
        new RegExp('Value \'{.+}\' at \'requestItems\' failed to satisfy constraint: ' +
          'Map keys must satisfy constraint: ' +
          '\\[Member must have length less than or equal to 255, ' +
          'Member must have length greater than or equal to 3, ' +
          'Member must satisfy regular expression pattern: \\[a-zA-Z0-9_.-\\]\\+\\]'),
        new RegExp('Value \'{.+}\' at \'requestItems\' failed to satisfy constraint: ' +
          'Map value must satisfy constraint: ' +
          '\\[Member must have length less than or equal to 25, ' +
          'Member must have length greater than or equal to 1\\]'),
      ], done)
    })

    it('should return ValidationException for empty item in RequestItems', function(done) {
      assertValidation({RequestItems: {abc: [{}]}},
        'Supplied AttributeValue has more than one datatypes set, ' +
        'must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException for short table name and missing requests', function(done) {
      assertValidation({RequestItems: {a: []}, ReturnConsumedCapacity: 'hi', ReturnItemCollectionMetrics: 'hi'}, [
        'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [INDEXES, TOTAL, NONE]',
        'Value \'hi\' at \'returnItemCollectionMetrics\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [SIZE, NONE]',
        new RegExp('Value \'{.+}\' at \'requestItems\' failed to satisfy constraint: ' +
          'Map keys must satisfy constraint: ' +
          '\\[Member must have length less than or equal to 255, ' +
          'Member must have length greater than or equal to 3, ' +
          'Member must satisfy regular expression pattern: \\[a-zA-Z0-9_.-\\]\\+\\]'),
        new RegExp('Value \'{.+}\' at \'requestItems\' failed to satisfy constraint: ' +
          'Map value must satisfy constraint: ' +
          '\\[Member must have length less than or equal to 25, ' +
          'Member must have length greater than or equal to 1\\]'),
      ], done)
    })

    it('should return ValidationException for incorrect attributes', function(done) {
      assertValidation({RequestItems: {'aa;': [{PutRequest: {}, DeleteRequest: {}}]},
        ReturnConsumedCapacity: 'hi', ReturnItemCollectionMetrics: 'hi'}, [
          'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
          'Member must satisfy enum value set: [INDEXES, TOTAL, NONE]',
          'Value \'hi\' at \'returnItemCollectionMetrics\' failed to satisfy constraint: ' +
          'Member must satisfy enum value set: [SIZE, NONE]',
          new RegExp('Value \'{.+}\' at \'requestItems\' ' +
            'failed to satisfy constraint: Map keys must satisfy constraint: ' +
            '\\[Member must have length less than or equal to 255, ' +
            'Member must have length greater than or equal to 3, ' +
            'Member must satisfy regular expression pattern: \\[a-zA-Z0-9_.-\\]\\+\\]'),
          'Value null at \'requestItems.aa;.member.1.member.deleteRequest.key\' failed to satisfy constraint: ' +
          'Member must not be null',
          'Value null at \'requestItems.aa;.member.1.member.putRequest.item\' failed to satisfy constraint: ' +
          'Member must not be null',
        ], done)
    })

    it('should return ValidationException when putting more than 25 items', function(done) {
      var requests = [], i
      for (i = 0; i < 26; i++) {
        requests.push(i % 2 ? {DeleteRequest: {Key: {a: {S: String(i)}}}} : {PutRequest: {Item: {a: {S: String(i)}}}})
      }
      assertValidation({RequestItems: {abc: requests}},
        new RegExp('1 validation error detected: ' +
          'Value \'{.+}\' at \'requestItems\' failed to satisfy constraint: ' +
          'Map value must satisfy constraint: ' +
          '\\[Member must have length less than or equal to 25, ' +
          'Member must have length greater than or equal to 1\\]'), done)
    })

    it('should return ResourceNotFoundException when fetching exactly 25 items and table does not exist', function(done) {
      var requests = [], i
      for (i = 0; i < 25; i++) {
        requests.push(i % 2 ? {DeleteRequest: {Key: {a: {S: String(i)}}}} : {PutRequest: {Item: {a: {S: String(i)}}}})
      }
      assertNotFound({RequestItems: {abc: requests}},
        'Requested resource not found', done)
    })

    it('should check table exists first before checking for duplicate keys', function(done) {
      var item = {a: {S: helpers.randomString()}, c: {S: 'c'}}
      assertNotFound({RequestItems: {abc: [{PutRequest: {Item: item}}, {DeleteRequest: {Key: {a: item.a}}}]}},
         'Requested resource not found', done)
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

    it('should return ValidationException for short table name', function(done) {
      assertValidation({RequestItems: {a: [{PutRequest: {Item: {a: {S: 'a'}}}}]}},
        new RegExp('1 validation error detected: ' +
          'Value \'{.+}\' at \'requestItems\' ' +
          'failed to satisfy constraint: ' +
          'Map keys must satisfy constraint: ' +
          '\\[Member must have length less than or equal to 255, ' +
          'Member must have length greater than or equal to 3, ' +
          'Member must satisfy regular expression pattern: \\[a-zA-Z0-9_.-\\]\\+\\]'), done)
    })

    it('should return ValidationException for unsupported datatype in Item', function(done) {
      async.forEach([
        {},
        {a: ''},
        {M: {a: {}}},
        {L: [{}]},
        {L: [{a: {}}]},
      ], function(expr, cb) {
        assertValidation({RequestItems: {abc: [{PutRequest: {Item: {a: expr}}}]}},
          'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', cb)
      }, done)
    })

    it('should return ValidationException for invalid values in Item', function(done) {
      async.forEach([
        [{NULL: 'no'}, 'Null attribute value types must have the value of true'],
        [{SS: []}, 'An string set  may not be empty'],
        [{NS: []}, 'An number set  may not be empty'],
        [{BS: []}, 'Binary sets should not be empty'],
        [{SS: ['a', 'a']}, 'Input collection [a, a] contains duplicates.'],
        [{BS: ['Yg==', 'Yg==']}, 'Input collection [Yg==, Yg==]of type BS contains duplicates.'],
      ], function(expr, cb) {
        assertValidation({RequestItems: {abc: [{PutRequest: {Item: {a: expr[0]}}}]}},
          'One or more parameter values were invalid: ' + expr[1], cb)
      }, done)
    })

    it('should return ValidationException for empty/invalid numbers in Item', function(done) {
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
        assertValidation({RequestItems: {abc: [{PutRequest: {Item: {a: expr[0]}}}]}}, expr[1], cb)
      }, done)
    })

    it('should return ValidationException for multiple datatypes in Item', function(done) {
      assertValidation({RequestItems: {abc: [{PutRequest: {Item: {a: {S: 'a', N: '1'}}}}]}},
        'Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException if item is too big with small attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1).join('a')
      assertValidation({RequestItems: {abc: [{PutRequest: {Item: {a: {S: keyStr}, b: {S: b}}}}]}},
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ResourceNotFoundException if item is just small enough with small attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 2).join('a')
      assertNotFound({RequestItems: {abc: [{PutRequest: {Item: {a: {S: keyStr}, b: {S: b}}}}]}},
        'Requested resource not found', done)
    })

    it('should return ValidationException if item is too big with larger attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 27).join('a')
      assertValidation({RequestItems: {abc: [{PutRequest: {Item: {a: {S: keyStr}, bbbbbbbbbbbbbbbbbbbbbbbbbbb: {S: b}}}}]}},
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ResourceNotFoundException if item is just small enough with larger attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 28).join('a')
      assertNotFound({RequestItems: {abc: [{PutRequest: {Item: {a: {S: keyStr}, bbbbbbbbbbbbbbbbbbbbbbbbbbb: {S: b}}}}]}},
        'Requested resource not found', done)
    })

    it('should return ValidationException if item is too big with multi attributes', function(done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 7).join('a')
      assertValidation({RequestItems: {abc: [{PutRequest: {Item: {a: {S: keyStr}, bb: {S: b}, ccc: {S: 'cc'}}}}]}},
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ResourceNotFoundException if item is just small enough with multi attributes', function(done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 8).join('a')
      assertNotFound({RequestItems: {abc: [{PutRequest: {Item: {a: {S: keyStr}, bb: {S: b}, ccc: {S: 'cc'}}}}]}},
        'Requested resource not found', done)
    })

    it('should return ValidationException if item is too big with big number attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 20).join('a'),
        c = new Array(38 + 1).join('1') + new Array(89).join('0')
      assertValidation({RequestItems: {abc: [{PutRequest: {Item: {a: {S: keyStr}, b: {S: b}, c: {N: c}}}}]}},
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ValidationException if item is too big with smallest number attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 2).join('a'),
        c = '1' + new Array(126).join('0')
      assertValidation({RequestItems: {abc: [{PutRequest: {Item: {a: {S: keyStr}, b: {S: b}, c: {N: c}}}}]}},
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ValidationException if item is too big with smaller number attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 2).join('a'),
        c = '11' + new Array(125).join('0')
      assertValidation({RequestItems: {abc: [{PutRequest: {Item: {a: {S: keyStr}, b: {S: b}, c: {N: c}}}}]}},
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ValidationException if item is too big with medium number attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 4).join('a'),
        c = '11111' + new Array(122).join('0')
      assertValidation({RequestItems: {abc: [{PutRequest: {Item: {a: {S: keyStr}, b: {S: b}, c: {N: c}}}}]}},
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ValidationException if item is too big with medium number attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 4).join('a'),
        c = '111111' + new Array(121).join('0')
      assertValidation({RequestItems: {abc: [{PutRequest: {Item: {a: {S: keyStr}, b: {S: b}, c: {N: c}}}}]}},
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ValidationException if item is too big with medium number attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 5).join('a'),
        c = '1111111' + new Array(120).join('0')
      assertValidation({RequestItems: {abc: [{PutRequest: {Item: {a: {S: keyStr}, b: {S: b}, c: {N: c}}}}]}},
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ValidationException if item is too big with multi number attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 5 - 1 - 5).join('a'),
        c = '1111111' + new Array(120).join('0'), d = '1111111' + new Array(120).join('0')
      assertValidation({RequestItems: {abc: [{PutRequest: {Item: {a: {S: keyStr}, b: {S: b}, c: {N: c}, d: {N: d}}}}]}},
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ResourceNotFoundException if item is just small enough with multi number attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(helpers.MAX_SIZE + 1 - keyStr.length - 1 - 1 - 5 - 1 - 6).join('a'),
        c = '1111111' + new Array(120).join('0'), d = '1111111' + new Array(120).join('0')
      assertNotFound({RequestItems: {abc: [{PutRequest: {Item: {a: {S: keyStr}, b: {S: b}, c: {N: c}, d: {N: d}}}}]}},
        'Requested resource not found', done)
    })

    it('should return ResourceNotFoundException if key is empty and table does not exist', function(done) {
      var batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.randomString()] = [{PutRequest: {Item: {}}}]
      assertNotFound(batchReq,
        'Requested resource not found', done)
    })

    it('should return ValidationException if key does not match schema', function(done) {
      async.forEach([
        {},
        {b: {S: 'a'}},
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
        batchReq.RequestItems[helpers.testHashTable] = [{PutRequest: {Item: expr}}]
        assertValidation(batchReq,
          'The provided key element does not match the schema', cb)
      }, done)
    })

    it('should return ValidationException if range key does not match schema', function(done) {
      var batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testRangeTable] = [{PutRequest: {Item: {a: {S: 'a'}}}}]
      assertValidation(batchReq,
        'The provided key element does not match the schema', done)
    })

    it('should return ValidationException if secondary index key is incorrect type', function(done) {
      var batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testRangeTable] = [{PutRequest: {Item: {a: {S: 'a'}, b: {S: 'a'}, c: {N: '1'}}}}]
      assertValidation(batchReq,
        new RegExp('^One or more parameter values were invalid: ' +
          'Type mismatch for Index Key c Expected: S Actual: N IndexName: index\\d$'), done)
    })

    it('should return ValidationException if hash key is too big', function(done) {
      var batchReq = {RequestItems: {}}, keyStr = (helpers.randomString() + new Array(2048).join('a')).slice(0, 2049)
      batchReq.RequestItems[helpers.testHashTable] = [{PutRequest: {Item: {a: {S: keyStr}}}}]
      assertValidation(batchReq,
        'One or more parameter values were invalid: ' +
        'Size of hashkey has exceeded the maximum size limit of2048 bytes', done)
    })

    it('should return ValidationException if range key is too big', function(done) {
      var batchReq = {RequestItems: {}}, keyStr = (helpers.randomString() + new Array(1024).join('a')).slice(0, 1025)
      batchReq.RequestItems[helpers.testRangeTable] = [{PutRequest: {Item: {a: {S: 'a'}, b: {S: keyStr}}}}]
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
      request(helpers.opts('CreateTable', table), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        var batchReq = {RequestItems: {}}
        batchReq.RequestItems[table.TableName] = [{PutRequest: {Item: {a: {S: 'a'}}}}]
        assertNotFound(batchReq, 'Requested resource not found', done)
        helpers.deleteWhenActive(table.TableName)
      })
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
          key2 = helpers.randomString(), key3 = helpers.randomNumber(),
          batchReq = {RequestItems: {}, ReturnConsumedCapacity: 'TOTAL'}
      batchReq.RequestItems[helpers.testHashTable] = [{PutRequest: {Item: item}}, {PutRequest: {Item: {a: {S: key2}}}}]
      batchReq.RequestItems[helpers.testHashNTable] = [{PutRequest: {Item: {a: {N: key3}}}}]
      request(opts(batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.ConsumedCapacity.should.containEql({CapacityUnits: 2, TableName: helpers.testHashTable})
        res.body.ConsumedCapacity.should.containEql({CapacityUnits: 1, TableName: helpers.testHashNTable})
        batchReq.ReturnConsumedCapacity = 'INDEXES'
        request(opts(batchReq), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.ConsumedCapacity.should.containEql({CapacityUnits: 2, Table: {CapacityUnits: 2}, TableName: helpers.testHashTable})
          res.body.ConsumedCapacity.should.containEql({CapacityUnits: 1, Table: {CapacityUnits: 1}, TableName: helpers.testHashNTable})
          batchReq.ReturnConsumedCapacity = 'TOTAL'
          batchReq.RequestItems[helpers.testHashTable] = [{DeleteRequest: {Key: {a: item.a}}}, {DeleteRequest: {Key: {a: {S: key2}}}}]
          batchReq.RequestItems[helpers.testHashNTable] = [{DeleteRequest: {Key: {a: {N: key3}}}}]
          request(opts(batchReq), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.ConsumedCapacity.should.containEql({CapacityUnits: 2, TableName: helpers.testHashTable})
            res.body.ConsumedCapacity.should.containEql({CapacityUnits: 1, TableName: helpers.testHashNTable})
            batchReq.ReturnConsumedCapacity = 'INDEXES'
            request(opts(batchReq), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)
              res.body.ConsumedCapacity.should.containEql({CapacityUnits: 2, Table: {CapacityUnits: 2}, TableName: helpers.testHashTable})
              res.body.ConsumedCapacity.should.containEql({CapacityUnits: 1, Table: {CapacityUnits: 1}, TableName: helpers.testHashNTable})
              done()
            })
          })
        })
      })
    })

    it('should return ConsumedCapacity from each specified table when putting and deleting larger item', function(done) {
      var a = helpers.randomString(), b = new Array(1012 - a.length).join('b'),
          item = {a: {S: a}, b: {S: b}, c: {N: '12.3456'}, d: {B: 'AQI='}, e: {BS: ['AQI=', 'Ag==']}},
          key2 = helpers.randomString(), key3 = helpers.randomNumber(),
          batchReq = {RequestItems: {}, ReturnConsumedCapacity: 'TOTAL'}
      batchReq.RequestItems[helpers.testHashTable] = [{PutRequest: {Item: item}}, {PutRequest: {Item: {a: {S: key2}}}}]
      batchReq.RequestItems[helpers.testHashNTable] = [{PutRequest: {Item: {a: {N: key3}}}}]
      request(opts(batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.ConsumedCapacity.should.containEql({CapacityUnits: 3, TableName: helpers.testHashTable})
        res.body.ConsumedCapacity.should.containEql({CapacityUnits: 1, TableName: helpers.testHashNTable})
        batchReq.ReturnConsumedCapacity = 'INDEXES'
        request(opts(batchReq), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.ConsumedCapacity.should.containEql({CapacityUnits: 3, Table: {CapacityUnits: 3}, TableName: helpers.testHashTable})
          res.body.ConsumedCapacity.should.containEql({CapacityUnits: 1, Table: {CapacityUnits: 1}, TableName: helpers.testHashNTable})
          batchReq.ReturnConsumedCapacity = 'TOTAL'
          batchReq.RequestItems[helpers.testHashTable] = [{DeleteRequest: {Key: {a: item.a}}}, {DeleteRequest: {Key: {a: {S: key2}}}}]
          batchReq.RequestItems[helpers.testHashNTable] = [{DeleteRequest: {Key: {a: {N: key3}}}}]
          request(opts(batchReq), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.ConsumedCapacity.should.containEql({CapacityUnits: 3, TableName: helpers.testHashTable})
            res.body.ConsumedCapacity.should.containEql({CapacityUnits: 1, TableName: helpers.testHashNTable})
            batchReq.ReturnConsumedCapacity = 'INDEXES'
            request(opts(batchReq), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)
              res.body.ConsumedCapacity.should.containEql({CapacityUnits: 2, Table: {CapacityUnits: 2}, TableName: helpers.testHashTable})
              res.body.ConsumedCapacity.should.containEql({CapacityUnits: 1, Table: {CapacityUnits: 1}, TableName: helpers.testHashNTable})
              done()
            })
          })
        })
      })
    })


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
          // if (err) return cb(err)
          if (err) {
            // console.log('Caught err: ' + err)
            return cb()
          }
          if (/ProvisionedThroughputExceededException$/.test(res.body.__type)) {
            // console.log('ProvisionedThroughputExceededException$')
            return cb()
          } else if (res.body.__type) {
            // return cb(new Error(JSON.stringify(res.body)))
            return cb()
          }
          res.statusCode.should.equal(200)
          // eslint-disable-next-line no-console
          console.log([CAPACITY, res.body.ConsumedCapacity[0].CapacityUnits, totalSize].join())
          setTimeout(cb, res.body.ConsumedCapacity[0].CapacityUnits * 1000 / CAPACITY)
        })
      }
    })
  })

})
