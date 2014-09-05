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

describe('putItem', function() {

  describe('serializations', function() {

    it('should return SerializationException when TableName is not a string', function(done) {
      assertType('TableName', 'String', done)
    })

    it('should return SerializationException when Item is not a map', function(done) {
      assertType('Item', 'Map', done)
    })

    it('should return SerializationException when Item.Attr is not a struct', function(done) {
      assertType('Item.Attr', 'Structure', done)
    })

    it('should return SerializationException when Item.Attr.S is not a string', function(done) {
      assertType('Item.Attr.S', 'String', done)
    })

    it('should return SerializationException when Item.Attr.B is not a blob', function(done) {
      assertType('Item.Attr.B', 'Blob', done)
    })

    it('should return SerializationException when Item.Attr.N is not a string', function(done) {
      assertType('Item.Attr.N', 'String', done)
    })

    it('should return SerializationException when Item.Attr.SS is not a list', function(done) {
      assertType('Item.Attr.SS', 'List', done)
    })

    it('should return SerializationException when Item.Attr.SS.0 is not a string', function(done) {
      assertType('Item.Attr.SS.0', 'String', done)
    })

    it('should return SerializationException when Item.Attr.NS is not a list', function(done) {
      assertType('Item.Attr.NS', 'List', done)
    })

    it('should return SerializationException when Item.Attr.NS.0 is not a string', function(done) {
      assertType('Item.Attr.NS.0', 'String', done)
    })

    it('should return SerializationException when Item.Attr.BS is not a list', function(done) {
      assertType('Item.Attr.BS', 'List', done)
    })

    it('should return SerializationException when Item.Attr.BS.0 is not a blob', function(done) {
      assertType('Item.Attr.BS.0', 'Blob', done)
    })

    it('should return SerializationException when Expected is not a map', function(done) {
      assertType('Expected', 'Map', done)
    })

    it('should return SerializationException when Expected.Attr is not a struct', function(done) {
      assertType('Expected.Attr', 'Structure', done)
    })

    it('should return SerializationException when Expected.Attr.Exists is not a boolean', function(done) {
      assertType('Expected.Attr.Exists', 'Boolean', done)
    })

    it('should return SerializationException when Expected.Attr.Value is not a struct', function(done) {
      assertType('Expected.Attr.Value', 'Structure', done)
    })

    it('should return SerializationException when Expected.Attr.Value.S is not a string', function(done) {
      assertType('Expected.Attr.Value.S', 'String', done)
    })

    it('should return SerializationException when Expected.Attr.Value.B is not a blob', function(done) {
      assertType('Expected.Attr.Value.B', 'Blob', done)
    })

    it('should return SerializationException when Expected.Attr.Value.N is not a string', function(done) {
      assertType('Expected.Attr.Value.N', 'String', done)
    })

    it('should return SerializationException when Expected.Attr.Value.SS is not a list', function(done) {
      assertType('Expected.Attr.Value.SS', 'List', done)
    })

    it('should return SerializationException when Expected.Attr.Value.SS.0 is not a string', function(done) {
      assertType('Expected.Attr.Value.SS.0', 'String', done)
    })

    it('should return SerializationException when Expected.Attr.Value.NS is not a list', function(done) {
      assertType('Expected.Attr.Value.NS', 'List', done)
    })

    it('should return SerializationException when Expected.Attr.Value.NS.0 is not a string', function(done) {
      assertType('Expected.Attr.Value.NS.0', 'String', done)
    })

    it('should return SerializationException when Expected.Attr.Value.BS is not a list', function(done) {
      assertType('Expected.Attr.Value.BS', 'List', done)
    })

    it('should return SerializationException when Expected.Attr.Value.BS.0 is not a blob', function(done) {
      assertType('Expected.Attr.Value.BS.0', 'Blob', done)
    })

    it('should return SerializationException when ReturnConsumedCapacity is not a string', function(done) {
      assertType('ReturnConsumedCapacity', 'String', done)
    })

    it('should return SerializationException when ReturnItemCollectionMetrics is not a string', function(done) {
      assertType('ReturnItemCollectionMetrics', 'String', done)
    })

    it('should return SerializationException when ReturnValues is not a string', function(done) {
      assertType('ReturnValues', 'String', done)
    })

  })

  describe('validations', function() {

    it('should return ValidationException for no TableName', function(done) {
      assertValidation({},
        '2 validation errors detected: ' +
        'Value null at \'tableName\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'item\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for empty TableName', function(done) {
      assertValidation({TableName: ''},
        '3 validation errors detected: ' +
        'Value \'\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 3; ' +
        'Value null at \'item\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for short TableName', function(done) {
      assertValidation({TableName: 'a;'},
        '3 validation errors detected: ' +
        'Value \'a;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'a;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 3; ' +
        'Value null at \'item\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for long TableName', function(done) {
      var name = new Array(256 + 1).join('a')
      assertValidation({TableName: name},
        '2 validation errors detected: ' +
        'Value \'' + name + '\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 255; ' +
        'Value null at \'item\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for incorrect attributes', function(done) {
      assertValidation({TableName: 'abc;', ReturnConsumedCapacity: 'hi',
        ReturnItemCollectionMetrics: 'hi', ReturnValues: 'hi'},
        '5 validation errors detected: ' +
        'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [INDEXES, TOTAL, NONE]; ' +
        'Value \'abc;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value null at \'item\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value \'hi\' at \'returnValues\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [ALL_NEW, UPDATED_OLD, ALL_OLD, NONE, UPDATED_NEW]; ' +
        'Value \'hi\' at \'returnItemCollectionMetrics\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [SIZE, NONE]', done)
    })

    it('should return ValidationException if no value and no exists', function(done) {
      assertValidation({TableName: 'abc', Item: {}, Expected: {a: {}}},
        'One or more parameter values were invalid: Value must be provided when Exists is null for Attribute: a', done)
    })

    it('should return ValidationException for Exists true with no value', function(done) {
      assertValidation({TableName: 'abc', Item: {}, Expected: {a: {Exists: true}}},
        'One or more parameter values were invalid: Value must be provided when Exists is true for Attribute: a', done)
    })

    it('should return ValidationException for Exists false with value', function(done) {
      assertValidation({TableName: 'abc', Item: {}, Expected: {a: {Exists: false, Value: {S: 'a'}}}},
        'One or more parameter values were invalid: Value cannot be used when Exists is false for Attribute: a', done)
    })

    it('should return ValidationException for incorrect ReturnValues', function(done) {
      async.forEach(['UPDATED_OLD', 'ALL_NEW', 'UPDATED_NEW'], function(returnValues, cb) {
        assertValidation({TableName: 'abc', Item: {}, ReturnValues: returnValues},
          'ReturnValues can only be ALL_OLD or NONE', cb)
      }, done)
    })

    it('should return ValidationException for empty key type', function(done) {
      assertValidation({TableName: 'abc', Item: {a: {}}},
        'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException for bad key type', function(done) {
      assertValidation({TableName: 'abc', Item: {a: {a: ''}}},
        'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException for empty string', function(done) {
      assertValidation({TableName: 'abc', Item: {a: {S: ''}}},
        'One or more parameter values were invalid: An AttributeValue may not contain an empty string.', done)
    })

    it('should return ValidationException for empty binary', function(done) {
      assertValidation({TableName: 'abc', Item: {a: {B: ''}}},
        'One or more parameter values were invalid: An AttributeValue may not contain an empty binary type.', done)
    })

    // Somehow allows set types for keys
    it('should return ValidationException for empty set key', function(done) {
      assertValidation({TableName: 'abc', Item: {a: {SS: []}}},
        'One or more parameter values were invalid: An AttributeValue may not contain an empty set.', done)
    })

    it('should return ValidationException for empty string in set', function(done) {
      assertValidation({TableName: 'abc', Item: {a: {SS: ['a', '']}}},
        'One or more parameter values were invalid: An AttributeValue may not contain an empty string.', done)
    })

    it('should return ValidationException for empty binary in set', function(done) {
      assertValidation({TableName: 'abc', Item: {a: {BS: ['aaaa', '']}}},
        'One or more parameter values were invalid: Binary sets may not contain null or empty values', done)
    })

    it('should return ValidationException if key has empty numeric in set', function(done) {
      assertValidation({TableName: 'abc', Item: {a: {NS: ['1', '']}}},
        'The parameter cannot be converted to a numeric value', done)
    })

    it('should return ValidationException for duplicate string in set', function(done) {
      assertValidation({TableName: 'abc', Item: {a: {SS: ['a', 'a']}}},
        'One or more parameter values were invalid: Input collection [a, a] contains duplicates.', done)
    })

    it('should return ValidationException for duplicate number in set', function(done) {
      assertValidation({TableName: 'abc', Item: {a: {NS: ['1', '1']}}},
        'Input collection contains duplicates', done)
    })

    it('should return ValidationException for duplicate binary in set', function(done) {
      assertValidation({TableName: 'abc', Item: {a: {BS: ['Yg==', 'Yg==']}}},
        'One or more parameter values were invalid: Input collection [Yg==, Yg==]of type BS contains duplicates.', done)
    })

    it('should return ValidationException for multiple types', function(done) {
      assertValidation({TableName: 'abc', Item: {a: {S: 'a', N: '1'}}},
        'Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException if key has empty numeric type', function(done) {
      assertValidation({TableName: 'abc', Item: {a: {N: ''}}},
        'The parameter cannot be converted to a numeric value', done)
    })

    it('should return ValidationException if key has incorrect numeric type', function(done) {
      assertValidation({TableName: 'abc', Item: {a: {N: 'b'}}},
        'The parameter cannot be converted to a numeric value: b', done)
    })

    it('should return ValidationException if key has incorrect numeric type in set', function(done) {
      assertValidation({TableName: 'abc', Item: {a: {NS: ['1', 'b', 'a']}}},
        'The parameter cannot be converted to a numeric value: b', done)
    })

    it('should return ValidationException for empty expected value', function(done) {
      assertValidation({TableName: 'abc', Item: {}, Expected: {a: {Value: {}}}},
        'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException if expected value has empty numeric in set', function(done) {
      assertValidation({TableName: 'abc', Item: {}, Expected: {a: {Value: {NS: ['1', '']}}}},
        'The parameter cannot be converted to a numeric value', done)
    })

    it('should return ValidationException if trying to store large number', function(done) {
      assertValidation({TableName: 'aaa', Item: {a: {N: '123456789012345678901234567890123456789'}}},
        'Attempting to store more than 38 significant digits in a Number', done)
    })

    it('should return ValidationException if trying to store long digited number', function(done) {
      assertValidation({TableName: 'aaa', Item: {a: {N: '-1.23456789012345678901234567890123456789'}}},
        'Attempting to store more than 38 significant digits in a Number', done)
    })

    it('should return ValidationException if trying to store huge positive number', function(done) {
      assertValidation({TableName: 'aaa', Item: {a: {N: '1e126'}}},
        'Number overflow. Attempting to store a number with magnitude larger than supported range', done)
    })

    it('should return ValidationException if trying to store huge negative number', function(done) {
      assertValidation({TableName: 'aaa', Item: {a: {N: '-1e126'}}},
        'Number overflow. Attempting to store a number with magnitude larger than supported range', done)
    })

    it('should return ValidationException if trying to store tiny positive number', function(done) {
      assertValidation({TableName: 'aaa', Item: {a: {N: '1e-131'}}},
        'Number underflow. Attempting to store a number with magnitude smaller than supported range', done)
    })

    it('should return ValidationException if trying to store tiny negative number', function(done) {
      assertValidation({TableName: 'aaa', Item: {a: {N: '-1e-131'}}},
        'Number underflow. Attempting to store a number with magnitude smaller than supported range', done)
    })

    it('should return ValidationException if item is too big with small attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(65536 + 1 - keyStr.length - 1).join('a')
      assertValidation({TableName: 'aaa', Item: {a: {S: keyStr}, b: {S: b}}},
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ValidationException if ComparisonOperator and Exists are used together', function(done) {
      assertValidation({TableName: 'aaa', Item: {}, Expected: {a: { Exists: true, ComparisonOperator: 'LT'}}},
        'One or more parameter values were invalid: Exists and ComparisonOperator cannot be used together for Attribute: a', done);
    });

    it('should return ValidationException if AttributeValueList and Value are used together', function(done) {
      var expected = { a: { 
        AttributeValueList: [ { S: 'a' } ],
        Value: { S: 'a' }, 
        ComparisonOperator: 'LT'
      } };
      assertValidation({TableName: 'aaa', Item: {}, Expected: expected},
        'One or more parameter values were invalid: Value and AttributeValueList cannot be used together for Attribute: a', done);
    });

    it('should return ValidationException if AttributeValueList used without ComparisonOperator', function(done) {
      assertValidation({TableName: 'aaa', Item: {}, Expected: {a: { Exists: true, AttributeValueList: [{S:'a'}]}}},
        'One or more parameter values were invalid: AttributeValueList can only be used with a ComparisonOperator for Attribute: a', done);
    });

    it('should return ValidationException if AttributeValueList is incorrect length: EQ', function(done) {
      var expected = { a: { 
        AttributeValueList: [ ], 
        ComparisonOperator: 'EQ'
      } };
      assertValidation({TableName: 'aaa', Item: {}, Expected: expected},
        'One or more parameter values were invalid: Invalid number of argument(s) for the EQ ComparisonOperator', done);
    });

    it('should return ValidationException if AttributeValueList is incorrect length: NULL', function(done) {
      var expected = { a: { 
        AttributeValueList: [{S:'a'}], 
        ComparisonOperator: 'NULL'
      } };
      assertValidation({TableName: 'aaa', Item: {}, Expected: expected},
        'One or more parameter values were invalid: Invalid number of argument(s) for the NULL ComparisonOperator', done);
    });

    it('should return ValidationException if AttributeValueList is incorrect length: IN', function(done) {
      var expected = { a: { 
        AttributeValueList: [], 
        ComparisonOperator: 'IN'
      } };
      assertValidation({TableName: 'aaa', Item: {}, Expected: expected},
        'One or more parameter values were invalid: Invalid number of argument(s) for the IN ComparisonOperator', done);
    });

    it('should return ValidationException if AttributeValueList is incorrect length: BETWEEN', function(done) {
      var expected = { a: { 
        AttributeValueList: [{N:'1'},{N:'10'},{N:'12'}], 
        ComparisonOperator: 'BETWEEN'
      } };
      assertValidation({TableName: 'aaa', Item: {}, Expected: expected},
        'One or more parameter values were invalid: Invalid number of argument(s) for the BETWEEN ComparisonOperator', done);
    });

    it('should return ValidationException if Value provides incorrect number of attributes: NULL', function(done) {
      var expected = { a: { 
        Value: {S:'a'}, 
        ComparisonOperator: 'NULL'
      } };
      assertValidation({TableName: 'aaa', Item: {}, Expected: expected},
        'One or more parameter values were invalid: Invalid number of argument(s) for the NULL ComparisonOperator', done);
    });

    it('should return ResourceNotFoundException if item is just small enough with small attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(65536 + 1 - keyStr.length - 2).join('a')
      assertNotFound({TableName: 'aaa', Item: {a: {S: keyStr}, b: {S: b}}},
        'Requested resource not found', done)
    })

    it('should return ValidationException if item is too big with larger attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(65536 + 1 - keyStr.length - 27).join('a')
      assertValidation({TableName: 'aaa', Item: {a: {S: keyStr}, bbbbbbbbbbbbbbbbbbbbbbbbbbb: {S: b}}},
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ResourceNotFoundException if item is just small enough with larger attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(65536 + 1 - keyStr.length - 28).join('a')
      assertNotFound({TableName: 'aaa', Item: {a: {S: keyStr}, bbbbbbbbbbbbbbbbbbbbbbbbbbb: {S: b}}},
        'Requested resource not found', done)
    })

    it('should return ValidationException if item is too big with multi attributes', function(done) {
      var keyStr = helpers.randomString(), b = new Array(65536 + 1 - keyStr.length - 7).join('a')
      assertValidation({TableName: 'aaa', Item: {a: {S: keyStr}, bb: {S: b}, ccc: {S: 'cc'}}},
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ResourceNotFoundException if item is just small enough with multi attributes', function(done) {
      var keyStr = helpers.randomString(), b = new Array(65536 + 1 - keyStr.length - 8).join('a')
      assertNotFound({TableName: 'aaa', Item: {a: {S: keyStr}, bb: {S: b}, ccc: {S: 'cc'}}},
        'Requested resource not found', done)
    })

    it('should return ValidationException if item is too big with big number attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(65536 + 1 - keyStr.length - 1 - 1 - 20).join('a'),
        c = new Array(38 + 1).join('1') + new Array(89).join('0')
      assertValidation({TableName: 'aaa', Item: {a: {S: keyStr}, b: {S: b}, c: {N: c}}},
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ValidationException if item is too big with smallest number attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(65536 + 1 - keyStr.length - 1 - 1 - 2).join('a'),
        c = '1' + new Array(126).join('0')
      assertValidation({TableName: 'aaa', Item: {a: {S: keyStr}, b: {S: b}, c: {N: c}}},
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ValidationException if item is too big with smaller number attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(65536 + 1 - keyStr.length - 1 - 1 - 2).join('a'),
        c = '11' + new Array(125).join('0')
      assertValidation({TableName: 'aaa', Item: {a: {S: keyStr}, b: {S: b}, c: {N: c}}},
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ValidationException if item is too big with medium number attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(65536 + 1 - keyStr.length - 1 - 1 - 4).join('a'),
        c = '11111' + new Array(122).join('0')
      assertValidation({TableName: 'aaa', Item: {a: {S: keyStr}, b: {S: b}, c: {N: c}}},
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ValidationException if item is too big with medium number attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(65536 + 1 - keyStr.length - 1 - 1 - 4).join('a'),
        c = '111111' + new Array(121).join('0')
      assertValidation({TableName: 'aaa', Item: {a: {S: keyStr}, b: {S: b}, c: {N: c}}},
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ValidationException if item is too big with medium number attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(65536 + 1 - keyStr.length - 1 - 1 - 5).join('a'),
        c = '1111111' + new Array(120).join('0')
      assertValidation({TableName: 'aaa', Item: {a: {S: keyStr}, b: {S: b}, c: {N: c}}},
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ValidationException if item is too big with multi number attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(65536 + 1 - keyStr.length - 1 - 1 - 5 - 1 - 5).join('a'),
        c = '1111111' + new Array(120).join('0'), d = '1111111' + new Array(120).join('0')
      assertValidation({TableName: 'aaa', Item: {a: {S: keyStr}, b: {S: b}, c: {N: c}, d: {N: d}}},
        'Item size has exceeded the maximum allowed size', done)
    })

    it('should return ResourceNotFoundException if item is just small enough with multi number attribute', function(done) {
      var keyStr = helpers.randomString(), b = new Array(65536 + 1 - keyStr.length - 1 - 1 - 5 - 1 - 6).join('a'),
        c = '1111111' + new Array(120).join('0'), d = '1111111' + new Array(120).join('0')
      assertNotFound({TableName: 'aaa', Item: {a: {S: keyStr}, b: {S: b}, c: {N: c}, d: {N: d}}},
        'Requested resource not found', done)
    })

    it('should return ResourceNotFoundException if key is empty and table does not exist', function(done) {
      assertNotFound({TableName: helpers.randomString(), Item: {}},
        'Requested resource not found', done)
    })

    it('should return ValidationException if key is empty and table does exist', function(done) {
      assertValidation({TableName: helpers.testHashTable, Item: {}},
        'One or more parameter values were invalid: Missing the key a in the item', done)
    })

    it('should return ValidationException if key has incorrect attributes', function(done) {
      assertValidation({TableName: helpers.testHashTable, Item: {b: {S: 'a'}}},
        'One or more parameter values were invalid: Missing the key a in the item', done)
    })

    it('should return ValidationException if key is incorrect binary type', function(done) {
      assertValidation({TableName: helpers.testHashTable, Item: {a: {B: 'abcd'}}},
        'One or more parameter values were invalid: Type mismatch for key a expected: S actual: B', done)
    })

    it('should return ValidationException if key is incorrect numeric type', function(done) {
      assertValidation({TableName: helpers.testHashTable, Item: {a: {N: '1'}}},
        'One or more parameter values were invalid: Type mismatch for key a expected: S actual: N', done)
    })

    it('should return ValidationException if key is incorrect string set type', function(done) {
      assertValidation({TableName: helpers.testHashTable, Item: {a: {SS: ['a']}}},
        'One or more parameter values were invalid: Type mismatch for key a expected: S actual: SS', done)
    })

    it('should return ValidationException if key is incorrect numeric set type', function(done) {
      assertValidation({TableName: helpers.testHashTable, Item: {a: {NS: ['1']}}},
        'One or more parameter values were invalid: Type mismatch for key a expected: S actual: NS', done)
    })

    it('should return ValidationException if key is incorrect binary set type', function(done) {
      assertValidation({TableName: helpers.testHashTable, Item: {a: {BS: ['aaaa']}}},
        'One or more parameter values were invalid: Type mismatch for key a expected: S actual: BS', done)
    })

    it('should return ValidationException if missing range key', function(done) {
      assertValidation({TableName: helpers.testRangeTable, Item: {a: {S: 'a'}}},
        'One or more parameter values were invalid: Missing the key b in the item', done)
    })

    it('should return ValidationException if secondary index key is incorrect type', function(done) {
      assertValidation({TableName: helpers.testRangeTable, Item: {a: {S: 'a'}, b: {S: 'a'}, c: {N: '1'}}},
        'One or more parameter values were invalid: ' +
        'Type mismatch for Index Key c Expected: S Actual: N IndexName: index4', done)
    })

    it('should return ValidationException if hash key is too big', function(done) {
      var keyStr = (helpers.randomString() + new Array(2048).join('a')).slice(0, 2049)
      assertValidation({TableName: helpers.testHashTable, Item: {a: {S: keyStr}}},
        'One or more parameter values were invalid: ' +
        'Size of hashkey has exceeded the maximum size limit of2048 bytes', done)
    })

    it('should return ValidationException if range key is too big', function(done) {
      var keyStr = (helpers.randomString() + new Array(1024).join('a')).slice(0, 1025)
      assertValidation({TableName: helpers.testRangeTable, Item: {a: {S: 'a'}, b: {S: keyStr}}},
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
        assertNotFound({TableName: table.TableName, Item: {a: {S: 'a'}}},
          'Requested resource not found', done)
      })
    })
  })

  // A number can have up to 38 digits precision and can be between 10^-128 to 10^+126

  describe('functionality', function() {

    it('should put basic item', function(done) {
      var item = {a: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({})
        request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item.a}, ConsistentRead: true}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({Item: item})
          done()
        })
      })
    })

    it('should put really long numbers', function(done) {
      var item = {
        a: {S: helpers.randomString()},
        b: {N: '0000012345678901234567890123456789012345678'},
        c: {N: '-00001.23456789012345678901234567890123456780000'},
        d: {N: '0009.99999999999999999999999999999999999990000e125'},
        e: {N: '-0009.99999999999999999999999999999999999990000e125'},
        f: {N: '0001.000e-130'},
        g: {N: '-0001.000e-130'},
      }
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({})
        request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item.a}, ConsistentRead: true}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          item.b = {N: '12345678901234567890123456789012345678'}
          item.c = {N: '-1.2345678901234567890123456789012345678'}
          item.d = {N: Array(39).join('9') + Array(89).join('0')}
          item.e = {N: '-' + Array(39).join('9') + Array(89).join('0')}
          item.f = {N: '0.' + Array(130).join('0') + '1'}
          item.g = {N: '-0.' + Array(130).join('0') + '1'}
          res.body.should.eql({Item: item})
          done()
        })
      })
    })

    it('should put multi attribute item', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {N: '-000056.789'}, c: {B: 'Yg=='}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({})
        request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item.a}, ConsistentRead: true}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          item.b = {N: '-56.789'}
          res.body.should.eql({Item: item})
          done()
        })
      })
    })

    it('should return empty when there are no old values', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'a'}, c: {S: 'a'}}
      request(opts({TableName: helpers.testHashTable, Item: item, ReturnValues: 'ALL_OLD'}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({})
        done()
      })
    })

    it('should return correct old values when they exist', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {N: '-0015.789e6'}, c: {S: 'a'}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        item.b = {S: 'b'}
        request(opts({TableName: helpers.testHashTable, Item: item, ReturnValues: 'ALL_OLD'}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          item.b = {N: '-15789000'}
          res.body.should.eql({Attributes: item})
          done()
        })
      })
    })

    it('should put basic range item', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'a'}, c: {S: 'a'}}
      request(opts({TableName: helpers.testRangeTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({})
        // Put another item with the same hash key to prove we're retrieving the correct one
        request(opts({TableName: helpers.testRangeTable, Item: {a: item.a, b: {S: 'b'}}}), function(err, res) {
          if (err) return done(err)
          request(helpers.opts('GetItem', {TableName: helpers.testRangeTable, Key: {a: item.a, b: item.b}, ConsistentRead: true}), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({Item: item})
            done()
          })
        })
      })
    })

    it('should return ConditionalCheckFailedException if expecting non-existent key to exist (legacy)', function(done) {
      assertConditional({
        TableName: helpers.testHashTable,
        Item: {a: {S: helpers.randomString()}},
        Expected: {a: {Value: {S: helpers.randomString()}}},
      }, done)
    })

    it('should return ConditionalCheckFailedException if expecting non-existent key to exist', function(done) {
      assertConditional({
        TableName: helpers.testHashTable,
        Item: {a: {S: helpers.randomString()}},
        Expected: {a: {ComparisonOperator: 'NOT_NULL'}},
      }, done)
    })

    it('should return ConditionalCheckFailedException if expecting existing key to not exist (legacy)', function(done) {
      var item = {a: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        assertConditional({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {Exists: false}},
        }, done)
      })
    })

    it('should return ConditionalCheckFailedException if expecting existing key to not exist', function(done) {
      var item = {a: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        assertConditional({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {ComparisonOperator: 'NULL'}},
        }, done)
      })
    });

    it('should succeed if conditional key is different and exists is false (legacy)', function(done) {
      var item = {a: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({
          TableName: helpers.testHashTable,
          Item: {a: {S: helpers.randomString()}},
          Expected: {a: {Exists: false}},
        }), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({})
          done()
        })
      })
    })

    it('should succeed if conditional key is same and exists is true (legacy)', function(done) {
      var item = {a: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {Value: item.a}},
        }), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({})
          done()
        })
      })
    })

    it('should succeed if conditional key is same', function(done) {
      var item = {a: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {
            AttributeValueList: [item.a],
            ComparisonOperator: 'EQ'
          }},
        }), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({})
          done()
        })
      })
    })

    it('should succeed if expecting non-existant value to not exist (legacy)', function(done) {
      var item = {a: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {b: {Exists: false}},
        }), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({})
          done()
        })
      })
    })

    it('should succeed if expecting non-existant value to not exist', function(done) {
      var item = {a: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {b: {ComparisonOperator: 'NULL'}},
        }), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({})
          done()
        })
      })
    })

    it('should return ConditionalCheckFailedException if expecting existing value to not exist if different value specified (legacy)', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        assertConditional({
          TableName: helpers.testHashTable,
          Item: {a: item.a, b: {S: helpers.randomString()}},
          Expected: {b: {Exists: false}},
        }, done)
      })
    })

    it('should return ConditionalCheckFailedException if expecting existing value to not exist if different value specified', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        assertConditional({
          TableName: helpers.testHashTable,
          Item: {a: item.a, b: {S: helpers.randomString()}},
          Expected: {b: {ComparisonOperator: 'NULL'}},
        }, done)
      })
    })

    it('should return ConditionalCheckFailedException if expecting existing value to not exist if value not specified (legacy)', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        assertConditional({
          TableName: helpers.testHashTable,
          Item: {a: item.a},
          Expected: {b: {Exists: false}},
        }, done)
      })
    })

    it('should return ConditionalCheckFailedException if expecting existing value to not exist if value not specified', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        assertConditional({
          TableName: helpers.testHashTable,
          Item: {a: item.a},
          Expected: {b: {ComparisonOperator: 'NULL'}},
        }, done)
      })
    })

    it('should return ConditionalCheckFailedException if expecting existing value to not exist if same value specified (legacy)', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        assertConditional({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {b: {Exists: false}},
        }, done)
      })
    })

    it('should return ConditionalCheckFailedException if expecting existing value to not exist if same value specified', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        assertConditional({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {b: {ComparisonOperator: 'NULL'}},
        }, done)
      })
    })

    it('should succeed for multiple conditional checks if all are valid (legacy)', function(done) {
      var item = {a: {S: helpers.randomString()}, c: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {Value: item.a}, b: {Exists: false}, c: {Value: item.c}},
        }), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({})
          done()
        })
      })
    })

    it('should succeed for multiple conditional checks if all are valid', function(done) {
      var item = {a: {S: helpers.randomString()}, c: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {
            a: {
              ComparisonOperator: 'EQ',
              AttributeValueList: [item.a]
            }, 
            b: {
              ComparisonOperator: 'NULL'
            }, 
            c: {
              ComparisonOperator: 'EQ',
              AttributeValueList: [item.c]
            }
          }
        }), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({})
          done()
        })
      })
    })

    it('should return ConditionalCheckFailedException for multiple conditional checks if one is invalid (legacy)', function(done) {
      var item = {a: {S: helpers.randomString()}, c: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        assertConditional({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {Value: item.a}, b: {Exists: false}, c: {Value: {S: helpers.randomString()}}},
        }, done)
      })
    })

    it('should return ConditionalCheckFailedException for multiple conditional checks if one is invalid with', function(done) {
      var item = {a: {S: helpers.randomString()}, c: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        assertConditional({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {
            a: {
              AttributeValueList: [item.a],
              ComparisonOperator: 'EQ'
            }, b: {
              ComparisonOperator: 'NULL'
            }, c: {
              AttributeValueList: [{S: helpers.randomString()}],
              ComparisonOperator: 'EQ'
            }
          }
        }, done)
      })
    })

    it('should succeed for multiple conditional checks if one is invalid and OR is specified', function(done) {
      var item = {a: {S: helpers.randomString()}, c: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({
          TableName: helpers.testHashTable,
          Item: item,
          ConditionalOperator: 'OR',
          Expected: {
            a: {
              ComparisonOperator: 'EQ',
              AttributeValueList: [item.a]
            }, 
            b: {
              ComparisonOperator: 'NULL'
            }, 
            c: {
              ComparisonOperator: 'EQ',
              AttributeValueList: [{S: helpers.randomString()}]
            }
          }
        }), function(err, res) {
          if (err) return done(err)
          res.body.should.eql({})
          res.statusCode.should.equal(200)
          done()
        })
      })
    })

    it('should succeed if condition is valid: EQ', function(done) {
      var item = {a: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {
            ComparisonOperator: 'EQ',
            AttributeValueList: [item.a]
          }},
        }), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({})
          done()
        })
      })
    })

    it('should fail if condition is invalid: EQ', function(done) {
      var item = {a: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        assertConditional({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {
            ComparisonOperator: 'EQ',
            AttributeValueList: [{S: helpers.randomString()}]
          }}
        }, done)
      })
    })

    it('should succeed if condition is valid: NE', function(done) {
      var item = {a: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {
            ComparisonOperator: 'NE',
            AttributeValueList: [{S: helpers.randomString()}]
          }},
        }), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({})
          done()
        })
      })
    })

    it('should fail if condition is invalid: NE', function(done) {
      var item = {a: {S: helpers.randomString()}}
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        assertConditional({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {
            ComparisonOperator: 'NE',
            AttributeValueList: [item.a]
          }}
        }, done)
      })
    })

    it('should succeed if condition is valid: LE', function(done) {
      var item = {a: {S: "b"}};
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {
            ComparisonOperator: 'LE',
            AttributeValueList: [{S: "c"}]
          }},
        }), function(err, res) {
          if (err) return done(err)
          res.body.should.eql({})
          res.statusCode.should.equal(200)
          done()
        })
      })
    })

    it('should fail if condition is invalid: LE', function(done) {
      var item = {a: {S: "d"}};
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        assertConditional({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {
            ComparisonOperator: 'LE',
            AttributeValueList: [{S: "c"}]
          }}
        }, done)
      })
    })

    it('should succeed if condition is valid: LT', function(done) {
      var item = {a: {S: "b"}};
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {
            ComparisonOperator: 'LT',
            AttributeValueList: [{S: "c"}]
          }},
        }), function(err, res) {
          if (err) return done(err)
          res.body.should.eql({})
          res.statusCode.should.equal(200)
          done()
        })
      })
    })

    it('should fail if condition is invalid: LT', function(done) {
      var item = {a: {S: "d"}};
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        assertConditional({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {
            ComparisonOperator: 'LT',
            AttributeValueList: [{S: "c"}]
          }}
        }, done)
      })
    })

    it('should succeed if condition is valid: GE', function(done) {
      var item = {a: {S: "b"}};
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {
            ComparisonOperator: 'GE',
            AttributeValueList: [{S: "a"}]
          }},
        }), function(err, res) {
          if (err) return done(err)
          res.body.should.eql({})
          res.statusCode.should.equal(200)
          done()
        })
      })
    })

    it('should fail if condition is invalid: GE', function(done) {
      var item = {a: {S: "b"}};
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        assertConditional({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {
            ComparisonOperator: 'GE',
            AttributeValueList: [{S: "c"}]
          }}
        }, done)
      })
    })

    it('should succeed if condition is valid: GT', function(done) {
      var item = {a: {S: "b"}};
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {
            ComparisonOperator: 'GT',
            AttributeValueList: [{S: "a"}]
          }},
        }), function(err, res) {
          if (err) return done(err)
          res.body.should.eql({})
          res.statusCode.should.equal(200)
          done()
        })
      })
    })

    it('should fail if condition is invalid: GT', function(done) {
      var item = {a: {S: "a"}};
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        assertConditional({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {
            ComparisonOperator: 'GT',
            AttributeValueList: [{S: "c"}]
          }}
        }, done)
      })
    })

    it('should succeed if condition is valid: CONTAINS', function(done) {
      var item = {a: {S: "hello"}};
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {
            ComparisonOperator: 'CONTAINS',
            AttributeValueList: [{S: "ell"}]
          }},
        }), function(err, res) {
          if (err) return done(err)
          res.body.should.eql({})
          res.statusCode.should.equal(200)
          done()
        })
      })
    })

    it('should fail if condition is invalid: CONTAINS', function(done) {
      var item = {a: {S: "hello"}};
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        assertConditional({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {
            ComparisonOperator: 'CONTAINS',
            AttributeValueList: [{S: "goodbye"}]
          }}
        }, done)
      })
    })

    it('should succeed if condition is valid: BEGINS_WITH', function(done) {
      var item = {a: {S: "hello"}};
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {
            ComparisonOperator: 'BEGINS_WITH',
            AttributeValueList: [{S: "he"}]
          }},
        }), function(err, res) {
          if (err) return done(err)
          res.body.should.eql({})
          res.statusCode.should.equal(200)
          done()
        })
      })
    })

    it('should fail if condition is invalid: BEGINS_WITH', function(done) {
      var item = {a: {S: "hello"}};
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        assertConditional({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {
            ComparisonOperator: 'BEGINS_WITH',
            AttributeValueList: [{S: "goodbye"}]
          }}
        }, done)
      })
    })

    it('should succeed if condition is valid: NOT_CONTAINS', function(done) {
      var item = {a: {S: "hello"}};
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {
            ComparisonOperator: 'NOT_CONTAINS',
            AttributeValueList: [{S: "goodbye"}]
          }},
        }), function(err, res) {
          if (err) return done(err)
          res.body.should.eql({})
          res.statusCode.should.equal(200)
          done()
        })
      })
    })

    it('should fail if condition is invalid: NOT_CONTAINS', function(done) {
      var item = {a: {S: "hello"}};
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        assertConditional({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {
            ComparisonOperator: 'NOT_CONTAINS',
            AttributeValueList: [{S: "ell"}]
          }}
        }, done)
      })
    })

    it('should succeed if condition is valid: NOT_NULL', function(done) {
      var item = {a: {S: "b"}};
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {
            ComparisonOperator: 'NOT_NULL'
          }},
        }), function(err, res) {
          if (err) return done(err)
          res.body.should.eql({})
          res.statusCode.should.equal(200)
          done()
        })
      })
    })

    it('should fail if condition is invalid: NOT_NULL', function(done) {
      var item = {a: {S: "d"}};
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        assertConditional({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {b: {
            ComparisonOperator: 'NOT_NULL'
          }}
        }, done)
      })
    })

    it('should succeed if condition is valid: NULL', function(done) {
      var item = {a: {S: "b"}};
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {b: {
            ComparisonOperator: 'NULL'
          }},
        }), function(err, res) {
          if (err) return done(err)
          res.body.should.eql({})
          res.statusCode.should.equal(200)
          done()
        })
      })
    })

    it('should fail if condition is invalid: NULL', function(done) {
      var item = {a: {S: "d"}};
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        assertConditional({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {
            ComparisonOperator: 'NULL'
          }}
        }, done)
      })
    })

    it('should succeed if condition is valid: IN', function(done) {
      var item = {a: {S: "b"}};
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {
            ComparisonOperator: 'IN',
            AttributeValueList: [{S: "c"},{S: "b"}]
          }},
        }), function(err, res) {
          if (err) return done(err)
          res.body.should.eql({})
          res.statusCode.should.equal(200)
          done()
        })
      })
    })

    it('should fail if condition is invalid: IN', function(done) {
      var item = {a: {S: "d"}};
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        assertConditional({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {
            ComparisonOperator: 'IN',
            AttributeValueList: [{S: "c"}]
          }}
        }, done)
      })
    })

    it('should succeed if condition is valid: BETWEEN', function(done) {
      var item = {a: {S: "b"}};
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {
            ComparisonOperator: 'BETWEEN',
            AttributeValueList: [{S: "a"},{S: "c"}]
          }},
        }), function(err, res) {
          if (err) return done(err)
          res.body.should.eql({})
          res.statusCode.should.equal(200)
          done()
        })
      })
    })

    it('should fail if condition is invalid: BETWEEN', function(done) {
      var item = {a: {S: "b"}};
      request(opts({TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        assertConditional({
          TableName: helpers.testHashTable,
          Item: item,
          Expected: {a: {
            ComparisonOperator: 'BETWEEN',
            AttributeValueList: [{S: "c"},{S: "d"}]
          }}
        }, done)
      })
    })

    it('should return ConsumedCapacity for small item', function(done) {
      var a = helpers.randomString(), b = new Array(1010 - a.length).join('b'),
        item = {a: {S: a}, b: {S: b}, c: {N: '12.3456'}, d: {B: 'AQI='}, e: {BS: ['AQI=', 'Ag==', 'AQ==']}}
      request(opts({TableName: helpers.testHashTable, Item: item, ReturnConsumedCapacity: 'TOTAL'}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({ConsumedCapacity: {CapacityUnits: 1, TableName: helpers.testHashTable}})
        done()
      })
    })

    it('should return ConsumedCapacity for larger item', function(done) {
      var a = helpers.randomString(), b = new Array(1012 - a.length).join('b'),
        item = {a: {S: a}, b: {S: b}, c: {N: '12.3456'}, d: {B: 'AQI='}, e: {BS: ['AQI=', 'Ag==']}}
      request(opts({TableName: helpers.testHashTable, Item: item, ReturnConsumedCapacity: 'TOTAL'}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({ConsumedCapacity: {CapacityUnits: 2, TableName: helpers.testHashTable}})
        request(opts({TableName: helpers.testHashTable, Item: {a: item.a}, ReturnConsumedCapacity: 'TOTAL'}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({ConsumedCapacity: {CapacityUnits: 2, TableName: helpers.testHashTable}})
          request(opts({TableName: helpers.testHashTable, Item: {a: item.a}, ReturnConsumedCapacity: 'TOTAL'}), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({ConsumedCapacity: {CapacityUnits: 1, TableName: helpers.testHashTable}})
            done()
          })
        })
      })
    })

  })

})


