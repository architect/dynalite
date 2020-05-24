var helpers = require('./helpers'),
    should = require('should'),
    async = require('async')

var target = 'Query',
    request = helpers.request,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target)

describe('query', function() {

  describe('serializations', function() {

    it('should return SerializationException when TableName is not a string', function(done) {
      assertType('TableName', 'String', done)
    })

    it('should return SerializationException when ExclusiveStartKey is not a map', function(done) {
      assertType('ExclusiveStartKey', 'Map<AttributeValue>', done)
    })

    it('should return SerializationException when ExclusiveStartKey.Attr is not an attr struct', function(done) {
      this.timeout(60000)
      assertType('ExclusiveStartKey.Attr', 'AttrStruct<ValueStruct>', done)
    })

    it('should return SerializationException when AttributesToGet is not a list', function(done) {
      assertType('AttributesToGet', 'List', done)
    })

    it('should return SerializationException when ConsistentRead is not a boolean', function(done) {
      assertType('ConsistentRead', 'Boolean', done)
    })

    it('should return SerializationException when ReturnConsumedCapacity is not a string', function(done) {
      assertType('ReturnConsumedCapacity', 'String', done)
    })

    it('should return SerializationException when QueryFilter is not a map', function(done) {
      assertType('QueryFilter', 'Map<Condition>', done)
    })

    it('should return SerializationException when QueryFilter.Attr is not a struct', function(done) {
      assertType('QueryFilter.Attr', 'ValueStruct<Condition>', done)
    })

    it('should return SerializationException when QueryFilter.Attr.ComparisonOperator is not a string', function(done) {
      assertType('QueryFilter.Attr.ComparisonOperator', 'String', done)
    })

    it('should return SerializationException when QueryFilter.Attr.AttributeValueList is not a list', function(done) {
      assertType('QueryFilter.Attr.AttributeValueList', 'List', done)
    })

    it('should return SerializationException when QueryFilter.Attr.AttributeValueList.0 is not an attr struct', function(done) {
      this.timeout(60000)
      assertType('QueryFilter.Attr.AttributeValueList.0', 'AttrStruct<ValueStruct>', done)
    })

    it('should return SerializationException when IndexName is not a string', function(done) {
      assertType('IndexName', 'String', done)
    })

    it('should return SerializationException when ScanIndexForward is not a boolean', function(done) {
      assertType('ScanIndexForward', 'Boolean', done)
    })

    it('should return SerializationException when Select is not a string', function(done) {
      assertType('Select', 'String', done)
    })

    it('should return SerializationException when Limit is not an integer', function(done) {
      assertType('Limit', 'Integer', done)
    })

    it('should return SerializationException when ConditionalOperator is not a string', function(done) {
      assertType('ConditionalOperator', 'String', done)
    })

    it('should return SerializationException when KeyConditions is not a map', function(done) {
      assertType('KeyConditions', 'Map<Condition>', done)
    })

    it('should return SerializationException when KeyConditions.Attr is not a struct', function(done) {
      assertType('KeyConditions.Attr', 'ValueStruct<Condition>', done)
    })

    it('should return SerializationException when KeyConditions.Attr.ComparisonOperator is not a string', function(done) {
      assertType('KeyConditions.Attr.ComparisonOperator', 'String', done)
    })

    it('should return SerializationException when KeyConditions.Attr.AttributeValueList is not a list', function(done) {
      assertType('KeyConditions.Attr.AttributeValueList', 'List', done)
    })

    it('should return SerializationException when KeyConditions.Attr.AttributeValueList.0 is not an attr struct', function(done) {
      this.timeout(60000)
      assertType('KeyConditions.Attr.AttributeValueList.0', 'AttrStruct<ValueStruct>', done)
    })

    it('should return SerializationException when KeyConditionExpression is not a string', function(done) {
      assertType('KeyConditionExpression', 'String', done)
    })

    it('should return SerializationException when FilterExpression is not a string', function(done) {
      assertType('FilterExpression', 'String', done)
    })

    it('should return SerializationException when ExpressionAttributeValues is not a map', function(done) {
      assertType('ExpressionAttributeValues', 'Map<AttributeValue>', done)
    })

    it('should return SerializationException when ExpressionAttributeValues.Attr is not an attr struct', function(done) {
      this.timeout(60000)
      assertType('ExpressionAttributeValues.Attr', 'AttrStruct<ValueStruct>', done)
    })

    it('should return SerializationException when ExpressionAttributeNames is not a map', function(done) {
      assertType('ExpressionAttributeNames', 'Map<java.lang.String>', done)
    })

    it('should return SerializationException when ExpressionAttributeNames.Attr is not a string', function(done) {
      assertType('ExpressionAttributeNames.Attr', 'String', done)
    })

    it('should return SerializationException when ProjectionExpression is not a string', function(done) {
      assertType('ProjectionExpression', 'String', done)
    })

  })

  describe('validations', function() {

    it('should return ValidationException for no TableName', function(done) {
      assertValidation({},
        '1 validation error detected: ' +
        'Value null at \'tableName\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for empty TableName', function(done) {
      assertValidation({TableName: ''}, [
        'Value \'\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        'Value \'\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 3',
      ], done)
    })

    it('should return ValidationException for short TableName', function(done) {
      assertValidation({TableName: 'a;'}, [
        'Value \'a;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        'Value \'a;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 3',
      ], done)
    })

    it('should return ValidationException for long TableName', function(done) {
      var name = new Array(256 + 1).join('a')
      assertValidation({TableName: name},
        '1 validation error detected: ' +
        'Value \'' + name + '\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 255', done)
    })

    it('should return ValidationException for empty IndexName', function(done) {
      assertValidation({TableName: 'abc', IndexName: ''}, [
        'Value \'\' at \'indexName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        'Value \'\' at \'indexName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 3',
      ], done)
    })

    it('should return ValidationException for short IndexName', function(done) {
      assertValidation({TableName: 'abc', IndexName: 'a;'}, [
        'Value \'a;\' at \'indexName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        'Value \'a;\' at \'indexName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 3',
      ], done)
    })

    it('should return ValidationException for long IndexName', function(done) {
      var name = '', i
      for (i = 0; i < 256; i++) name += 'a'
      assertValidation({TableName: 'abc', IndexName: name},
        '1 validation error detected: ' +
        'Value \'' + name + '\' at \'indexName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 255', done)
    })

    it('should return ValidationException for incorrect attributes', function(done) {
      assertValidation({TableName: 'abc;', ReturnConsumedCapacity: 'hi', AttributesToGet: [],
        IndexName: 'abc;', Select: 'hi', Limit: -1, KeyConditions: {a: {}, b: {ComparisonOperator: ''}},
        ConditionalOperator: 'AN', QueryFilter: {a: {}, b: {ComparisonOperator: ''}}}, [
          'Value \'hi\' at \'select\' failed to satisfy constraint: ' +
          'Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]',
          'Value \'abc;\' at \'indexName\' failed to satisfy constraint: ' +
          'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
          'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
          'Member must satisfy enum value set: [INDEXES, TOTAL, NONE]',
          'Value null at \'queryFilter.a.member.comparisonOperator\' failed to satisfy constraint: ' +
          'Member must not be null',
          'Value \'\' at \'queryFilter.b.member.comparisonOperator\' failed to satisfy constraint: ' +
          'Member must satisfy enum value set: [IN, NULL, BETWEEN, LT, NOT_CONTAINS, EQ, GT, NOT_NULL, NE, LE, BEGINS_WITH, GE, CONTAINS]',
          'Value \'abc;\' at \'tableName\' failed to satisfy constraint: ' +
          'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
          'Value \'AN\' at \'conditionalOperator\' failed to satisfy constraint: ' +
          'Member must satisfy enum value set: [OR, AND]',
          'Value \'[]\' at \'attributesToGet\' failed to satisfy constraint: ' +
          'Member must have length greater than or equal to 1',
          'Value \'-1\' at \'limit\' failed to satisfy constraint: ' +
          'Member must have value greater than or equal to 1',
          'Value null at \'keyConditions.a.member.comparisonOperator\' failed to satisfy constraint: ' +
          'Member must not be null',
        ], done)
    })

    it('should return ValidationException if all expressions and non-expression', function(done) {
      assertValidation({
        TableName: 'abc',
        ExclusiveStartKey: {a: {}},
        Select: 'SPECIFIC_ATTRIBUTES',
        AttributesToGet: ['a', 'a'],
        QueryFilter: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{}]}},
        ConditionalOperator: 'OR',
        KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{}]}},
        ProjectionExpression: '',
        FilterExpression: '',
        KeyConditionExpression: '',
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
      }, 'Can not use both expression and non-expression parameters in the same request: ' +
        'Non-expression parameters: {AttributesToGet, QueryFilter, ConditionalOperator, KeyConditions} ' +
        'Expression parameters: {ProjectionExpression, FilterExpression, KeyConditionExpression}', done)
    })

    it('should return ValidationException if all expressions and non-expression without KeyConditionExpression', function(done) {
      assertValidation({
        TableName: 'abc',
        QueryFilter: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{}]}},
        AttributesToGet: ['a', 'a'],
        ExclusiveStartKey: {a: {}},
        KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{}]}},
        ConditionalOperator: 'OR',
        Select: 'SPECIFIC_ATTRIBUTES',
        FilterExpression: '',
        ProjectionExpression: '',
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
      }, 'Can not use both expression and non-expression parameters in the same request: ' +
        'Non-expression parameters: {AttributesToGet, QueryFilter, ConditionalOperator} ' +
        'Expression parameters: {ProjectionExpression, FilterExpression}', done)
    })

    it('should return ValidationException if all expressions and non-expression without KeyConditions', function(done) {
      assertValidation({
        TableName: 'abc',
        QueryFilter: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{}]}},
        AttributesToGet: ['a', 'a'],
        ExclusiveStartKey: {a: {}},
        ConditionalOperator: 'OR',
        Select: 'SPECIFIC_ATTRIBUTES',
        KeyConditionExpression: '',
        FilterExpression: '',
        ProjectionExpression: '',
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
      }, 'Can not use both expression and non-expression parameters in the same request: ' +
        'Non-expression parameters: {AttributesToGet, QueryFilter, ConditionalOperator} ' +
        'Expression parameters: {ProjectionExpression, FilterExpression, KeyConditionExpression}', done)
    })

    it('should return ValidationException if ExpressionAttributeNames but no expressions', function(done) {
      assertValidation({
        TableName: 'abc',
        QueryFilter: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{}]}},
        AttributesToGet: ['a', 'a'],
        ExclusiveStartKey: {a: {}},
        ConditionalOperator: 'OR',
        Select: 'SPECIFIC_ATTRIBUTES',
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
      }, 'ExpressionAttributeNames can only be specified when using expressions', done)
    })

    it('should return ValidationException if ExpressionAttributeValues but no expressions', function(done) {
      assertValidation({
        TableName: 'abc',
        QueryFilter: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{}]}},
        AttributesToGet: ['a', 'a'],
        ExclusiveStartKey: {a: {}},
        ConditionalOperator: 'OR',
        Select: 'SPECIFIC_ATTRIBUTES',
        ExpressionAttributeValues: {},
      }, 'ExpressionAttributeValues can only be specified when using expressions: FilterExpression and KeyConditionExpression are null', done)
    })

    it('should return ValidationException for bad attribute values in QueryFilter', function(done) {
      async.forEach([
        {},
        {a: ''},
      ], function(expr, cb) {
        assertValidation({
          TableName: 'abc',
          ExclusiveStartKey: {a: {}},
          AttributesToGet: ['a', 'a'],
          KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{}]}},
          QueryFilter: {a: {ComparisonOperator: 'EQ', AttributeValueList: [expr, {S: ''}]}},
        }, 'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', cb)
      }, done)
    })

    it('should return ValidationException for invalid values in QueryFilter', function(done) {
      async.forEach([
        [{NULL: 'no'}, 'Null attribute value types must have the value of true'],
        [{SS: []}, 'An string set  may not be empty'],
        [{NS: []}, 'An number set  may not be empty'],
        [{BS: []}, 'Binary sets should not be empty'],
        [{SS: ['a', 'a']}, 'Input collection [a, a] contains duplicates.'],
        [{BS: ['Yg==', 'Yg==']}, 'Input collection [Yg==, Yg==]of type BS contains duplicates.'],
      ], function(expr, cb) {
        assertValidation({
          TableName: 'abc',
          ExclusiveStartKey: {a: {}},
          AttributesToGet: ['a', 'a'],
          KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{}]}},
          QueryFilter: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{N: '1'}, expr[0], {}]}},
        }, 'One or more parameter values were invalid: ' + expr[1], cb)
      }, done)
    })

    it('should return ValidationException for empty/invalid numbers in QueryFilter', function(done) {
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
        assertValidation({
          TableName: 'abc',
          ExclusiveStartKey: {a: {}},
          AttributesToGet: ['a', 'a'],
          KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{}]}},
          QueryFilter: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{N: '1'}, expr[0]]}},
        }, expr[1], cb)
      }, done)
    })

    it('should return ValidationException for multiple datatypes in QueryFilter', function(done) {
      assertValidation({
        TableName: 'abc',
        ExclusiveStartKey: {a: {}},
        AttributesToGet: ['a', 'a'],
        KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{}]}},
        QueryFilter: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{N: '1'}, {S: 'a', N: '1'}]}},
      }, 'Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException for incorrect number of QueryFilter arguments', function(done) {
      async.forEach([
        {a: {ComparisonOperator: 'EQ'}, b: {ComparisonOperator: 'NULL'}, c: {ComparisonOperator: 'NULL'}},
        {a: {ComparisonOperator: 'EQ'}},
        {a: {ComparisonOperator: 'EQ', AttributeValueList: []}},
        {a: {ComparisonOperator: 'NE'}},
        {a: {ComparisonOperator: 'LE'}},
        {a: {ComparisonOperator: 'LT'}},
        {a: {ComparisonOperator: 'GE'}},
        {a: {ComparisonOperator: 'GT'}},
        {a: {ComparisonOperator: 'CONTAINS'}},
        {a: {ComparisonOperator: 'NOT_CONTAINS'}},
        {a: {ComparisonOperator: 'BEGINS_WITH'}},
        {a: {ComparisonOperator: 'IN'}},
        {a: {ComparisonOperator: 'BETWEEN'}},
        {a: {ComparisonOperator: 'NULL', AttributeValueList: [{S: 'a'}]}},
        {a: {ComparisonOperator: 'NOT_NULL', AttributeValueList: [{S: 'a'}]}},
        {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'a'}, {S: 'a'}]}},
        {a: {ComparisonOperator: 'NE', AttributeValueList: [{S: 'a'}, {S: 'a'}]}},
        {a: {ComparisonOperator: 'LE', AttributeValueList: [{S: 'a'}, {S: 'a'}]}},
        {a: {ComparisonOperator: 'LT', AttributeValueList: [{S: 'a'}, {S: 'a'}]}},
        {a: {ComparisonOperator: 'GE', AttributeValueList: [{S: 'a'}, {S: 'a'}]}},
        {a: {ComparisonOperator: 'GT', AttributeValueList: [{S: 'a'}, {S: 'a'}]}},
        {a: {ComparisonOperator: 'CONTAINS', AttributeValueList: [{S: 'a'}, {S: 'a'}]}},
        {a: {ComparisonOperator: 'NOT_CONTAINS', AttributeValueList: [{S: 'a'}, {S: 'a'}]}},
        {a: {ComparisonOperator: 'NULL', AttributeValueList: [{S: 'a'}, {S: 'a'}]}},
        {a: {ComparisonOperator: 'NOT_NULL', AttributeValueList: [{S: 'a'}, {S: 'a'}]}},
        {a: {ComparisonOperator: 'BETWEEN', AttributeValueList: [{S: 'a'}, {S: 'a'}, {S: 'a'}]}},
      ], function(expr, cb) {
        assertValidation({
          TableName: 'abc',
          ExclusiveStartKey: {a: {}},
          AttributesToGet: ['a', 'a'],
          KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{}]}},
          QueryFilter: expr,
        }, 'One or more parameter values were invalid: Invalid number of argument(s) for the ' +
          expr.a.ComparisonOperator + ' ComparisonOperator', cb)
      }, done)
    })

    it('should return ValidationException for duplicate values in AttributesToGet', function(done) {
      assertValidation({
        TableName: 'abc',
        QueryFilter: {},
        ExclusiveStartKey: {a: {}},
        AttributesToGet: ['a', 'a'],
      }, 'One or more parameter values were invalid: Duplicate value in attribute name: a', done)
    })

    it('should return ValidationException for unsupported datatype in ExclusiveStartKey', function(done) {
      async.forEach([
        {KeyConditions: {a: {ComparisonOperator: 'EQ'}}},
        {KeyConditionExpression: '', ExpressionAttributeNames: {}, ExpressionAttributeValues: {}},
      ], function(keyOpts, cb) {
        async.forEach([
          {},
          {a: ''},
          {M: {a: {}}},
          {L: [{}]},
          {L: [{a: {}}]},
        ], function(expr, cb) {
          assertValidation({
            TableName: 'abc',
            KeyConditions: keyOpts.KeyConditions,
            KeyConditionExpression: keyOpts.KeyConditionExpression,
            ExclusiveStartKey: {a: expr},
          }, 'The provided starting key is invalid: ' +
            'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException for invalid values in ExclusiveStartKey', function(done) {
      async.forEach([
        {KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{}, {}]}}},
        {KeyConditionExpression: '', ExpressionAttributeNames: {}, ExpressionAttributeValues: {}},
      ], function(keyOpts, cb) {
        async.forEach([
          [{NULL: 'no'}, 'Null attribute value types must have the value of true'],
          [{SS: []}, 'An string set  may not be empty'],
          [{BS: []}, 'Binary sets should not be empty'],
        ], function(expr, cb) {
          assertValidation({
            TableName: 'abc',
            KeyConditions: keyOpts.KeyConditions,
            KeyConditionExpression: keyOpts.KeyConditionExpression,
            ExclusiveStartKey: {a: expr[0]},
          }, 'The provided starting key is invalid: ' +
            'One or more parameter values were invalid: ' + expr[1], cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException for invalid values in ExclusiveStartKey without provided message', function(done) {
      async.forEach([
        {KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{}, {}]}}},
        {KeyConditionExpression: '', ExpressionAttributeNames: {}, ExpressionAttributeValues: {}},
      ], function(keyOpts, cb) {
        async.forEach([
          [{NS: []}, 'An number set  may not be empty'],
          [{SS: ['a', 'a']}, 'Input collection [a, a] contains duplicates.'],
          [{BS: ['Yg==', 'Yg==']}, 'Input collection [Yg==, Yg==]of type BS contains duplicates.'],
        ], function(expr, cb) {
          assertValidation({
            TableName: 'abc',
            KeyConditions: keyOpts.KeyConditions,
            KeyConditionExpression: keyOpts.KeyConditionExpression,
            ExclusiveStartKey: {a: expr[0]},
          }, 'One or more parameter values were invalid: ' + expr[1], cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException for empty/invalid numbers in ExclusiveStartKey', function(done) {
      async.forEach([
        {KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{}]}}},
        {KeyConditionExpression: '', ExpressionAttributeNames: {}, ExpressionAttributeValues: {}},
      ], function(keyOpts, cb) {
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
          assertValidation({
            TableName: 'abc',
            KeyConditions: keyOpts.KeyConditions,
            KeyConditionExpression: keyOpts.KeyConditionExpression,
            ExclusiveStartKey: {a: expr[0]},
          }, expr[1], cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException for multiple datatypes in ExclusiveStartKey', function(done) {
      async.forEach([
        {KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{}]}}},
        {KeyConditionExpression: '', ExpressionAttributeNames: {}, ExpressionAttributeValues: {}},
      ], function(keyOpts, cb) {
        assertValidation({
          TableName: 'abc',
          KeyConditions: keyOpts.KeyConditions,
          KeyConditionExpression: keyOpts.KeyConditionExpression,
          ExclusiveStartKey: {a: {S: 'a', N: '1'}},
        }, 'The provided starting key is invalid: ' +
          'Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes', cb)
      }, done)
    })

    it('should return ValidationException for bad attribute values in KeyConditions', function(done) {
      async.forEach([
        {},
        {a: ''},
      ], function(expr, cb) {
        assertValidation({
          TableName: 'abc',
          QueryFilter: {},
          KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [expr, {S: ''}]}},
        }, 'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', cb)
      }, done)
    })

    it('should return ValidationException for invalid values in KeyConditions', function(done) {
      async.forEach([
        [{NULL: 'no'}, 'Null attribute value types must have the value of true'],
        [{SS: []}, 'An string set  may not be empty'],
        [{NS: []}, 'An number set  may not be empty'],
        [{BS: []}, 'Binary sets should not be empty'],
        [{SS: ['a', 'a']}, 'Input collection [a, a] contains duplicates.'],
        [{BS: ['Yg==', 'Yg==']}, 'Input collection [Yg==, Yg==]of type BS contains duplicates.'],
      ], function(expr, cb) {
        assertValidation({
          TableName: 'abc',
          KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{N: '1'}, expr[0], {}]}},
        }, 'One or more parameter values were invalid: ' + expr[1], cb)
      }, done)
    })

    it('should return ValidationException for empty/invalid numbers in KeyConditions', function(done) {
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
        assertValidation({
          TableName: 'abc',
          KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{N: '1'}, expr[0]]}},
        }, expr[1], cb)
      }, done)
    })

    it('should return ValidationException for multiple datatypes in KeyConditions', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{N: '1'}, {S: 'a', N: '1'}]}},
      }, 'Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException for incorrect number of KeyConditions arguments', function(done) {
      async.forEach([
        {a: {ComparisonOperator: 'EQ'}, b: {ComparisonOperator: 'NULL'}, c: {ComparisonOperator: 'NULL'}},
        {a: {ComparisonOperator: 'EQ'}},
        {a: {ComparisonOperator: 'EQ', AttributeValueList: []}},
        {a: {ComparisonOperator: 'NE'}},
        {a: {ComparisonOperator: 'LE'}},
        {a: {ComparisonOperator: 'LT'}},
        {a: {ComparisonOperator: 'GE'}},
        {a: {ComparisonOperator: 'GT'}},
        {a: {ComparisonOperator: 'CONTAINS'}},
        {a: {ComparisonOperator: 'NOT_CONTAINS'}},
        {a: {ComparisonOperator: 'BEGINS_WITH'}},
        {a: {ComparisonOperator: 'IN'}},
        {a: {ComparisonOperator: 'BETWEEN'}},
        {a: {ComparisonOperator: 'NULL', AttributeValueList: [{S: 'a'}]}},
        {a: {ComparisonOperator: 'NOT_NULL', AttributeValueList: [{S: 'a'}]}},
        {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'a'}, {S: 'a'}]}},
        {a: {ComparisonOperator: 'NE', AttributeValueList: [{S: 'a'}, {S: 'a'}]}},
        {a: {ComparisonOperator: 'LE', AttributeValueList: [{S: 'a'}, {S: 'a'}]}},
        {a: {ComparisonOperator: 'LT', AttributeValueList: [{S: 'a'}, {S: 'a'}]}},
        {a: {ComparisonOperator: 'GE', AttributeValueList: [{S: 'a'}, {S: 'a'}]}},
        {a: {ComparisonOperator: 'GT', AttributeValueList: [{S: 'a'}, {S: 'a'}]}},
        {a: {ComparisonOperator: 'CONTAINS', AttributeValueList: [{S: 'a'}, {S: 'a'}]}},
        {a: {ComparisonOperator: 'NOT_CONTAINS', AttributeValueList: [{S: 'a'}, {S: 'a'}]}},
        {a: {ComparisonOperator: 'NULL', AttributeValueList: [{S: 'a'}, {S: 'a'}]}},
        {a: {ComparisonOperator: 'NOT_NULL', AttributeValueList: [{S: 'a'}, {S: 'a'}]}},
        {a: {ComparisonOperator: 'BETWEEN', AttributeValueList: [{S: 'a'}, {S: 'a'}, {S: 'a'}]}},
      ], function(expr, cb) {
        assertValidation({
          TableName: 'abc',
          QueryFilter: {},
          KeyConditions: expr,
        }, 'One or more parameter values were invalid: Invalid number of argument(s) for the ' +
          expr.a.ComparisonOperator + ' ComparisonOperator', cb)
      }, done)
    })

    it('should return ValidationException for incorrect number of KeyConditions', function(done) {
      async.forEach([
        {KeyConditions: {}},
        {KeyConditions: {a: {ComparisonOperator: 'NULL'}, b: {ComparisonOperator: 'NULL'}, c: {ComparisonOperator: 'NULL'}}},
        {KeyConditionExpression: ':a = a and b = :a and :a = c', ExpressionAttributeValues: {':a': {S: 'a'}}},
        {KeyConditionExpression: '(a > :a and b > :a) and (c > :a and d > :a) and (e > :a and f > :a)', ExpressionAttributeValues: {':a': {S: 'a'}}},
      ], function(queryOpts, cb) {
        assertValidation({
          TableName: 'abc',
          QueryFilter: queryOpts.KeyConditions ? {} : undefined,
          KeyConditions: queryOpts.KeyConditions,
          KeyConditionExpression: queryOpts.KeyConditionExpression,
          ExpressionAttributeValues: queryOpts.ExpressionAttributeValues,
        }, 'Conditions can be of length 1 or 2 only', cb)
      }, done)
    })

    it('should return ValidationException for invalid ComparisonOperator types', function(done) {
      async.forEach(['QueryFilter', 'KeyConditions'], function(attr, cb) {
        async.forEach([
          'LT',
          'LE',
          'GT',
          'GE',
          'IN',
        ], function(cond, cb) {
          async.forEach([
            [{BOOL: true}],
            [{NULL: true}],
            [{SS: ['a']}],
            [{NS: ['1']}],
            [{BS: ['abcd']}],
            [{M: {}}],
            [{L: []}],
          ], function(list, cb) {
            var queryOpts = {TableName: 'abc'}
            queryOpts[attr] = {a: {ComparisonOperator: cond, AttributeValueList: list}}
            assertValidation(queryOpts, 'One or more parameter values were invalid: ' +
              'ComparisonOperator ' + cond + ' is not valid for ' +
              Object.keys(list[0])[0] + ' AttributeValue type', cb)
          }, cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException for invalid CONTAINS ComparisonOperator types', function(done) {
      async.forEach(['QueryFilter', 'KeyConditions'], function(attr, cb) {
        async.forEach([
          'CONTAINS',
          'NOT_CONTAINS',
        ], function(cond, cb) {
          async.forEach([
            [{SS: ['a']}],
            [{NS: ['1']}],
            [{BS: ['abcd']}],
            [{M: {}}],
            [{L: []}],
          ], function(list, cb) {
            var queryOpts = {TableName: 'abc'}
            queryOpts[attr] = {a: {ComparisonOperator: cond, AttributeValueList: list}}
            assertValidation(queryOpts, 'One or more parameter values were invalid: ' +
              'ComparisonOperator ' + cond + ' is not valid for ' +
              Object.keys(list[0])[0] + ' AttributeValue type', cb)
          }, cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException for invalid BETWEEN ComparisonOperator types', function(done) {
      async.forEach(['QueryFilter', 'KeyConditions'], function(attr, cb) {
        async.forEach([
          [{BOOL: true}, {BOOL: true}],
          [{NULL: true}, {NULL: true}],
          [{SS: ['a']}, {SS: ['a']}],
          [{NS: ['1']}, {NS: ['1']}],
          [{BS: ['abcd']}, {BS: ['abcd']}],
          [{M: {}}, {M: {}}],
          [{L: []}, {L: []}],
        ], function(list, cb) {
          var queryOpts = {TableName: 'abc'}
          queryOpts[attr] = {a: {ComparisonOperator: 'BETWEEN', AttributeValueList: list}}
          assertValidation(queryOpts, 'One or more parameter values were invalid: ' +
            'ComparisonOperator BETWEEN is not valid for ' +
            Object.keys(list[0])[0] + ' AttributeValue type', cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException for invalid BEGINS_WITH ComparisonOperator types', function(done) {
      async.forEach(['QueryFilter', 'KeyConditions'], function(attr, cb) {
        async.forEach([
          [{N: '1'}],
          // [{B: 'YQ=='}], // B is fine
          [{BOOL: true}],
          [{NULL: true}],
          [{SS: ['a']}],
          [{NS: ['1']}],
          [{BS: ['abcd']}],
          [{M: {}}],
          [{L: []}],
        ], function(list, cb) {
          var queryOpts = {TableName: 'abc'}
          queryOpts[attr] = {a: {ComparisonOperator: 'BEGINS_WITH', AttributeValueList: list}}
          assertValidation(queryOpts, 'One or more parameter values were invalid: ' +
            'ComparisonOperator BEGINS_WITH is not valid for ' +
            Object.keys(list[0])[0] + ' AttributeValue type', cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException if AttributeValueList has different types', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'IN', AttributeValueList: [{S: 'b'}, {N: '1'}]}},
      }, 'One or more parameter values were invalid: AttributeValues inside AttributeValueList must be of same type', done)
    })

    it('should return ValidationException if BETWEEN arguments are in the incorrect order', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'BETWEEN', AttributeValueList: [{S: 'b'}, {S: 'a'}]}},
      }, 'The BETWEEN condition was provided a range where the lower bound is greater than the upper bound', done)
    })

    it('should return ValidationException for empty ExpressionAttributeNames', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditionExpression: '',
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
      }, 'ExpressionAttributeNames must not be empty', done)
    })

    it('should return ValidationException for invalid ExpressionAttributeNames', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditionExpression: '',
        ExpressionAttributeNames: {'a': 'a'},
        ExpressionAttributeValues: {},
      }, 'ExpressionAttributeNames contains invalid key: Syntax error; key: "a"', done)
    })

    it('should return ValidationException for empty ExpressionAttributeValues', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditionExpression: '',
        ExpressionAttributeValues: {},
      }, 'ExpressionAttributeValues must not be empty', done)
    })

    it('should return ValidationException for invalid ExpressionAttributeValues', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditionExpression: '',
        ExpressionAttributeValues: {'a': {S: 'b'}},
      }, 'ExpressionAttributeValues contains invalid key: Syntax error; key: "a"', done)
    })

    it('should return ValidationException for empty KeyConditionExpression', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditionExpression: '',
        FilterExpression: '',
        ProjectionExpression: '',
        ExpressionAttributeValues: {':0': {S: 'b'}},
      }, 'Invalid KeyConditionExpression: The expression can not be empty;', done)
    })

    it('should return ValidationException for syntax errors in KeyConditionExpression', function(done) {
      var expressions = [
        'things are not gonna be ok',
        'a > 4',
        '(size(a))[0] > a',
      ]
      async.forEach(expressions, function(expression, cb) {
        assertValidation({
          TableName: 'abc',
          FilterExpression: '',
          KeyConditionExpression: expression,
        }, /^Invalid KeyConditionExpression: Syntax error; /, cb)
      }, done)
    })

    it('should return ValidationException for invalid operand types', function(done) {
      var expressions = [
        'attribute_type(a, b)',
      ]
      async.forEach(expressions, function(expression, cb) {
        assertValidation({
          TableName: 'abc',
          FilterExpression: '',
          KeyConditionExpression: expression,
        }, /^Invalid KeyConditionExpression: Incorrect operand type for operator or function; operator or function: attribute_type, operand type:/, cb)
      }, done)
    })

    it('should return ValidationException for invalid operand types with attributes', function(done) {
      var expressions = [
        'attribute_type(a, :a)',
      ]
      async.forEach(expressions, function(expression, cb) {
        assertValidation({
          TableName: 'abc',
          FilterExpression: '',
          KeyConditionExpression: expression,
          ExpressionAttributeValues: {':a': {N: '1'}},
        }, /^Invalid KeyConditionExpression: Incorrect operand type for operator or function; operator or function: attribute_type, operand type:/, cb)
      }, done)
    })

    it('should return ValidationException for empty FilterExpression', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditionExpression: 'attribute_type(a, :a)',
        FilterExpression: '',
        ProjectionExpression: '',
        ExpressionAttributeValues: {':a': {S: 'N'}},
      }, 'Invalid FilterExpression: The expression can not be empty;', done)
    })

    it('should return ValidationException for empty ProjectionExpression', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditionExpression: 'attribute_type(a, :a)',
        FilterExpression: 'a > b',
        ProjectionExpression: '',
        ExpressionAttributeValues: {':a': {S: 'N'}},
      }, 'Invalid ProjectionExpression: The expression can not be empty;', done)
    })

    it('should return ValidationException for invalid operator', function(done) {
      var expressions = [
        'attribute_type(a, :a)',
      ]
      async.forEach(expressions, function(expression, cb) {
        assertValidation({
          TableName: 'abc',
          KeyConditionExpression: expression,
          ExpressionAttributeValues: {':a': {S: 'S'}},
        }, 'Invalid operator used in KeyConditionExpression: attribute_type', cb)
      }, done)
    })

    it('should return ValidationException for invalid operators', function(done) {
      var expressions = [
        ['a > b and size(b) > c or a > c', 'OR'],
        ['a in (b, size(c), d)', 'IN'],
        ['attribute_exists(a)', 'attribute_exists'],
        ['attribute_not_exists(a)', 'attribute_not_exists'],
        ['contains(a.d, b)', 'contains'],
        ['not a > b', 'NOT'],
        ['a <> b', '<>'],
      ]
      async.forEach(expressions, function(expr, cb) {
        assertValidation({
          TableName: 'abc',
          KeyConditionExpression: expr[0],
        }, 'Invalid operator used in KeyConditionExpression: ' + expr[1], cb)
      }, done)
    })

    it('should return ValidationException no key attribute as first operator', function(done) {
      var expressions = [
        [':a between size(b) and size(a) and b > :b', 'BETWEEN'],
        ['begins_with(:a, b) and a > :b', 'begins_with'],
      ]
      async.forEach(expressions, function(expr, cb) {
        assertValidation({
          TableName: 'abc',
          KeyConditionExpression: expr[0],
          ExpressionAttributeValues: {':a': {S: '1'}, ':b': {S: '1'}},
        }, 'Invalid condition in KeyConditionExpression: ' + expr[1] + ' operator must have the key attribute as its first operand', cb)
      }, done)
    })

    it('should return ValidationException for nested operations', function(done) {
      var expressions = [
        'size(b) > a AND a.b > b AND b > c',
        'a > size(b.d)',
        'a between size(b) and size(a)',
      ]
      async.forEach(expressions, function(expression, cb) {
        assertValidation({
          TableName: 'abc',
          KeyConditionExpression: expression,
        }, 'KeyConditionExpressions cannot contain nested operations', cb)
      }, done)
    })

    it('should return ValidationException for multiple attribute names', function(done) {
      var expressions = [
        'b > a.b',
        'b between c[1] and d',
        'a between b and size(a)',
        'begins_with(a, b)',
      ]
      async.forEach(expressions, function(expression, cb) {
        assertValidation({
          TableName: 'abc',
          KeyConditionExpression: expression,
        }, 'Invalid condition in KeyConditionExpression: Multiple attribute names used in one condition', cb)
      }, done)
    })

    it('should return ValidationException for nested attributes', function(done) {
      var expressions = [
        'b.d > a AND c.d > e.f',
        'a.d > c',
        'b[0] > a',
      ]
      async.forEach(expressions, function(expression, cb) {
        assertValidation({
          TableName: 'abc',
          KeyConditionExpression: expression,
        }, 'KeyConditionExpressions cannot have conditions on nested attributes', cb)
      }, done)
    })

    it('should return ValidationException for no key attribute', function(done) {
      var expressions = [
        ':b > :a AND a < :b',
        ':a > b AND :a < :b',
      ]
      async.forEach(expressions, function(expression, cb) {
        assertValidation({
          TableName: 'abc',
          KeyConditionExpression: expression,
          ExpressionAttributeValues: {':a': {N: '1'}, ':b': {N: '1'}},
        }, 'Invalid condition in KeyConditionExpression: No key attribute specified', cb)
      }, done)
    })

    it('should return ValidationException for multiple conditions per key', function(done) {
      var expressions = [
        'b > :a AND b < :a and c = :a and d = :a',
        '(a > :a and b > :a) and (b > :a and c > :a)',
      ]
      async.forEach(expressions, function(expression, cb) {
        assertValidation({
          TableName: 'abc',
          KeyConditionExpression: expression,
          ExpressionAttributeValues: {':a': {N: '1'}},
        }, 'KeyConditionExpressions must only contain one condition per key', cb)
      }, done)
    })

    it('should return ValidationException if KeyConditionExpression BETWEEN args have different types', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditionExpression: 'a between :b and :a',
        ExpressionAttributeValues: {':a': {S: 'a'}, ':b': {N: '1'}},
      }, 'Invalid KeyConditionExpression: The BETWEEN operator requires same data type for lower and upper bounds; ' +
        'lower bound operand: AttributeValue: {N:1}, upper bound operand: AttributeValue: {S:a}', done)
    })

    it('should return ValidationException if KeyConditionExpression BETWEEN args are in the incorrect order', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditionExpression: 'a between :b and :a',
        ExpressionAttributeValues: {':a': {S: 'a'}, ':b': {S: 'b'}},
      }, 'Invalid KeyConditionExpression: The BETWEEN operator requires upper bound to be greater than or equal to lower bound; ' +
        'lower bound operand: AttributeValue: {S:b}, upper bound operand: AttributeValue: {S:a}', done)
    })

    it('should check table exists before checking key validity', function(done) {
      async.forEach([
        {KeyConditions: {z: {ComparisonOperator: 'NULL'}}, QueryFilter: {}},
        {KeyConditionExpression: 'z = :a', ExpressionAttributeValues: {':a': {N: '1'}}},
        {KeyConditionExpression: 'b between :a and :b', ExpressionAttributeValues: {':a': {N: '1'}, ':b': {N: '1'}}},
      ], function(keyOpts, cb) {
        async.forEach([
          {},
          {b: {S: 'a'}},
          {a: {S: 'a'}, b: {S: 'a'}},
        ], function(expr, cb) {
          assertNotFound({
            TableName: 'abc',
            ExclusiveStartKey: expr,
            QueryFilter: keyOpts.QueryFilter,
            KeyConditions: keyOpts.KeyConditions,
            KeyConditionExpression: keyOpts.KeyConditionExpression,
            ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
          }, 'Requested resource not found', cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException for non-existent index name', function(done) {
      async.forEach([
        helpers.testHashTable,
        helpers.testRangeTable,
      ], function(table, cb) {
        assertValidation({
          TableName: table,
          IndexName: 'whatever',
          ExclusiveStartKey: {},
          KeyConditions: {z: {ComparisonOperator: 'NULL'}},
        }, 'The table does not have the specified index: whatever', cb)
      }, done)
    })

    it('should return ValidationException for querying global index with ConsistentRead', function(done) {
      assertValidation({
        TableName: helpers.testRangeTable,
        IndexName: 'index3',
        ConsistentRead: true,
        ExclusiveStartKey: {},
        KeyConditions: {z: {ComparisonOperator: 'NULL'}},
      }, 'Consistent reads are not supported on global secondary indexes', done)
    })

    it('should return ValidationException if ExclusiveStartKey is invalid', function(done) {
      async.forEach([
        {KeyConditions: {z: {ComparisonOperator: 'NULL'}}, QueryFilter: {}},
        {KeyConditionExpression: 'z = :a', ExpressionAttributeValues: {':a': {N: '1'}}},
      ], function(keyOpts, cb) {
        async.forEach([
          {},
          {b: {S: 'a'}},
          {a: {S: 'a'}, b: {S: 'a'}},
        ], function(expr, cb) {
          assertValidation({
            TableName: helpers.testHashTable,
            ExclusiveStartKey: expr,
            QueryFilter: keyOpts.QueryFilter,
            KeyConditions: keyOpts.KeyConditions,
            KeyConditionExpression: keyOpts.KeyConditionExpression,
            ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
          }, 'The provided starting key is invalid', cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException if ExclusiveStartKey for range table is invalid', function(done) {
      async.forEach([
        {KeyConditions: {z: {ComparisonOperator: 'NULL'}}, QueryFilter: {}},
        {KeyConditionExpression: 'z = :a', ExpressionAttributeValues: {':a': {N: '1'}}},
      ], function(keyOpts, cb) {
        async.forEach([
          {},
          {z: {N: '1'}},
          {b: {S: 'a'}, c: {S: 'b'}},
          {a: {B: 'abcd'}},
          {a: {S: 'a'}},
          {a: {N: '1'}, b: {S: 'a'}, c: {S: 'b'}},
          {a: {N: '1'}, b: {N: '1'}, z: {N: '1'}},
        ], function(expr, cb) {
          assertValidation({
            TableName: helpers.testRangeTable,
            ExclusiveStartKey: expr,
            QueryFilter: keyOpts.QueryFilter,
            KeyConditions: keyOpts.KeyConditions,
            KeyConditionExpression: keyOpts.KeyConditionExpression,
            ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
          }, 'The provided starting key is invalid', cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException if ExclusiveStartKey is invalid for local index', function(done) {
      async.forEach([
        {KeyConditions: {z: {ComparisonOperator: 'NULL'}}, QueryFilter: {}},
        {KeyConditionExpression: 'z = :a', ExpressionAttributeValues: {':a': {N: '1'}}},
      ], function(keyOpts, cb) {
        async.forEach([
          {},
          {z: {N: '1'}},
          {a: {B: 'abcd'}},
          {a: {S: 'a'}},
          {a: {S: 'a'}, b: {S: 'a'}},
          {a: {S: 'a'}, c: {S: 'a'}},
          {b: {S: 'a'}, c: {S: 'a'}},
        ], function(expr, cb) {
          assertValidation({
            TableName: helpers.testRangeTable,
            IndexName: 'index1',
            ExclusiveStartKey: expr,
            QueryFilter: keyOpts.QueryFilter,
            KeyConditions: keyOpts.KeyConditions,
            KeyConditionExpression: keyOpts.KeyConditionExpression,
            ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
          }, 'The provided starting key is invalid', cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException if ExclusiveStartKey is invalid for global index', function(done) {
      async.forEach([
        {KeyConditions: {z: {ComparisonOperator: 'NULL'}}, QueryFilter: {}},
        {KeyConditionExpression: 'z = :a', ExpressionAttributeValues: {':a': {N: '1'}}},
      ], function(keyOpts, cb) {
        async.forEach([
          {},
          {z: {N: '1'}},
          {a: {B: 'abcd'}},
          {a: {S: 'a'}},
          {a: {S: 'a'}, b: {S: 'a'}},
          {a: {S: 'a'}, c: {S: 'a'}},
          {a: {S: 'a'}, b: {S: 'a'}, z: {S: 'a'}},
          {a: {S: 'a'}, b: {S: 'a'}, c: {S: 'a'}, z: {S: 'a'}},
          {c: {N: '1'}},
          {c: {S: '1'}},
        ], function(expr, cb) {
          assertValidation({
            TableName: helpers.testRangeTable,
            IndexName: 'index3',
            ExclusiveStartKey: expr,
            QueryFilter: keyOpts.QueryFilter,
            KeyConditions: keyOpts.KeyConditions,
            KeyConditionExpression: keyOpts.KeyConditionExpression,
            ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
          }, 'The provided starting key is invalid', cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException if ExclusiveStartKey does not match hash schema', function(done) {
      async.forEach([
        {KeyConditions: {z: {ComparisonOperator: 'NULL'}}, QueryFilter: {}},
        {KeyConditionExpression: 'z = :a', ExpressionAttributeValues: {':a': {N: '1'}}},
      ], function(keyOpts, cb) {
        async.forEach([
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
          assertValidation({
            TableName: helpers.testHashTable,
            ExclusiveStartKey: expr,
            QueryFilter: keyOpts.QueryFilter,
            KeyConditions: keyOpts.KeyConditions,
            KeyConditionExpression: keyOpts.KeyConditionExpression,
            ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
          }, 'The provided key element does not match the schema', cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException if ExclusiveStartKey does not match range schema', function(done) {
      async.forEach([
        {KeyConditions: {z: {ComparisonOperator: 'NULL'}}, QueryFilter: {}},
        {KeyConditionExpression: 'z = :a', ExpressionAttributeValues: {':a': {N: '1'}}},
      ], function(keyOpts, cb) {
        async.forEach([
          {a: {N: '1'}, z: {S: 'a'}},
          {a: {B: 'YQ=='}, b: {S: 'a'}},
        ], function(expr, cb) {
          assertValidation({
            TableName: helpers.testRangeTable,
            ExclusiveStartKey: expr,
            QueryFilter: keyOpts.QueryFilter,
            KeyConditions: keyOpts.KeyConditions,
            KeyConditionExpression: keyOpts.KeyConditionExpression,
            ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
          }, 'The provided key element does not match the schema', cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException if ExclusiveStartKey does not match schema for local index', function(done) {
      async.forEach([
        {KeyConditions: {z: {ComparisonOperator: 'NULL'}}, QueryFilter: {}},
        {KeyConditionExpression: 'z = :a', ExpressionAttributeValues: {':a': {N: '1'}}},
      ], function(keyOpts, cb) {
        async.forEach([
          {a: {N: '1'}, x: {S: '1'}, y: {S: '1'}},
          {a: {B: 'YQ=='}, b: {S: '1'}, c: {S: '1'}},
        ], function(expr, cb) {
          assertValidation({
            TableName: helpers.testRangeTable,
            IndexName: 'index1',
            ExclusiveStartKey: expr,
            QueryFilter: keyOpts.QueryFilter,
            KeyConditions: keyOpts.KeyConditions,
            KeyConditionExpression: keyOpts.KeyConditionExpression,
            ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
          }, 'The provided key element does not match the schema', cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException if ExclusiveStartKey does not match schema for global index', function(done) {
      async.forEach([
        {KeyConditions: {z: {ComparisonOperator: 'NULL'}}, QueryFilter: {}},
        {KeyConditionExpression: 'z = :a', ExpressionAttributeValues: {':a': {N: '1'}}},
      ], function(keyOpts, cb) {
        async.forEach([
          {x: {S: '1'}, y: {S: '1'}, c: {N: '1'}},
          {a: {S: '1'}, b: {S: '1'}, c: {B: 'YQ=='}},
        ], function(expr, cb) {
          assertValidation({
            TableName: helpers.testRangeTable,
            IndexName: 'index3',
            ExclusiveStartKey: expr,
            QueryFilter: keyOpts.QueryFilter,
            KeyConditions: keyOpts.KeyConditions,
            KeyConditionExpression: keyOpts.KeyConditionExpression,
            ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
          }, 'The provided key element does not match the schema', cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException if hash in ExclusiveStartKey but not in query', function(done) {
      async.forEach([
        undefined,
        {a: {S: 'a'}, b: {N: '1'}},
        {a: {S: 'a'}, c: {N: '1'}},
        {a: {S: 'a'}, z: {S: '1'}},
      ], function(expr, cb) {
        async.forEach([
          {KeyConditions: {z: {ComparisonOperator: 'NULL'}}},
          {KeyConditionExpression: 'z between :a and :b', ExpressionAttributeValues: {':a': {N: '1'}, ':b': {N: '1'}}},
        ], function(keyOpts, cb) {
          assertValidation({
            TableName: helpers.testRangeTable,
            ExclusiveStartKey: expr,
            KeyConditions: keyOpts.KeyConditions,
            KeyConditionExpression: keyOpts.KeyConditionExpression,
            ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
          }, 'Query condition missed key schema element: a', cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException if local hash in ExclusiveStartKey but not in query', function(done) {
      async.forEach([
        undefined,
        {a: {S: '1'}, b: {N: '1'}, c: {N: '1'}},
        {a: {S: 'a'}, b: {S: 'a'}, c: {S: 'a'}},
        {a: {S: 'a'}, b: {S: 'a'}, z: {S: 'a'}},
      ], function(expr, cb) {
        async.forEach([
          {KeyConditions: {z: {ComparisonOperator: 'NULL'}}},
          {KeyConditionExpression: 'z between :a and :b', ExpressionAttributeValues: {':a': {N: '1'}, ':b': {N: '1'}}},
        ], function(keyOpts, cb) {
          assertValidation({
            TableName: helpers.testRangeTable,
            IndexName: 'index1',
            ExclusiveStartKey: expr,
            KeyConditions: keyOpts.KeyConditions,
            KeyConditionExpression: keyOpts.KeyConditionExpression,
            ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
          }, 'Query condition missed key schema element: a', cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException if global hash in ExclusiveStartKey but not in query', function(done) {
      async.forEach([
        undefined,
        {x: {N: '1'}, y: {N: '1'}, c: {S: '1'}},
        {a: {N: '1'}, b: {N: '1'}, c: {S: '1'}},
        {a: {S: 'a'}, b: {S: 'a'}, c: {S: 'a'}},
      ], function(expr, cb) {
        async.forEach([
          {KeyConditions: {z: {ComparisonOperator: 'NULL'}}},
          {KeyConditionExpression: 'z between :a and :b', ExpressionAttributeValues: {':a': {N: '1'}, ':b': {N: '1'}}},
        ], function(keyOpts, cb) {
          assertValidation({
            TableName: helpers.testRangeTable,
            IndexName: 'index3',
            ExclusiveStartKey: expr,
            KeyConditions: keyOpts.KeyConditions,
            KeyConditionExpression: keyOpts.KeyConditionExpression,
            ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
          }, 'Query condition missed key schema element: c', cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException if range in ExclusiveStartKey is invalid', function(done) {
      async.forEach([
        {a: {S: 'a'}},
        {a: {S: 'a'}, c: {N: '1'}},
        {a: {S: 'a'}, z: {S: '1'}},
        {a: {S: 'a'}, b: {S: '1'}, c: {S: '1'}},
      ], function(expr, cb) {
        async.forEach([
          {KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'b'}]}}},
          {KeyConditionExpression: 'a = :a', ExpressionAttributeValues: {':a': {S: 'b'}}},
        ], function(keyOpts, cb) {
          assertValidation({
            TableName: helpers.testRangeTable,
            ExclusiveStartKey: expr,
            KeyConditions: keyOpts.KeyConditions,
            KeyConditionExpression: keyOpts.KeyConditionExpression,
            ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
          }, 'The provided starting key is invalid', cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException if local range in ExclusiveStartKey is invalid', function(done) {
      async.forEach([
        {a: {S: 'a'}},
        {a: {S: 'a'}, c: {N: '1'}},
        {a: {S: 'a'}, z: {S: '1'}},
        {a: {S: 'a'}, b: {S: '1'}, c: {S: '1'}, d: {S: '1'}},
      ], function(expr, cb) {
        async.forEach([
          {KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'b'}]}}},
          {KeyConditionExpression: 'a = :a', ExpressionAttributeValues: {':a': {S: 'b'}}},
        ], function(keyOpts, cb) {
          assertValidation({
            TableName: helpers.testRangeTable,
            IndexName: 'index1',
            ExclusiveStartKey: expr,
            KeyConditions: keyOpts.KeyConditions,
            KeyConditionExpression: keyOpts.KeyConditionExpression,
            ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
          }, 'The provided starting key is invalid', cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException if global range in ExclusiveStartKey is invalid', function(done) {
      async.forEach([
        {c: {S: '1'}},
        {a: {N: '1'}, c: {S: '1'}},
        {a: {N: '1'}, b: {N: '1'}, c: {S: '1'}},
        {a: {N: '1'}, b: {N: '1'}, c: {S: '1'}, e: {N: '1'}},
        {a: {S: 'a'}, b: {S: '1'}, c: {S: '1'}, d: {S: '1'}, e: {S: '1'}},
      ], function(expr, cb) {
        async.forEach([
          {KeyConditions: {c: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'b'}]}}},
          {KeyConditionExpression: 'c = :a', ExpressionAttributeValues: {':a': {S: 'b'}}},
        ], function(keyOpts, cb) {
          assertValidation({
            TableName: helpers.testRangeTable,
            IndexName: 'index4',
            Select: 'ALL_ATTRIBUTES',
            ExclusiveStartKey: expr,
            KeyConditions: keyOpts.KeyConditions,
            KeyConditionExpression: keyOpts.KeyConditionExpression,
            ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
          }, 'The provided starting key is invalid', cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException if hash in ExclusiveStartKey and KeyConditions but range has incorrect schema', function(done) {
      async.forEach([
        {a: {S: 'a'}, b: {N: '1'}},
        {a: {S: 'a'}, b: {B: 'YQ=='}},
      ], function(expr, cb) {
        async.forEach([
          {KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'b'}]}}},
          {KeyConditionExpression: 'a = :a', ExpressionAttributeValues: {':a': {S: 'b'}}},
        ], function(keyOpts, cb) {
          assertValidation({
            TableName: helpers.testRangeTable,
            ExclusiveStartKey: expr,
            KeyConditions: keyOpts.KeyConditions,
            KeyConditionExpression: keyOpts.KeyConditionExpression,
            ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
          }, 'The provided key element does not match the schema', cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException if hash in ExclusiveStartKey and KeyConditions but local has incorrect schema', function(done) {
      async.forEach([
        {a: {S: 'a'}, b: {N: '1'}, c: {N: '1'}},
        {a: {S: 'a'}, b: {B: 'YQ=='}, c: {N: '1'}},
        {a: {S: 'a'}, b: {S: 'a'}, c: {N: '1'}},
        {a: {S: 'a'}, b: {S: 'a'}, c: {B: 'YQ=='}},
      ], function(expr, cb) {
        async.forEach([
          {KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'b'}]}}},
          {KeyConditionExpression: 'a = :a', ExpressionAttributeValues: {':a': {S: 'b'}}},
        ], function(keyOpts, cb) {
          assertValidation({
            TableName: helpers.testRangeTable,
            IndexName: 'index1',
            ExclusiveStartKey: expr,
            KeyConditions: keyOpts.KeyConditions,
            KeyConditionExpression: keyOpts.KeyConditionExpression,
            ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
          }, 'The provided key element does not match the schema', cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException if range in ExclusiveStartKey is invalid, but hash and local are ok', function(done) {
      async.forEach([
        {a: {S: '1'}, b: {N: '1'}, c: {S: 'a'}},
        {a: {S: '1'}, b: {B: 'YQ=='}, c: {S: 'a'}},
      ], function(expr, cb) {
        async.forEach([
          {KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'b'}]}}},
          {KeyConditionExpression: 'a = :a', ExpressionAttributeValues: {':a': {S: 'b'}}},
        ], function(keyOpts, cb) {
          assertValidation({
            TableName: helpers.testRangeTable,
            IndexName: 'index1',
            ExclusiveStartKey: expr,
            KeyConditions: keyOpts.KeyConditions,
            KeyConditionExpression: keyOpts.KeyConditionExpression,
            ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
          }, 'The provided starting key is invalid: The provided key element does not match the schema', cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException if global hash in ExclusiveStartKey but bad in query', function(done) {
      async.forEach([
        {x: {N: '1'}, y: {N: '1'}, c: {S: 'a'}},
        {a: {N: '1'}, b: {S: '1'}, c: {S: 'a'}},
        {a: {S: '1'}, b: {N: '1'}, c: {S: 'a'}},
      ], function(expr, cb) {
        async.forEach([
          {KeyConditions: {c: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'b'}]}}},
          {KeyConditionExpression: 'c = :a', ExpressionAttributeValues: {':a': {S: 'b'}}},
        ], function(keyOpts, cb) {
          assertValidation({
            TableName: helpers.testRangeTable,
            IndexName: 'index3',
            ExclusiveStartKey: expr,
            KeyConditions: keyOpts.KeyConditions,
            KeyConditionExpression: keyOpts.KeyConditionExpression,
            ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
          }, 'The provided starting key is invalid: The provided key element does not match the schema', cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException if global range in ExclusiveStartKey but bad in query', function(done) {
      async.forEach([
        {x: {N: '1'}, y: {N: '1'}, c: {S: 'a'}, d: {S: 'a'}},
        {a: {N: '1'}, b: {S: '1'}, c: {S: 'a'}, d: {S: 'a'}},
        {a: {S: '1'}, b: {N: '1'}, c: {S: 'a'}, d: {S: 'a'}},
      ], function(expr, cb) {
        async.forEach([
          {KeyConditions: {c: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'b'}]}}},
          {KeyConditionExpression: 'c = :a', ExpressionAttributeValues: {':a': {S: 'b'}}},
        ], function(keyOpts, cb) {
          assertValidation({
            TableName: helpers.testRangeTable,
            IndexName: 'index4',
            ExclusiveStartKey: expr,
            KeyConditions: keyOpts.KeyConditions,
            KeyConditionExpression: keyOpts.KeyConditionExpression,
            ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
          }, 'The provided starting key is invalid: The provided key element does not match the schema', cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException for missing range element', function(done) {
      async.forEach([
        {KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'b'}]}, c: {ComparisonOperator: 'NULL'}}},
        {KeyConditionExpression: 'a = :a and c = :b', ExpressionAttributeValues: {':a': {S: 'b'}, ':b': {N: '1'}}},
      ], function(keyOpts, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          ExclusiveStartKey: {a: {S: 'a'}, b: {S: 'a'}},
          KeyConditions: keyOpts.KeyConditions,
          KeyConditionExpression: keyOpts.KeyConditionExpression,
          ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
        }, 'Query condition missed key schema element: b', cb)
      }, done)
    })

    it('should return ValidationException for ExclusiveStartKey with local index and missing part', function(done) {
      async.forEach([
        {KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'b'}]}, b: {ComparisonOperator: 'NULL'}}},
        {KeyConditionExpression: 'a = :a and b = :b', ExpressionAttributeValues: {':a': {S: 'b'}, ':b': {N: '1'}}},
      ], function(keyOpts, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          IndexName: 'index1',
          ExclusiveStartKey: {a: {S: 'a'}, b: {S: 'a'}, c: {S: 'a'}},
          KeyConditions: keyOpts.KeyConditions,
          KeyConditionExpression: keyOpts.KeyConditionExpression,
          ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
        }, 'Query condition missed key schema element: c', cb)
      }, done)
    })

    it('should return ValidationException for ExclusiveStartKey with global index and missing part', function(done) {
      async.forEach([
        {KeyConditions: {c: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'b'}]}, b: {ComparisonOperator: 'NULL'}}},
        {KeyConditionExpression: 'c = :a and b = :b', ExpressionAttributeValues: {':a': {S: 'b'}, ':b': {N: '1'}}},
      ], function(keyOpts, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          IndexName: 'index4',
          Select: 'ALL_ATTRIBUTES',
          ExclusiveStartKey: {a: {S: 'a'}, b: {S: 'a'}, c: {S: 'a'}, d: {S: 'a'}},
          KeyConditions: keyOpts.KeyConditions,
          KeyConditionExpression: keyOpts.KeyConditionExpression,
          ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
        }, 'Query condition missed key schema element: d', cb)
      }, done)
    })

    it('should return ValidationException if querying with non-indexable operations', function(done) {
      async.forEach([
        {ComparisonOperator: 'NULL'},
        {ComparisonOperator: 'NOT_NULL'},
        {ComparisonOperator: 'NE', AttributeValueList: [{N: '1'}]},
        {ComparisonOperator: 'CONTAINS', AttributeValueList: [{S: 'a'}]},
        {ComparisonOperator: 'NOT_CONTAINS', AttributeValueList: [{S: 'a'}]},
        {ComparisonOperator: 'IN', AttributeValueList: [{S: 'a'}]},
      ], function(keyOpts, cb) {
        assertValidation({
          TableName: helpers.testHashTable,
          KeyConditions: {a: keyOpts},
        }, 'Attempted conditional constraint is not an indexable operation', cb)
      }, done)
    })

    it('should return ValidationException for unsupported comparison on range', function(done) {
      async.forEach([
        {ComparisonOperator: 'NULL'},
        {ComparisonOperator: 'NOT_NULL'},
        {ComparisonOperator: 'CONTAINS', AttributeValueList: [{S: 'a'}]},
        {ComparisonOperator: 'NOT_CONTAINS', AttributeValueList: [{S: 'a'}]},
        {ComparisonOperator: 'IN', AttributeValueList: [{S: 'a'}]},
      ], function(keyOpts, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          ExclusiveStartKey: {a: {S: 'a'}, b: {S: 'a'}},
          KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'b'}]}, b: keyOpts},
        }, 'Attempted conditional constraint is not an indexable operation', cb)
      }, done)
    })

    it('should return ValidationException for incorrect comparison operator on index', function(done) {
      assertValidation({
        TableName: helpers.testRangeTable,
        IndexName: 'index1',
        KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'a'}]},
          c: {ComparisonOperator: 'NULL'},
        }},
        'Attempted conditional constraint is not an indexable operation', done)
    })

    it('should return ValidationException for mismatching param type', function(done) {
      var expressions = [
        ':a > a',
      ]
      async.forEach(expressions, function(expression, cb) {
        assertValidation({
          TableName: helpers.testHashTable,
          KeyConditionExpression: expression,
          ExpressionAttributeValues: {':a': {N: '1'}},
        }, 'One or more parameter values were invalid: Condition parameter type does not match schema type', cb)
      }, done)
    })

    it('should return ValidationException if querying with unsupported conditions', function(done) {
      async.forEach([
        {KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'a'}]}, b: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'a'}]}}},
        {KeyConditions: {a: {ComparisonOperator: 'LE', AttributeValueList: [{S: 'a'}]}}},
        {KeyConditions: {a: {ComparisonOperator: 'LT', AttributeValueList: [{S: 'a'}]}}},
        {KeyConditions: {a: {ComparisonOperator: 'GE', AttributeValueList: [{S: 'a'}]}}},
        {KeyConditions: {a: {ComparisonOperator: 'GT', AttributeValueList: [{S: 'a'}]}}},
        {KeyConditions: {a: {ComparisonOperator: 'BEGINS_WITH', AttributeValueList: [{S: 'a'}]}}},
        {KeyConditions: {a: {ComparisonOperator: 'BETWEEN', AttributeValueList: [{S: 'a'}, {S: 'a'}]}}},
        {KeyConditionExpression: 'a > :a', ExpressionAttributeValues: {':a': {S: '1'}}},
        {KeyConditionExpression: 'a < :a', ExpressionAttributeValues: {':a': {S: '1'}}},
        {KeyConditionExpression: ':a <= a', ExpressionAttributeValues: {':a': {S: '1'}}},
        {KeyConditionExpression: ':a >= a', ExpressionAttributeValues: {':a': {S: '1'}}},
        {KeyConditionExpression: 'begins_with(a, :a)', ExpressionAttributeValues: {':a': {S: '1'}}},
        {KeyConditionExpression: 'a between :a and :a', ExpressionAttributeValues: {':a': {S: '1'}}},
        {KeyConditionExpression: 'a = :a AND b = :a', ExpressionAttributeValues: {':a': {S: '1'}}},
        {KeyConditionExpression: 'y = :a and z = :a', ExpressionAttributeValues: {':a': {S: '1'}}},
      ], function(keyOpts, cb) {
        assertValidation({
          TableName: helpers.testHashTable,
          KeyConditions: keyOpts.KeyConditions,
          KeyConditionExpression: keyOpts.KeyConditionExpression,
          ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
        }, 'Query key condition not supported', cb)
      }, done)
    })

    it('should return ValidationException if querying global with unsupported conditions', function(done) {
      async.forEach([
        {KeyConditions: {c: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'a'}]}, z: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'a'}]}}},
        {KeyConditions: {c: {ComparisonOperator: 'LE', AttributeValueList: [{S: 'a'}]}}},
        {KeyConditions: {c: {ComparisonOperator: 'LT', AttributeValueList: [{S: 'a'}]}}},
        {KeyConditions: {c: {ComparisonOperator: 'GE', AttributeValueList: [{S: 'a'}]}}},
        {KeyConditions: {c: {ComparisonOperator: 'GT', AttributeValueList: [{S: 'a'}]}}},
        {KeyConditions: {c: {ComparisonOperator: 'BEGINS_WITH', AttributeValueList: [{S: 'a'}]}}},
        {KeyConditions: {c: {ComparisonOperator: 'BETWEEN', AttributeValueList: [{S: 'a'}, {S: 'a'}]}}},
        {KeyConditionExpression: 'c > :a', ExpressionAttributeValues: {':a': {S: '1'}}},
        {KeyConditionExpression: 'c < :a', ExpressionAttributeValues: {':a': {S: '1'}}},
        {KeyConditionExpression: ':a <= c', ExpressionAttributeValues: {':a': {S: '1'}}},
        {KeyConditionExpression: ':a >= c', ExpressionAttributeValues: {':a': {S: '1'}}},
        {KeyConditionExpression: 'begins_with(c, :a)', ExpressionAttributeValues: {':a': {S: '1'}}},
        {KeyConditionExpression: 'c between :a and :a', ExpressionAttributeValues: {':a': {S: '1'}}},
        {KeyConditionExpression: 'c = :a AND b = :a', ExpressionAttributeValues: {':a': {S: '1'}}},
        {KeyConditionExpression: 'y = :a and z = :a', ExpressionAttributeValues: {':a': {S: '1'}}},
      ], function(keyOpts, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          IndexName: 'index3',
          KeyConditions: keyOpts.KeyConditions,
          KeyConditionExpression: keyOpts.KeyConditionExpression,
          ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
        }, 'Query key condition not supported', cb)
      }, done)
    })

    it('should return ValidationException for ExclusiveStartKey with out-of-bounds hash key', function(done) {
      async.forEach([
        {KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'b'}]}}},
        {KeyConditionExpression: 'a = :a', ExpressionAttributeValues: {':a': {S: 'b'}}},
      ], function(keyOpts, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          ExclusiveStartKey: {a: {S: 'a'}, b: {S: 'a'}},
          KeyConditions: keyOpts.KeyConditions,
          KeyConditionExpression: keyOpts.KeyConditionExpression,
          ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
        }, 'The provided starting key is outside query boundaries based on provided conditions', cb)
      }, done)
    })

    it('should return ValidationException for ExclusiveStartKey with local index and out-of-bounds hash key', function(done) {
      async.forEach([
        {KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'b'}]}}},
        {KeyConditionExpression: 'a = :a', ExpressionAttributeValues: {':a': {S: 'b'}}},
      ], function(keyOpts, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          IndexName: 'index1',
          ExclusiveStartKey: {a: {S: 'a'}, b: {S: 'a'}, c: {S: 'a'}},
          KeyConditions: keyOpts.KeyConditions,
          KeyConditionExpression: keyOpts.KeyConditionExpression,
          ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
        }, 'The provided starting key is outside query boundaries based on provided conditions', cb)
      }, done)
    })

    it('should return ValidationException if global hash in ExclusiveStartKey but outside range', function(done) {
      async.forEach([
        {KeyConditions: {c: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'b'}]}}},
        {KeyConditionExpression: 'c = :a', ExpressionAttributeValues: {':a': {S: 'b'}}},
      ], function(keyOpts, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          IndexName: 'index3',
          ExclusiveStartKey: {a: {S: 'a'}, b: {S: 'a'}, c: {S: 'a'}},
          KeyConditions: keyOpts.KeyConditions,
          KeyConditionExpression: keyOpts.KeyConditionExpression,
          ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
        }, 'The provided starting key is outside query boundaries based on provided conditions', cb)
      }, done)
    })

    it('should return ValidationException if second global hash in ExclusiveStartKey but outside range', function(done) {
      async.forEach([
        {KeyConditions: {c: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'b'}]}}},
        {KeyConditionExpression: 'c = :a', ExpressionAttributeValues: {':a': {S: 'b'}}},
      ], function(keyOpts, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          IndexName: 'index4',
          ExclusiveStartKey: {a: {S: 'a'}, b: {S: 'a'}, c: {S: 'a'}, d: {S: 'a'}},
          KeyConditions: keyOpts.KeyConditions,
          KeyConditionExpression: keyOpts.KeyConditionExpression,
          ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
        }, 'The provided starting key is outside query boundaries based on provided conditions', cb)
      }, done)
    })

    it('should return ValidationException for ExclusiveStartKey with non-matching range key', function(done) {
      async.forEach([{
        KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'b'}]},
          b: {ComparisonOperator: 'GT', AttributeValueList: [{S: 'a'}]},
        },
      }, {
        KeyConditionExpression: 'a = :a and b > :a',
        ExpressionAttributeValues: {':a': {S: 'b'}},
      }], function(keyOpts, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          ExclusiveStartKey: {a: {S: 'a'}, b: {S: 'a'}},
          KeyConditions: keyOpts.KeyConditions,
          KeyConditionExpression: keyOpts.KeyConditionExpression,
          ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
        }, 'The provided starting key does not match the range key predicate', cb)
      }, done)
    })

    it('should return ValidationException for ExclusiveStartKey with local index and not matching predicate', function(done) {
      async.forEach([{
        KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'a'}]},
          c: {ComparisonOperator: 'GT', AttributeValueList: [{S: 'a'}]},
        },
      }, {
        KeyConditionExpression: 'a = :a and c > :a',
        ExpressionAttributeValues: {':a': {S: 'b'}},
      }], function(keyOpts, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          IndexName: 'index1',
          ExclusiveStartKey: {a: {S: 'a'}, b: {S: 'a'}, c: {S: 'a'}},
          KeyConditions: keyOpts.KeyConditions,
          KeyConditionExpression: keyOpts.KeyConditionExpression,
          ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
        }, 'The provided starting key does not match the range key predicate', cb)
      }, done)
    })

    it('should return ValidationException if global hash in ExclusiveStartKey but not matching predicate', function(done) {
      async.forEach([{
        KeyConditions: {
          c: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'a'}]},
          d: {ComparisonOperator: 'GT', AttributeValueList: [{S: 'a'}]},
        },
      }, {
        KeyConditionExpression: 'c = :a and d > :a',
        ExpressionAttributeValues: {':a': {S: 'b'}},
      }], function(keyOpts, cb) {
        async.forEach([
          {a: {S: 'a'}, b: {S: 'a'}, c: {S: 'a'}, d: {S: 'a'}},
          {a: {S: 'a'}, b: {S: 'a'}, c: {S: 'b'}, d: {S: 'a'}},
        ], function(expr, cb) {
          assertValidation({
            TableName: helpers.testRangeTable,
            IndexName: 'index4',
            ExclusiveStartKey: expr,
            KeyConditions: keyOpts.KeyConditions,
            KeyConditionExpression: keyOpts.KeyConditionExpression,
            ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
          }, 'The provided starting key does not match the range key predicate', cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException for ExclusiveStartKey with matching range but non-matching hash key', function(done) {
      async.forEach([{
        KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'b'}]},
          b: {ComparisonOperator: 'LT', AttributeValueList: [{S: 'b'}]},
        },
      }, {
        KeyConditionExpression: 'a = :a and b < :a',
        ExpressionAttributeValues: {':a': {S: 'b'}},
      }], function(keyOpts, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          ExclusiveStartKey: {a: {S: 'a'}, b: {S: 'a'}},
          KeyConditions: keyOpts.KeyConditions,
          KeyConditionExpression: keyOpts.KeyConditionExpression,
          ExpressionAttributeValues: keyOpts.ExpressionAttributeValues,
        }, 'The query can return at most one row and cannot be restarted', cb)
      }, done)
    })

    it('should return ValidationException if global hash in ExclusiveStartKey but exact match', function(done) {
      async.forEach([
        {a: {S: 'a'}, b: {S: 'a'}, c: {S: 'c'}, d: {S: 'a'}},
        {a: {S: 'a'}, b: {S: 'a'}, c: {S: 'b'}, d: {S: 'a'}},
      ], function(expr, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          IndexName: 'index4',
          ExclusiveStartKey: expr,
          KeyConditions: {
            c: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'a'}]},
            d: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'a'}]},
          },
        }, 'The query can return at most one row and cannot be restarted', cb)
      }, done)
    })

    it('should return ValidationException if hash key in QueryFilter', function(done) {
      assertValidation({
        TableName: helpers.testHashTable,
        KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'a'}]}},
        QueryFilter: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'a'}]}},
      }, 'QueryFilter can only contain non-primary key attributes: Primary key attribute: a', done)
    })

    it('should return ValidationException if hash key in FilterExpression', function(done) {
      async.forEach([
        'attribute_exists(a.b) AND b = :b',
        'a = :b',
        'a[1] = :b',
      ], function(expr, cb) {
        assertValidation({
          TableName: helpers.testHashTable,
          FilterExpression: expr,
          KeyConditionExpression: 'a = :a',
          ExpressionAttributeValues: {':a': {S: '1'}, ':b': {N: '1'}},
        }, 'Filter Expression can only contain non-primary key attributes: Primary key attribute: a', cb)
      }, done)
    })

    it('should return ValidationException if range key in QueryFilter', function(done) {
      assertValidation({
        TableName: helpers.testRangeTable,
        KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'a'}]}},
        QueryFilter: {b: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'a'}]}},
      }, 'QueryFilter can only contain non-primary key attributes: Primary key attribute: b', done)
    })

    it('should return ValidationException if global range key in QueryFilter', function(done) {
      assertValidation({
        TableName: helpers.testRangeTable,
        IndexName: 'index4',
        Select: 'ALL_ATTRIBUTES',
        KeyConditions: {c: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'a'}]}},
        QueryFilter: {d: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'a'}]}},
      }, 'QueryFilter can only contain non-primary key attributes: Primary key attribute: d', done)
    })

    it('should return ValidationException if range key in FilterExpression', function(done) {
      async.forEach([
        'attribute_exists(b.c) AND c = :b',
        'b = :b',
        'b[1] = :b',
      ], function(expr, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          FilterExpression: expr,
          KeyConditionExpression: 'a = :a',
          ExpressionAttributeValues: {':a': {S: '1'}, ':b': {N: '1'}},
        }, 'Filter Expression can only contain non-primary key attributes: Primary key attribute: b', cb)
      }, done)
    })

    it('should return ValidationException for non-scalar index access in FilterExpression', function(done) {
      async.forEach([
        'attribute_exists(d.c) AND c = :b',
        'd[1] = :b',
      ], function(expr, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          FilterExpression: expr,
          KeyConditionExpression: 'a = :a',
          ExpressionAttributeValues: {':a': {S: '1'}, ':b': {N: '1'}},
        }, 'Key attributes must be scalars; list random access \'[]\' and map lookup \'.\' are not allowed: IndexKey: d', cb)
      }, done)
    })

    it('should return ValidationException for specifying ALL_ATTRIBUTES when global index does not have ALL', function(done) {
      async.forEach([{
        KeyConditions: {c: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'a'}]}},
        QueryFilter: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{N: '1'}]}},
      }, {
        KeyConditionExpression: 'c = :a',
        FilterExpression: 'a = :b and a.b = :a',
        ExpressionAttributeValues: {':a': {S: 'a'}, ':b': {N: '1'}},
      }], function(queryOpts, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          IndexName: 'index4',
          Select: 'ALL_ATTRIBUTES',
          ExclusiveStartKey: {a: {S: 'a'}, b: {S: 'b'}, c: {S: 'a'}, d: {S: 'b'}},
          KeyConditions: queryOpts.KeyConditions,
          QueryFilter: queryOpts.QueryFilter,
          KeyConditionExpression: queryOpts.KeyConditionExpression,
          FilterExpression: queryOpts.FilterExpression,
          ExpressionAttributeValues: queryOpts.ExpressionAttributeValues,
        }, 'One or more parameter values were invalid: ' +
          'Select type ALL_ATTRIBUTES is not supported for global secondary index index4 ' +
          'because its projection type is not ALL', cb)
      }, done)
    })
  })

  describe('functionality', function() {

    it('should query a hash table when empty', function(done) {
      async.forEach([{
        KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: helpers.randomString()}]}},
      }, {
        KeyConditionExpression: 'a = :a',
        ExpressionAttributeValues: {':a': {S: helpers.randomString()}},
      }], function(queryOpts, cb) {
        queryOpts.TableName = helpers.testHashTable
        queryOpts.ConsistentRead = false
        queryOpts.ReturnConsumedCapacity = 'NONE'
        queryOpts.ScanIndexForward = true
        queryOpts.Select = 'ALL_ATTRIBUTES'
        request(opts(queryOpts), function(err, res) {
          if (err) return cb(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({Count: 0, ScannedCount: 0, Items: []})
          cb()
        })
      }, done)
    })

    it('should query a hash table with items', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {N: helpers.randomNumber()}},
          item2 = {a: {S: helpers.randomString()}, b: item.b},
          item3 = {a: {S: helpers.randomString()}, b: {N: helpers.randomNumber()}},
          items = [item, item2, item3]
      helpers.batchBulkPut(helpers.testHashTable, items, function(err) {
        if (err) return done(err)
        async.forEach([{
          QueryFilter: {},
          KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [item2.a]}},
        }, {
          KeyConditionExpression: 'a = :a',
          ExpressionAttributeValues: {':a': item2.a},
        }], function(queryOpts, cb) {
          queryOpts.TableName = helpers.testHashTable
          queryOpts.ConsistentRead = true
          request(opts(queryOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({Count: 1, ScannedCount: 1, Items: [item2]})
            cb()
          })
        }, done)
      })
    })

    it('should query a range table with EQ on just hash key', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: '1'}},
          item2 = {a: item.a, b: {S: '2'}},
          item3 = {a: item.a, b: {S: '3'}},
          items = [item, item2, item3]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        async.forEach([{
          KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]}},
        }, {
          KeyConditionExpression: 'a = :a',
          ExpressionAttributeValues: {':a': item.a},
        }], function(queryOpts, cb) {
          queryOpts.TableName = helpers.testRangeTable
          queryOpts.ConsistentRead = true
          request(opts(queryOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({Count: 3, ScannedCount: 3, Items: [item, item2, item3]})
            cb()
          })
        }, done)
      })
    })

    it('should query a range table with EQ', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: '1'}},
          item2 = {a: item.a, b: {S: '2'}},
          item3 = {a: item.a, b: {S: '3'}},
          items = [item, item2, item3]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        async.forEach([{
          KeyConditions: {
            a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
            b: {ComparisonOperator: 'EQ', AttributeValueList: [item2.b]},
          },
        }, {
          KeyConditionExpression: 'a = :a AND b = :b',
          ExpressionAttributeValues: {':a': item.a, ':b': item2.b},
        }], function(queryOpts, cb) {
          queryOpts.TableName = helpers.testRangeTable
          queryOpts.ConsistentRead = true
          request(opts(queryOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({Count: 1, ScannedCount: 1, Items: [item2]})
            cb()
          })
        }, done)
      })
    })

    it('should query a range table with LE', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: '1'}},
          item2 = {a: item.a, b: {S: '2'}},
          item3 = {a: item.a, b: {S: '3'}},
          items = [item, item2, item3]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        async.forEach([{
          KeyConditions: {
            a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
            b: {ComparisonOperator: 'LE', AttributeValueList: [item2.b]},
          },
        }, {
          KeyConditionExpression: 'a = :a AND b <= :b',
          ExpressionAttributeValues: {':a': item.a, ':b': item2.b},
        }], function(queryOpts, cb) {
          queryOpts.TableName = helpers.testRangeTable
          queryOpts.ConsistentRead = true
          request(opts(queryOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({Count: 2, ScannedCount: 2, Items: [item, item2]})
            cb()
          })
        }, done)
      })
    })

    it('should query a range table with LT', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: '1'}},
          item2 = {a: item.a, b: {S: '2'}},
          item3 = {a: item.a, b: {S: '3'}},
          items = [item, item2, item3]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        async.forEach([{
          KeyConditions: {
            a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
            b: {ComparisonOperator: 'LT', AttributeValueList: [item2.b]},
          },
        }, {
          KeyConditionExpression: 'a = :a AND b < :b',
          ExpressionAttributeValues: {':a': item.a, ':b': item2.b},
        }], function(queryOpts, cb) {
          queryOpts.TableName = helpers.testRangeTable
          queryOpts.ConsistentRead = true
          request(opts(queryOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({Count: 1, ScannedCount: 1, Items: [item]})
            cb()
          })
        }, done)
      })
    })

    it('should query a range table with GE', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: '1'}},
          item2 = {a: item.a, b: {S: '2'}},
          item3 = {a: item.a, b: {S: '3'}},
          items = [item, item2, item3]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        async.forEach([{
          KeyConditions: {
            a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
            b: {ComparisonOperator: 'GE', AttributeValueList: [item2.b]},
          },
        }, {
          KeyConditionExpression: 'a = :a AND b >= :b',
          ExpressionAttributeValues: {':a': item.a, ':b': item2.b},
        }], function(queryOpts, cb) {
          queryOpts.TableName = helpers.testRangeTable
          queryOpts.ConsistentRead = true
          request(opts(queryOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({Count: 2, ScannedCount: 2, Items: [item2, item3]})
            cb()
          })
        }, done)
      })
    })

    it('should query a range table with GT', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: '1'}},
          item2 = {a: item.a, b: {S: '2'}},
          item3 = {a: item.a, b: {S: '3'}},
          items = [item, item2, item3]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        async.forEach([{
          KeyConditions: {
            a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
            b: {ComparisonOperator: 'GT', AttributeValueList: [item2.b]},
          },
        }, {
          KeyConditionExpression: 'a = :a AND b > :b',
          ExpressionAttributeValues: {':a': item.a, ':b': item2.b},
        }], function(queryOpts, cb) {
          queryOpts.TableName = helpers.testRangeTable
          queryOpts.ConsistentRead = true
          request(opts(queryOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({Count: 1, ScannedCount: 1, Items: [item3]})
            cb()
          })
        }, done)
      })
    })

    it('should query a range table with BEGINS_WITH', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'aaa'}},
          item2 = {a: item.a, b: {S: 'aab'}},
          item3 = {a: item.a, b: {S: 'abc'}},
          items = [item, item2, item3]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        async.forEach([{
          KeyConditions: {
            a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
            b: {ComparisonOperator: 'BEGINS_WITH', AttributeValueList: [{S: 'aa'}]},
          },
        }, {
          KeyConditionExpression: 'a = :a AND begins_with(b, :b)',
          ExpressionAttributeValues: {':a': item.a, ':b': {S: 'aa'}},
        }], function(queryOpts, cb) {
          queryOpts.TableName = helpers.testRangeTable
          queryOpts.ConsistentRead = true
          request(opts(queryOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({Count: 2, ScannedCount: 2, Items: [item, item2]})
            cb()
          })
        }, done)
      })
    })

    it('should query a range table with BETWEEN', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'aa'}},
          item2 = {a: item.a, b: {S: 'ab'}},
          item3 = {a: item.a, b: {S: 'abc'}},
          item4 = {a: item.a, b: {S: 'ac'}},
          item5 = {a: item.a, b: {S: 'aca'}},
          items = [item, item2, item3, item4, item5]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        async.forEach([{
          KeyConditions: {
            a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
            b: {ComparisonOperator: 'BETWEEN', AttributeValueList: [{S: 'ab'}, {S: 'ac'}]},
          },
        }, {
          KeyConditionExpression: 'a = :a AND b BETWEEN :b AND :c',
          ExpressionAttributeValues: {':a': item.a, ':b': {S: 'ab'}, ':c': {S: 'ac'}},
        }], function(queryOpts, cb) {
          queryOpts.TableName = helpers.testRangeTable
          queryOpts.ConsistentRead = true
          request(opts(queryOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({Count: 3, ScannedCount: 3, Items: [item2, item3, item4]})
            cb()
          })
        }, done)
      })
    })

    it('should only return requested attributes', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'b1'}, d: {S: 'd1'}},
          item2 = {a: item.a, b: {S: 'b2'}},
          item3 = {a: item.a, b: {S: 'b3'}, d: {S: 'd3'}, e: {S: 'e3'}},
          items = [item, item2, item3]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        async.forEach([{
          KeyConditions: {
            a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
          },
          AttributesToGet: ['b', 'd'],
        }, {
          KeyConditions: {
            a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
          },
          ProjectionExpression: 'b, d',
        }, {
          KeyConditionExpression: 'a = :a',
          ExpressionAttributeValues: {':a': item.a},
          ProjectionExpression: 'b, d',
        }, {
          KeyConditionExpression: 'a = :a',
          ExpressionAttributeValues: {':a': item.a},
          ExpressionAttributeNames: {'#b': 'b', '#d': 'd'},
          ProjectionExpression: '#b, #d',
        }], function(queryOpts, cb) {
          queryOpts.TableName = helpers.testRangeTable
          queryOpts.ConsistentRead = true
          request(opts(queryOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({Count: 3, ScannedCount: 3, Items: [
              {b: {S: 'b1'}, d: {S: 'd1'}},
              {b: {S: 'b2'}},
              {b: {S: 'b3'}, d: {S: 'd3'}},
            ]})
            cb()
          })
        }, done)
      })
    })

    it('should only return requested nested attributes', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'b1'}, e: {M: {a: {S: 'b1'}, d: {S: 'b1'}}}, f: {L: [{S: 'd1'}, {S: 'd2'}, {S: 'd3'}]}},
          item2 = {a: item.a, b: {S: 'b2'}},
          item3 = {a: item.a, b: {S: 'b3'}, d: {S: 'd3'}, e: {S: 'e3'}},
          items = [item, item2, item3]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        async.forEach([{
          KeyConditions: {
            a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
          },
          ProjectionExpression: 'f[2], f[0], e.d, e.a, d',
        }, {
          KeyConditionExpression: 'a = :a',
          ExpressionAttributeValues: {':a': item.a},
          ProjectionExpression: 'f[2], f[0], e.d, e.a, d',
        }, {
          KeyConditionExpression: 'a = :a',
          ExpressionAttributeValues: {':a': item.a},
          ExpressionAttributeNames: {'#f': 'f', '#e': 'e', '#a': 'a'},
          ProjectionExpression: '#f[2],#f[0],#e.d,e.#a,d',
        }], function(queryOpts, cb) {
          queryOpts.TableName = helpers.testRangeTable
          queryOpts.ConsistentRead = true
          request(opts(queryOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({Count: 3, ScannedCount: 3, Items: [
              {e: {M: {a: {S: 'b1'}, d: {S: 'b1'}}}, f: {L: [{S: 'd1'}, {S: 'd3'}]}},
              {},
              {d: {S: 'd3'}},
            ]})
            cb()
          })
        }, done)
      })
    })

    it('should filter items by query filter', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'b1'}, d: {S: '1'}},
          item2 = {a: item.a, b: {S: 'b2'}},
          item3 = {a: item.a, b: {S: 'b3'}, d: {S: 'd3'}, e: {S: 'e3'}},
          items = [item, item2, item3]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        async.forEach([{
          KeyConditions: {
            a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
          },
          QueryFilter: {
            e: {ComparisonOperator: 'NOT_NULL'},
          },
        }, {
          KeyConditionExpression: 'a = :a',
          ExpressionAttributeValues: {':a': item.a},
          FilterExpression: 'attribute_exists(e)',
        }], function(queryOpts, cb) {
          queryOpts.TableName = helpers.testRangeTable
          queryOpts.ConsistentRead = true
          request(opts(queryOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({Count: 1, ScannedCount: 3, Items: [
              {a: item.a, b: {S: 'b3'}, d: {S: 'd3'}, e: {S: 'e3'}},
            ]})
            cb()
          })
        }, done)
      })
    })

    it('should only return projected attributes by default for secondary indexes', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'b1'}, c: {S: 'c1'}, d: {S: 'd1'}},
          item2 = {a: item.a, b: {S: 'b2'}},
          item3 = {a: item.a, b: {S: 'b3'}, d: {S: 'd3'}, e: {S: 'e3'}, f: {S: 'f3'}},
          item4 = {a: item.a, b: {S: 'b4'}, c: {S: 'c4'}, d: {S: 'd4'}, e: {S: 'e4'}},
          items = [item, item2, item3, item4]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        var req = {TableName: helpers.testRangeTable, ConsistentRead: true, IndexName: 'index2',
          KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]}},
          ReturnConsumedCapacity: 'TOTAL'}
        request(opts(req), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          delete item3.e
          delete item3.f
          delete item4.e
          res.body.should.eql({
            Count: 3,
            ScannedCount: 3,
            Items: [item, item3, item4],
            ConsumedCapacity: {CapacityUnits: 1, TableName: helpers.testRangeTable},
          })
          req.ReturnConsumedCapacity = 'INDEXES'
          request(opts(req), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({
              Count: 3,
              ScannedCount: 3,
              Items: [item, item3, item4],
              ConsumedCapacity: {
                CapacityUnits: 1,
                TableName: helpers.testRangeTable,
                Table: {CapacityUnits: 0},
                LocalSecondaryIndexes: {index2: {CapacityUnits: 1}},
              },
            })
            done()
          })
        })
      })
    })

    it('should return all attributes when specified for secondary indexes', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'b1'}, c: {S: 'c1'}, d: {S: 'd1'}},
          item2 = {a: item.a, b: {S: 'b2'}},
          item3 = {a: item.a, b: {S: 'b3'}, d: {S: 'd3'}, e: {M: {e3: {S: new Array(4062).join('e')}}}, f: {L: [{S: 'f3'}, {S: 'ff3'}]}},
          item4 = {a: item.a, b: {S: 'b4'}, c: {S: 'c4'}, d: {S: 'd4'}, e: {M: {ee4: {S: 'e4'}, eee4: {S: new Array(4062).join('e')}}}},
          items = [item, item2, item3, item4]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        var req = {TableName: helpers.testRangeTable, ConsistentRead: true, IndexName: 'index2',
          KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]}},
          Select: 'ALL_ATTRIBUTES', ReturnConsumedCapacity: 'TOTAL'}
        request(opts(req), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Count: 3,
            ScannedCount: 3,
            Items: [item, item3, item4],
            ConsumedCapacity: {CapacityUnits: 4, TableName: helpers.testRangeTable},
          })
          req.ReturnConsumedCapacity = 'INDEXES'
          request(opts(req), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({
              Count: 3,
              ScannedCount: 3,
              Items: [item, item3, item4],
              ConsumedCapacity: {
                CapacityUnits: 4,
                TableName: helpers.testRangeTable,
                Table: {CapacityUnits: 3},
                LocalSecondaryIndexes: {index2: {CapacityUnits: 1}},
              },
            })
            done()
          })
        })
      })
    })

    it('should return COUNT if requested', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: '2'}},
          item2 = {a: item.a, b: {S: '1'}},
          item3 = {a: item.a, b: {S: '3'}},
          item4 = {a: item.a, b: {S: '4'}},
          item5 = {a: item.a, b: {S: '5'}},
          items = [item, item2, item3, item4, item5]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        request(opts({TableName: helpers.testRangeTable, ConsistentRead: true, KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
          b: {ComparisonOperator: 'GE', AttributeValueList: [item.b]},
        }, Select: 'COUNT'}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          should.not.exist(res.body.Items)
          res.body.should.eql({Count: 4, ScannedCount: 4})
          done()
        })
      })
    })

    it('should only return Limit items if requested', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: '2'}, c: {S: 'c'}},
          item2 = {a: item.a, b: {S: '1'}, c: {S: 'c'}},
          item3 = {a: item.a, b: {S: '3'}, c: {S: 'c'}},
          item4 = {a: item.a, b: {S: '4'}, c: {S: 'c'}},
          item5 = {a: item.a, b: {S: '5'}, c: {S: 'c'}},
          items = [item, item2, item3, item4, item5]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        request(opts({TableName: helpers.testRangeTable, ConsistentRead: true, KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
          b: {ComparisonOperator: 'GE', AttributeValueList: [item.b]},
        }, Limit: 2}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({Count: 2, ScannedCount: 2, Items: [item, item3], LastEvaluatedKey: {a: item3.a, b: item3.b}})
          done()
        })
      })
    })

    it('should only return Limit items if requested and QueryFilter', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: '2'}, c: {S: 'c'}},
          item2 = {a: item.a, b: {S: '1'}, c: {S: 'c'}},
          item3 = {a: item.a, b: {S: '3'}, c: {S: 'c'}, d: {S: 'd'}},
          item4 = {a: item.a, b: {S: '4'}, c: {S: 'c'}},
          item5 = {a: item.a, b: {S: '5'}, c: {S: 'c'}},
          items = [item, item2, item3, item4, item5]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        async.forEach([{
          KeyConditions: {
            a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
            b: {ComparisonOperator: 'GE', AttributeValueList: [item.b]},
          },
          QueryFilter: {
            d: {ComparisonOperator: 'EQ', AttributeValueList: [item3.d]},
          },
        }, {
          KeyConditionExpression: 'a = :a AND b >= :b',
          ExpressionAttributeValues: {':a': item.a, ':b': item.b, ':d': item3.d},
          FilterExpression: 'd = :d',
        }], function(queryOpts, cb) {
          queryOpts.TableName = helpers.testRangeTable
          queryOpts.ConsistentRead = true
          queryOpts.Limit = 2
          request(opts(queryOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({Count: 1, ScannedCount: 2, Items: [item3], LastEvaluatedKey: {a: item3.a, b: item3.b}})
            cb()
          })
        }, done)
      })
    })

    it('should return LastEvaluatedKey even if only Count is selected', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: '2'}, c: {S: 'c'}},
          item2 = {a: item.a, b: {S: '1'}, c: {S: 'c'}},
          item3 = {a: item.a, b: {S: '3'}, c: {S: 'c'}},
          item4 = {a: item.a, b: {S: '4'}, c: {S: 'c'}},
          item5 = {a: item.a, b: {S: '5'}, c: {S: 'c'}},
          items = [item, item2, item3, item4, item5]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        request(opts({TableName: helpers.testRangeTable, ConsistentRead: true, KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
          b: {ComparisonOperator: 'GE', AttributeValueList: [item.b]},
        }, Limit: 2, Select: 'COUNT'}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({Count: 2, ScannedCount: 2, LastEvaluatedKey: {a: item3.a, b: item3.b}})
          done()
        })
      })
    })

    it('should return LastEvaluatedKey even if only Count is selected and QueryFilter', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: '2'}, c: {S: 'c'}},
          item2 = {a: item.a, b: {S: '1'}, c: {S: 'c'}},
          item3 = {a: item.a, b: {S: '3'}, c: {S: 'c'}, d: {S: 'd'}},
          item4 = {a: item.a, b: {S: '4'}, c: {S: 'c'}},
          item5 = {a: item.a, b: {S: '5'}, c: {S: 'c'}},
          items = [item, item2, item3, item4, item5]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        request(opts({TableName: helpers.testRangeTable, ConsistentRead: true, KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
          b: {ComparisonOperator: 'GE', AttributeValueList: [item.b]},
        }, QueryFilter: {
          d: {ComparisonOperator: 'EQ', AttributeValueList: [item3.d]},
        }, Limit: 2, Select: 'COUNT'}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({Count: 1, ScannedCount: 2, LastEvaluatedKey: {a: item3.a, b: item3.b}})
          done()
        })
      })
    })

    it('should not return LastEvaluatedKey if Limit is at least size of response', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: '1'}, c: {S: 'c'}},
          item2 = {a: item.a, b: {S: '2'}, c: {S: 'c'}},
          item3 = {a: {S: helpers.randomString()}, b: {S: '1'}, c: {S: 'c'}},
          item4 = {a: item3.a, b: {S: '2'}, c: {S: 'c'}}

      helpers.replaceTable(helpers.testRangeTable, ['a', 'b'], [item, item2, item3, item4], function(err) {
        if (err) return done(err)

        request(helpers.opts('Scan', {TableName: helpers.testRangeTable}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          var lastHashItem = res.body.Items[res.body.Items.length - 1],
              lastHashItems = res.body.Items.filter(function(item) { return item.a.S == lastHashItem.a.S }),
              otherHashItem = lastHashItem.a.S == item.a.S ? item3 : item,
              otherHashItems = res.body.Items.filter(function(item) { return item.a.S == otherHashItem.a.S })
          otherHashItems.length.should.equal(2)
          request(opts({TableName: helpers.testRangeTable, ConsistentRead: true, KeyConditions: {
            a: {ComparisonOperator: 'EQ', AttributeValueList: [lastHashItem.a]},
          }}), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({Count: lastHashItems.length, ScannedCount: lastHashItems.length, Items: lastHashItems})
            request(opts({TableName: helpers.testRangeTable, ConsistentRead: true, KeyConditions: {
              a: {ComparisonOperator: 'EQ', AttributeValueList: [lastHashItem.a]},
            }, Limit: lastHashItems.length}), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)
              res.body.should.eql({Count: lastHashItems.length, ScannedCount: lastHashItems.length, Items: lastHashItems, LastEvaluatedKey: {a: lastHashItem.a, b: lastHashItem.b}})
              request(opts({TableName: helpers.testRangeTable, ConsistentRead: true, KeyConditions: {
                a: {ComparisonOperator: 'EQ', AttributeValueList: [otherHashItem.a]},
              }, Limit: 2}), function(err, res) {
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

    it('should return items in order for strings', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: '1'}},
          item2 = {a: item.a, b: {S: '2'}},
          item3 = {a: item.a, b: {S: '10'}},
          item4 = {a: item.a, b: {S: 'a'}},
          item5 = {a: item.a, b: {S: 'b'}},
          item6 = {a: item.a, b: {S: 'aa'}},
          item7 = {a: item.a, b: {S: 'ab'}},
          item8 = {a: item.a, b: {S: 'A'}},
          item9 = {a: item.a, b: {S: 'B'}},
          items = [item, item2, item3, item4, item5, item6, item7, item8, item9]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        request(opts({TableName: helpers.testRangeTable, ConsistentRead: true, KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
        }}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({Count: 9, ScannedCount: 9, Items: [item, item3, item2, item8, item9, item4, item6, item7, item5]})
          done()
        })
      })
    })

    it('should return items in order for secondary index strings', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: '1'}, c: {S: '1'}, d: {S: '1'}},
          item2 = {a: item.a, b: {S: '2'}, c: {S: '2'}},
          item3 = {a: item.a, b: {S: '3'}, c: {S: '10'}},
          item4 = {a: item.a, b: {S: '4'}, c: {S: 'a'}},
          item5 = {a: item.a, b: {S: '5'}, c: {S: 'b'}},
          item6 = {a: item.a, b: {S: '6'}, c: {S: 'aa'}, e: {S: '6'}},
          item7 = {a: item.a, b: {S: '7'}, c: {S: 'ab'}},
          item8 = {a: item.a, b: {S: '8'}, c: {S: 'A'}},
          item9 = {a: item.a, b: {S: '9'}, c: {S: 'B'}},
          items = [item, item2, item3, item4, item5, item6, item7, item8, item9]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        var req = {TableName: helpers.testRangeTable, IndexName: 'index1',
          KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]}}, ReturnConsumedCapacity: 'TOTAL'}
        request(opts(req), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Count: 9,
            ScannedCount: 9,
            Items: [item, item3, item2, item8, item9, item4, item6, item7, item5],
            ConsumedCapacity: {
              CapacityUnits: 0.5,
              TableName: helpers.testRangeTable,
            },
          })
          req.ReturnConsumedCapacity = 'INDEXES'
          request(opts(req), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({
              Count: 9,
              ScannedCount: 9,
              Items: [item, item3, item2, item8, item9, item4, item6, item7, item5],
              ConsumedCapacity: {
                CapacityUnits: 0.5,
                TableName: helpers.testRangeTable,
                Table: {CapacityUnits: 0},
                LocalSecondaryIndexes: {index1: {CapacityUnits: 0.5}},
              },
            })
            done()
          })
        })
      })
    })

    it('should calculate comparisons correctly for secondary indexes', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: '1'}, c: {S: '1'}, d: {S: '1'}},
          item2 = {a: item.a, b: {S: '2'}, c: {S: '2'}},
          item3 = {a: item.a, b: {S: '3'}, c: {S: '10'}},
          item4 = {a: item.a, b: {S: '4'}, c: {S: 'a'}},
          item5 = {a: item.a, b: {S: '5'}, c: {S: 'b'}},
          item6 = {a: item.a, b: {S: '6'}, c: {S: 'aa'}, e: {S: '6'}},
          item7 = {a: item.a, b: {S: '7'}, c: {S: 'ab'}},
          item8 = {a: item.a, b: {S: '8'}, c: {S: 'A'}},
          item9 = {a: item.a, b: {S: '9'}, c: {S: 'B'}},
          items = [item, item2, item3, item4, item5, item6, item7, item8, item9]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        var req = {
          TableName: helpers.testRangeTable,
          IndexName: 'index1',
          KeyConditionExpression: 'a = :a AND c <= :c',
          ExpressionAttributeValues: {':a': item.a, ':c': item4.c},
        }
        request(opts(req), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Count: 6,
            ScannedCount: 6,
            Items: [item, item3, item2, item8, item9, item4],
          })
          req.KeyConditionExpression = 'a = :a AND c = :c'
          request(opts(req), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({
              Count: 1,
              ScannedCount: 1,
              Items: [item4],
            })
            req.KeyConditionExpression = 'a = :a AND c >= :c'
            request(opts(req), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)
              res.body.should.eql({
                Count: 4,
                ScannedCount: 4,
                Items: [item4, item6, item7, item5],
              })
              req.KeyConditionExpression = 'a = :a AND c > :c'
              request(opts(req), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)
                res.body.should.eql({
                  Count: 3,
                  ScannedCount: 3,
                  Items: [item6, item7, item5],
                })
                req.KeyConditionExpression = 'a = :a AND c < :c'
                request(opts(req), function(err, res) {
                  if (err) return done(err)
                  res.statusCode.should.equal(200)
                  res.body.should.eql({
                    Count: 5,
                    ScannedCount: 5,
                    Items: [item, item3, item2, item8, item9],
                  })
                  req.KeyConditionExpression = 'a = :a AND c BETWEEN :c AND :d'
                  req.ExpressionAttributeValues[':d'] = item7.c
                  request(opts(req), function(err, res) {
                    if (err) return done(err)
                    res.statusCode.should.equal(200)
                    res.body.should.eql({
                      Count: 3,
                      ScannedCount: 3,
                      Items: [item4, item6, item7],
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

    it('should return items in order for numbers', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {N: '0'}},
          item2 = {a: item.a, b: {N: '99.1'}},
          item3 = {a: item.a, b: {N: '10.9'}},
          item4 = {a: item.a, b: {N: '10.1'}},
          item5 = {a: item.a, b: {N: '9.1'}},
          item6 = {a: item.a, b: {N: '9'}},
          item7 = {a: item.a, b: {N: '1.9'}},
          item8 = {a: item.a, b: {N: '1.1'}},
          item9 = {a: item.a, b: {N: '1'}},
          item10 = {a: item.a, b: {N: '0.9'}},
          item11 = {a: item.a, b: {N: '0.1'}},
          item12 = {a: item.a, b: {N: '0.09'}},
          item13 = {a: item.a, b: {N: '0.01'}},
          item14 = {a: item.a, b: {N: '-0.01'}},
          item15 = {a: item.a, b: {N: '-0.09'}},
          item16 = {a: item.a, b: {N: '-0.1'}},
          item17 = {a: item.a, b: {N: '-0.9'}},
          item18 = {a: item.a, b: {N: '-1'}},
          item19 = {a: item.a, b: {N: '-1.01'}},
          item20 = {a: item.a, b: {N: '-9'}},
          item21 = {a: item.a, b: {N: '-9.9'}},
          item22 = {a: item.a, b: {N: '-10.1'}},
          item23 = {a: item.a, b: {N: '-99.1'}},
          items = [item, item2, item3, item4, item5, item6, item7, item8, item9, item10, item11, item12,
            item13, item14, item15, item16, item17, item18, item19, item20, item21, item22, item23]
      helpers.batchBulkPut(helpers.testRangeNTable, items, function(err) {
        if (err) return done(err)
        request(opts({TableName: helpers.testRangeNTable, ConsistentRead: true, KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
        }}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({Count: 23, ScannedCount: 23, Items: [item23, item22, item21, item20, item19, item18, item17, item16, item15,
            item14, item, item13, item12, item11, item10, item9, item8, item7, item6, item5, item4, item3, item2]})
          done()
        })
      })
    })

    it('should return items in order for binary', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {B: '1Py5xA=='}},
          item2 = {a: item.a, b: {B: 'JA=='}},
          item3 = {a: item.a, b: {B: '2w=='}},
          item4 = {a: item.a, b: {B: 'cAeRhZE='}},
          item5 = {a: item.a, b: {B: '6piVtA=='}},
          item6 = {a: item.a, b: {B: 'MjA0'}},
          item7 = {a: item.a, b: {B: '1g=='}},
          item8 = {a: item.a, b: {B: 'ER/jLQ=='}},
          item9 = {a: item.a, b: {B: 'T7MzEUw='}},
          item10 = {a: item.a, b: {B: '9FkiOH0='}},
          item11 = {a: item.a, b: {B: 'Iv/a'}},
          item12 = {a: item.a, b: {B: '9V0='}},
          items = [item, item2, item3, item4, item5, item6, item7, item8, item9, item10, item11, item12]
      helpers.batchBulkPut(helpers.testRangeBTable, items, function(err) {
        if (err) return done(err)
        request(opts({TableName: helpers.testRangeBTable, ConsistentRead: true, KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
        }}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({Count: 12, ScannedCount: 12, Items: [item8, item11, item2, item6, item9, item4,
            item, item7, item3, item5, item10, item12]})
          done()
        })
      })
    })

    it('should return items in reverse order for strings', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: '1'}},
          item2 = {a: item.a, b: {S: '2'}},
          item3 = {a: item.a, b: {S: '10'}},
          items = [item, item2, item3]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        request(opts({TableName: helpers.testRangeTable, ConsistentRead: true, KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
        }, ScanIndexForward: false}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({Count: 3, ScannedCount: 3, Items: [item2, item3, item]})
          done()
        })
      })
    })

    it('should return items in reverse order with Limit for strings', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: '1'}},
          item2 = {a: item.a, b: {S: '2'}},
          item3 = {a: item.a, b: {S: '10'}},
          items = [item, item2, item3]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        request(opts({TableName: helpers.testRangeTable, ConsistentRead: true, KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
        }, ScanIndexForward: false, Limit: 2}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({Count: 2, ScannedCount: 2, Items: [item2, item3], LastEvaluatedKey: item3})
          done()
        })
      })
    })

    it('should return items in reverse order with ExclusiveStartKey for strings', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: '1'}},
          item2 = {a: item.a, b: {S: '2'}},
          item3 = {a: item.a, b: {S: '10'}},
          items = [item, item2, item3]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        request(opts({TableName: helpers.testRangeTable, ConsistentRead: true, KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
        }, ScanIndexForward: false, ExclusiveStartKey: item2}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({Count: 2, ScannedCount: 2, Items: [item3, item]})
          done()
        })
      })
    })

    it('should return items in reverse order for numbers', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {N: '0'}},
          item2 = {a: item.a, b: {N: '99.1'}},
          item3 = {a: item.a, b: {N: '10.9'}},
          item4 = {a: item.a, b: {N: '9.1'}},
          item5 = {a: item.a, b: {N: '0.9'}},
          item6 = {a: item.a, b: {N: '-0.01'}},
          item7 = {a: item.a, b: {N: '-0.1'}},
          item8 = {a: item.a, b: {N: '-1'}},
          item9 = {a: item.a, b: {N: '-99.1'}},
          items = [item, item2, item3, item4, item5, item6, item7, item8, item9]
      helpers.batchBulkPut(helpers.testRangeNTable, items, function(err) {
        if (err) return done(err)
        request(opts({TableName: helpers.testRangeNTable, ConsistentRead: true, KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
        }, ScanIndexForward: false}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({Count: 9, ScannedCount: 9, Items: [item2, item3, item4, item5, item, item6, item7, item8, item9]})
          done()
        })
      })
    })

    it('should return items in reverse order with Limit for numbers', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {N: '0'}},
          item2 = {a: item.a, b: {N: '99.1'}, c: {S: 'c'}},
          item3 = {a: item.a, b: {N: '10.9'}, c: {S: 'c'}},
          item4 = {a: item.a, b: {N: '9.1'}, c: {S: 'c'}},
          item5 = {a: item.a, b: {N: '0.9'}, c: {S: 'c'}},
          items = [item, item2, item3, item4, item5]
      helpers.batchBulkPut(helpers.testRangeNTable, items, function(err) {
        if (err) return done(err)
        request(opts({TableName: helpers.testRangeNTable, ConsistentRead: true, KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
        }, ScanIndexForward: false, Limit: 3}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({Count: 3, ScannedCount: 3, Items: [item2, item3, item4], LastEvaluatedKey: {a: item4.a, b: item4.b}})
          done()
        })
      })
    })

    it('should return items in reverse order for binary', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {B: '1Py5xA=='}},
          item2 = {a: item.a, b: {B: 'JA=='}},
          item3 = {a: item.a, b: {B: '2w=='}},
          item4 = {a: item.a, b: {B: 'cAeRhZE='}},
          item5 = {a: item.a, b: {B: '6piVtA=='}},
          item6 = {a: item.a, b: {B: 'MjA0'}},
          item7 = {a: item.a, b: {B: '1g=='}},
          item8 = {a: item.a, b: {B: 'ER/jLQ=='}},
          item9 = {a: item.a, b: {B: 'T7MzEUw='}},
          items = [item, item2, item3, item4, item5, item6, item7, item8, item9]
      helpers.batchBulkPut(helpers.testRangeBTable, items, function(err) {
        if (err) return done(err)
        request(opts({TableName: helpers.testRangeBTable, ConsistentRead: true, KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
        }, ScanIndexForward: false}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({Count: 9, ScannedCount: 9, Items: [item5, item3, item7, item, item4, item9,
            item6, item2, item8]})
          done()
        })
      })
    })

    it('should return items in reverse order with Limit for binary', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {B: '1Py5xA=='}},
          item2 = {a: item.a, b: {B: 'JA=='}},
          item3 = {a: item.a, b: {B: '2w=='}},
          item4 = {a: item.a, b: {B: 'cAeRhZE='}},
          item5 = {a: item.a, b: {B: '6piVtA=='}},
          items = [item, item2, item3, item4, item5]
      helpers.batchBulkPut(helpers.testRangeBTable, items, function(err) {
        if (err) return done(err)
        request(opts({TableName: helpers.testRangeBTable, ConsistentRead: true, KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
        }, ScanIndexForward: false, Limit: 3}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({Count: 3, ScannedCount: 3, Items: [item5, item3, item], LastEvaluatedKey: {a: item.a, b: item.b}})
          done()
        })
      })
    })

    it('should query on basic hash global index', function(done) {
      var item = {a: {S: 'a'}, b: {S: 'a'}, c: {S: helpers.randomString()}, d: {S: 'a'}},
          item2 = {a: {S: 'b'}, b: {S: 'b'}, c: item.c, d: {S: 'a'}},
          item3 = {a: {S: 'c'}, b: {S: 'e'}, c: item.c, d: {S: 'a'}},
          item4 = {a: {S: 'c'}, b: {S: 'd'}, c: item.c, d: {S: 'a'}},
          item5 = {a: {S: 'c'}, b: {S: 'c'}, c: {S: 'c'}, d: {S: 'a'}},
          item6 = {a: {S: 'd'}, b: {S: 'a'}, c: item.c, d: {S: 'a'}},
          item7 = {a: {S: 'e'}, b: {S: 'a'}, c: item.c, d: {S: 'a'}},
          items = [item, item2, item3, item4, item5, item6, item7]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        var req = {TableName: helpers.testRangeTable,
          KeyConditions: {c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]}},
          IndexName: 'index3', Limit: 4, ReturnConsumedCapacity: 'TOTAL'}
        request(opts(req), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Count: 4,
            ScannedCount: 4,
            Items: [item2, item, item3, item7],
            LastEvaluatedKey: {a: item7.a, b: item7.b, c: item7.c},
            ConsumedCapacity: {CapacityUnits: 0.5, TableName: helpers.testRangeTable},
          })
          req.ReturnConsumedCapacity = 'INDEXES'
          request(opts(req), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({
              Count: 4,
              ScannedCount: 4,
              Items: [item2, item, item3, item7],
              LastEvaluatedKey: {a: item7.a, b: item7.b, c: item7.c},
              ConsumedCapacity: {
                CapacityUnits: 0.5,
                TableName: helpers.testRangeTable,
                Table: {CapacityUnits: 0},
                GlobalSecondaryIndexes: {index3: {CapacityUnits: 0.5}},
              },
            })
            done()
          })
        })
      })
    })

    it('should query in reverse on basic hash global index', function(done) {
      var item = {a: {S: 'a'}, b: {S: 'a'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: 'b'}, b: {S: 'b'}, c: item.c},
          item3 = {a: {S: 'c'}, b: {S: 'e'}, c: item.c},
          item4 = {a: {S: 'c'}, b: {S: 'd'}, c: item.c},
          item5 = {a: {S: 'c'}, b: {S: 'c'}, c: {S: 'c'}},
          item6 = {a: {S: 'd'}, b: {S: 'a'}, c: item.c},
          item7 = {a: {S: 'e'}, b: {S: 'a'}, c: item.c},
          items = [item, item2, item3, item4, item5, item6, item7]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        var req = {TableName: helpers.testRangeTable,
          KeyConditions: {c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]}},
          IndexName: 'index3', ScanIndexForward: false, Limit: 4, ReturnConsumedCapacity: 'INDEXES'}
        request(opts(req), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Count: 4,
            ScannedCount: 4,
            Items: [item4, item6, item7, item3],
            LastEvaluatedKey: {a: item3.a, b: item3.b, c: item3.c},
            ConsumedCapacity: {
              CapacityUnits: 0.5,
              TableName: helpers.testRangeTable,
              Table: {CapacityUnits: 0},
              GlobalSecondaryIndexes: {index3: {CapacityUnits: 0.5}},
            },
          })
          done()
        })
      })
    })

    it('should query on range global index', function(done) {
      var item = {a: {S: 'a'}, b: {S: 'a'}, c: {S: helpers.randomString()}, d: {S: 'f'}, e: {S: 'a'}, f: {S: 'a'}},
          item2 = {a: {S: 'b'}, b: {S: 'b'}, c: item.c, d: {S: 'a'}, e: {S: 'a'}, f: {S: 'a'}},
          item3 = {a: {S: 'c'}, b: {S: 'e'}, c: item.c, d: {S: 'b'}, e: {S: 'a'}, f: {S: 'a'}},
          item4 = {a: {S: 'c'}, b: {S: 'd'}, c: item.c, d: {S: 'c'}, e: {S: 'a'}, f: {S: 'a'}},
          item5 = {a: {S: 'c'}, b: {S: 'c'}, c: {S: 'c'}, d: {S: 'd'}, e: {S: 'a'}, f: {S: 'a'}},
          item6 = {a: {S: 'd'}, b: {S: 'a'}, c: item.c, d: {S: 'e'}, e: {S: 'a'}, f: {S: 'a'}},
          item7 = {a: {S: 'e'}, b: {S: 'a'}, c: item.c, d: {S: 'f'}, e: {S: 'a'}, f: {S: 'a'}},
          items = [item, item2, item3, item4, item5, item6, item7]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        request(opts({TableName: helpers.testRangeTable, KeyConditions: {
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          d: {ComparisonOperator: 'LT', AttributeValueList: [item.d]},
        }, IndexName: 'index4', Limit: 3, ReturnConsumedCapacity: 'INDEXES'}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          delete item2.f
          delete item3.f
          delete item4.f
          res.body.should.eql({
            Count: 3,
            ScannedCount: 3,
            Items: [item2, item3, item4],
            LastEvaluatedKey: {a: item4.a, b: item4.b, c: item4.c, d: item4.d},
            ConsumedCapacity: {
              CapacityUnits: 0.5,
              TableName: helpers.testRangeTable,
              Table: {CapacityUnits: 0},
              GlobalSecondaryIndexes: {index4: {CapacityUnits: 0.5}},
            },
          })
          done()
        })
      })
    })

    it('should query in reverse on range global index', function(done) {
      var item = {a: {S: 'a'}, b: {S: 'a'}, c: {S: helpers.randomString()}, d: {S: 'f'}},
          item2 = {a: {S: 'b'}, b: {S: 'b'}, c: item.c, d: {S: 'a'}},
          item3 = {a: {S: 'c'}, b: {S: 'e'}, c: item.c, d: {S: 'b'}},
          item4 = {a: {S: 'c'}, b: {S: 'd'}, c: item.c, d: {S: 'c'}},
          item5 = {a: {S: 'c'}, b: {S: 'c'}, c: {S: 'c'}, d: {S: 'd'}},
          item6 = {a: {S: 'd'}, b: {S: 'a'}, c: item.c, d: {S: 'e'}},
          item7 = {a: {S: 'e'}, b: {S: 'a'}, c: item.c, d: {S: 'f'}},
          items = [item, item2, item3, item4, item5, item6, item7]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        request(opts({TableName: helpers.testRangeTable, KeyConditions: {
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          d: {ComparisonOperator: 'LT', AttributeValueList: [item.d]},
        }, IndexName: 'index4', ScanIndexForward: false, Limit: 3, ReturnConsumedCapacity: 'INDEXES'}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Count: 3,
            ScannedCount: 3,
            Items: [item6, item4, item3],
            LastEvaluatedKey: {a: item3.a, b: item3.b, c: item3.c, d: item3.d},
            ConsumedCapacity: {
              CapacityUnits: 0.5,
              TableName: helpers.testRangeTable,
              Table: {CapacityUnits: 0},
              GlobalSecondaryIndexes: {index4: {CapacityUnits: 0.5}},
            },
          })
          done()
        })
      })
    })

    it('should query with ExclusiveStartKey on basic hash global index', function(done) {
      var item = {a: {S: 'a'}, b: {S: 'a'}, c: {S: helpers.randomString()}, d: {S: 'a'}},
          item2 = {a: {S: 'b'}, b: {S: 'b'}, c: item.c, d: {S: 'a'}},
          item3 = {a: {S: 'c'}, b: {S: 'e'}, c: item.c, d: {S: 'a'}},
          item4 = {a: {S: 'c'}, b: {S: 'd'}, c: item.c, d: {S: 'a'}},
          item5 = {a: {S: 'c'}, b: {S: 'c'}, c: {S: 'c'}, d: {S: 'a'}},
          item6 = {a: {S: 'd'}, b: {S: 'a'}, c: item.c, d: {S: 'a'}},
          item7 = {a: {S: 'e'}, b: {S: 'a'}, c: item.c, d: {S: 'a'}},
          items = [item, item2, item3, item4, item5, item6, item7]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        delete item3.d
        request(opts({TableName: helpers.testRangeTable, KeyConditions: {
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }, IndexName: 'index3', Limit: 2, ExclusiveStartKey: item3, ReturnConsumedCapacity: 'INDEXES'}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Count: 2,
            ScannedCount: 2,
            Items: [item7, item6],
            LastEvaluatedKey: {a: item6.a, b: item6.b, c: item6.c},
            ConsumedCapacity: {
              CapacityUnits: 0.5,
              TableName: helpers.testRangeTable,
              Table: {CapacityUnits: 0},
              GlobalSecondaryIndexes: {index3: {CapacityUnits: 0.5}},
            },
          })
          done()
        })
      })
    })

    it('should query in reverse with ExclusiveStartKey on basic hash global index', function(done) {
      var item = {a: {S: 'a'}, b: {S: 'a'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: 'b'}, b: {S: 'b'}, c: item.c},
          item3 = {a: {S: 'c'}, b: {S: 'e'}, c: item.c},
          item4 = {a: {S: 'c'}, b: {S: 'd'}, c: item.c},
          item5 = {a: {S: 'c'}, b: {S: 'c'}, c: {S: 'c'}},
          item6 = {a: {S: 'd'}, b: {S: 'a'}, c: item.c},
          item7 = {a: {S: 'e'}, b: {S: 'a'}, c: item.c},
          items = [item, item2, item3, item4, item5, item6, item7]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        delete item7.d
        request(opts({TableName: helpers.testRangeTable, KeyConditions: {
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }, IndexName: 'index3', ScanIndexForward: false, Limit: 2, ExclusiveStartKey: item7, ReturnConsumedCapacity: 'INDEXES'}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Count: 2,
            ScannedCount: 2,
            Items: [item3, item],
            LastEvaluatedKey: {a: item.a, b: item.b, c: item.c},
            ConsumedCapacity: {
              CapacityUnits: 0.5,
              TableName: helpers.testRangeTable,
              Table: {CapacityUnits: 0},
              GlobalSecondaryIndexes: {index3: {CapacityUnits: 0.5}},
            },
          })
          done()
        })
      })
    })

    it('should query with ExclusiveStartKey on range global index', function(done) {
      var item = {a: {S: 'a'}, b: {S: 'a'}, c: {S: helpers.randomString()}, d: {S: 'f'}, e: {S: 'a'}, f: {S: 'a'}},
          item2 = {a: {S: 'b'}, b: {S: 'b'}, c: item.c, d: {S: 'a'}, e: {S: 'a'}, f: {S: 'a'}},
          item3 = {a: {S: 'c'}, b: {S: 'e'}, c: item.c, d: {S: 'b'}, e: {S: 'a'}, f: {S: 'a'}},
          item4 = {a: {S: 'c'}, b: {S: 'd'}, c: item.c, d: {S: 'c'}, e: {S: 'a'}, f: {S: 'a'}},
          item5 = {a: {S: 'c'}, b: {S: 'c'}, c: {S: 'c'}, d: {S: 'd'}, e: {S: 'a'}, f: {S: 'a'}},
          item6 = {a: {S: 'd'}, b: {S: 'a'}, c: item.c, d: {S: 'e'}, e: {S: 'a'}, f: {S: 'a'}},
          item7 = {a: {S: 'e'}, b: {S: 'a'}, c: item.c, d: {S: 'f'}, e: {S: 'a'}, f: {S: 'a'}},
          items = [item, item2, item3, item4, item5, item6, item7]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        delete item3.e
        delete item3.f
        delete item4.f
        request(opts({TableName: helpers.testRangeTable, KeyConditions: {
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          d: {ComparisonOperator: 'LT', AttributeValueList: [item.d]},
        }, IndexName: 'index4', Limit: 1, ExclusiveStartKey: item3, ReturnConsumedCapacity: 'INDEXES'}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Count: 1,
            ScannedCount: 1,
            Items: [item4],
            LastEvaluatedKey: {a: item4.a, b: item4.b, c: item4.c, d: item4.d},
            ConsumedCapacity: {
              CapacityUnits: 0.5,
              TableName: helpers.testRangeTable,
              Table: {CapacityUnits: 0},
              GlobalSecondaryIndexes: {index4: {CapacityUnits: 0.5}},
            },
          })
          done()
        })
      })
    })

    it('should query in reverse with ExclusiveStartKey on range global index', function(done) {
      var item = {a: {S: 'a'}, b: {S: 'a'}, c: {S: helpers.randomString()}, d: {S: 'f'}, e: {S: 'a'}, f: {S: 'a'}},
          item2 = {a: {S: 'b'}, b: {S: 'b'}, c: item.c, d: {S: 'a'}, e: {S: 'a'}, f: {S: 'a'}},
          item3 = {a: {S: 'c'}, b: {S: 'e'}, c: item.c, d: {S: 'b'}, e: {S: 'a'}, f: {S: 'a'}},
          item4 = {a: {S: 'c'}, b: {S: 'd'}, c: item.c, d: {S: 'c'}, e: {S: 'a'}, f: {S: 'a'}},
          item5 = {a: {S: 'c'}, b: {S: 'c'}, c: {S: 'c'}, d: {S: 'd'}, e: {S: 'a'}, f: {S: 'a'}},
          item6 = {a: {S: 'd'}, b: {S: 'a'}, c: item.c, d: {S: 'e'}, e: {S: 'a'}, f: {S: 'a'}},
          item7 = {a: {S: 'e'}, b: {S: 'a'}, c: item.c, d: {S: 'f'}, e: {S: 'a'}, f: {S: 'a'}},
          items = [item, item2, item3, item4, item5, item6, item7]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        delete item4.e
        delete item4.f
        delete item3.f
        request(opts({TableName: helpers.testRangeTable, KeyConditions: {
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          d: {ComparisonOperator: 'LT', AttributeValueList: [item.d]},
        }, IndexName: 'index4', Limit: 1, ScanIndexForward: false, ExclusiveStartKey: item4, ReturnConsumedCapacity: 'INDEXES'}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Count: 1,
            ScannedCount: 1,
            Items: [item3],
            LastEvaluatedKey: {a: item3.a, b: item3.b, c: item3.c, d: item3.d},
            ConsumedCapacity: {
              CapacityUnits: 0.5,
              TableName: helpers.testRangeTable,
              Table: {CapacityUnits: 0},
              GlobalSecondaryIndexes: {index4: {CapacityUnits: 0.5}},
            },
          })
          done()
        })
      })
    })

    it('should query on a global index if values are equal', function(done) {
      var item = {a: {S: 'a'}, b: {S: 'a'}, c: {S: helpers.randomString()}, d: {S: 'a'}},
          item2 = {a: {S: 'b'}, b: {S: 'a'}, c: item.c, d: {S: 'a'}},
          item3 = {a: {S: 'c'}, b: {S: 'a'}, c: item.c, d: {S: 'a'}},
          item4 = {a: {S: 'c'}, b: {S: 'b'}, c: item.c, d: {S: 'a'}},
          item5 = {a: {S: 'd'}, b: {S: 'a'}, c: item.c, d: {S: 'a'}},
          item6 = {a: {S: 'd'}, b: {S: 'b'}, c: item.c, d: {S: 'a'}},
          items = [item, item2, item3, item4, item5, item6]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        request(opts({TableName: helpers.testRangeTable, KeyConditions: {
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }, IndexName: 'index4', ExclusiveStartKey: item, ReturnConsumedCapacity: 'INDEXES'}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Count: 5,
            ScannedCount: 5,
            Items: [item5, item2, item3, item6, item4],
            ConsumedCapacity: {
              CapacityUnits: 0.5,
              TableName: helpers.testRangeTable,
              Table: {CapacityUnits: 0},
              GlobalSecondaryIndexes: {index4: {CapacityUnits: 0.5}},
            },
          })
          done()
        })
      })
    })

    // TODO: Need high capacity to run this (~100 runs quickly)
    it.skip('should not return LastEvaluatedKey if just under limit', function(done) {
      this.timeout(200000)

      var i, items = [], id = helpers.randomString(), e = new Array(41646).join('e'), eAttr = e.slice(0, 255)
      for (i = 0; i < 25; i++) {
        var item = {a: {S: id}, b: {S: ('0' + i).slice(-2)}}
        item[eAttr] = {S: e}
        items.push(item)
      }

      helpers.replaceTable(helpers.testRangeTable, ['a', 'b'], items, function(err) {
        if (err) return done(err)

        request(opts({
          TableName: helpers.testRangeTable,
          KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: id}]}},
          Select: 'COUNT',
          ReturnConsumedCapacity: 'INDEXES',
          Limit: 26, // Limit of 25 includes LastEvaluatedKey, leaving this out does not
        }), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Count: 25,
            ScannedCount: 25,
            ConsumedCapacity: {
              CapacityUnits: 128,
              Table: {CapacityUnits: 128},
              TableName: helpers.testRangeTable,
            },
          })
          helpers.clearTable(helpers.testRangeTable, ['a', 'b'], done)
        })
      })
    })

    // TODO: Need high capacity to run this (~100 runs quickly)
    it.skip('should return LastEvaluatedKey if just over limit', function(done) {
      this.timeout(200000)

      var i, items = [], id = helpers.randomString(), e = new Array(41646).join('e')
      for (i = 0; i < 25; i++)
        items.push({a: {S: id}, b: {S: ('0' + i).slice(-2)}, e: {S: e}})
      items[24].e.S = new Array(41647).join('e')

      helpers.replaceTable(helpers.testRangeTable, ['a', 'b'], items, function(err) {
        if (err) return done(err)

        request(opts({
          TableName: helpers.testRangeTable,
          KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: id}]}},
          Select: 'COUNT',
          ReturnConsumedCapacity: 'INDEXES',
        }), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Count: 25,
            ScannedCount: 25,
            ConsumedCapacity: {
              CapacityUnits: 127.5,
              Table: {CapacityUnits: 127.5},
              TableName: helpers.testRangeTable,
            },
            LastEvaluatedKey: {a: items[24].a, b: items[24].b},
          })
          helpers.clearTable(helpers.testRangeTable, ['a', 'b'], done)
        })
      })
    })

    // TODO: Need high capacity to run this (~100 runs quickly)
    it.skip('should return all if just under limit', function(done) {
      this.timeout(200000)

      var i, items = [], id = helpers.randomString(), e = new Array(43373).join('e'), eAttr = e.slice(0, 255)
      for (i = 0; i < 25; i++) {
        var item = {a: {S: id}, b: {S: ('0' + i).slice(-2)}}
        item[eAttr] = {S: e}
        items.push(item)
      }
      items[23][eAttr].S = new Array(43388).join('e')
      items[24][eAttr].S = new Array(45000).join('e')

      helpers.replaceTable(helpers.testRangeTable, ['a', 'b'], items, function(err) {
        if (err) return done(err)

        request(opts({
          TableName: helpers.testRangeTable,
          KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: id}]}},
          Select: 'COUNT',
          ReturnConsumedCapacity: 'TOTAL',
        }), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Count: 25,
            ScannedCount: 25,
            ConsumedCapacity: {CapacityUnits: 133.5, TableName: helpers.testRangeTable},
            LastEvaluatedKey: {a: items[24].a, b: items[24].b},
          })
          helpers.clearTable(helpers.testRangeTable, ['a', 'b'], done)
        })
      })
    })

    // TODO: Need high capacity to run this (~100 runs quickly)
    it.skip('should return one less than all if just over limit', function(done) {
      this.timeout(200000)

      var i, items = [], id = helpers.randomString(), e = new Array(43373).join('e')
      for (i = 0; i < 25; i++)
        items.push({a: {S: id}, b: {S: ('0' + i).slice(-2)}, e: {S: e}})
      items[23].e.S = new Array(43389).join('e')
      items[24].e.S = new Array(45000).join('e')

      helpers.replaceTable(helpers.testRangeTable, ['a', 'b'], items, function(err) {
        if (err) return done(err)

        request(opts({
          TableName: helpers.testRangeTable,
          KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: id}]}},
          Select: 'COUNT',
          ReturnConsumedCapacity: 'TOTAL',
        }), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Count: 24,
            ScannedCount: 24,
            ConsumedCapacity: {CapacityUnits: 127.5, TableName: helpers.testRangeTable},
            LastEvaluatedKey: {a: items[23].a, b: items[23].b},
          })
          helpers.clearTable(helpers.testRangeTable, ['a', 'b'], done)
        })
      })
    })

  })

})
