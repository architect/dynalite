var helpers = require('./helpers'),
    should = require('should'),
    async = require('async')

var target = 'Scan',
    request = helpers.request,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target)

describe('scan', function() {

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

    it('should return SerializationException when ReturnConsumedCapacity is not a string', function(done) {
      assertType('ReturnConsumedCapacity', 'String', done)
    })

    it('should return SerializationException when Select is not a string', function(done) {
      assertType('Select', 'String', done)
    })

    it('should return SerializationException when Limit is not an integer', function(done) {
      assertType('Limit', 'Integer', done)
    })

    it('should return SerializationException when Segment is not an integer', function(done) {
      assertType('Segment', 'Integer', done)
    })

    it('should return SerializationException when ConditionalOperator is not a string', function(done) {
      assertType('ConditionalOperator', 'String', done)
    })

    it('should return SerializationException when TotalSegments is not an integer', function(done) {
      assertType('TotalSegments', 'Integer', done)
    })

    it('should return SerializationException when ScanFilter is not a map', function(done) {
      assertType('ScanFilter', 'Map<Condition>', done)
    })

    it('should return SerializationException when ScanFilter.Attr is not a struct', function(done) {
      assertType('ScanFilter.Attr', 'ValueStruct<Condition>', done)
    })

    it('should return SerializationException when ScanFilter.Attr.ComparisonOperator is not a string', function(done) {
      assertType('ScanFilter.Attr.ComparisonOperator', 'String', done)
    })

    it('should return SerializationException when ScanFilter.Attr.AttributeValueList is not a list', function(done) {
      assertType('ScanFilter.Attr.AttributeValueList', 'List', done)
    })

    it('should return SerializationException when ScanFilter.Attr.AttributeValueList.0 is not an attr struct', function(done) {
      this.timeout(60000)
      assertType('ScanFilter.Attr.AttributeValueList.0', 'AttrStruct<ValueStruct>', done)
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

    it('should return SerializationException when IndexName is not a string', function(done) {
      assertType('IndexName', 'String', done)
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

    it('should return ValidationException for incorrect attributes', function(done) {
      assertValidation({TableName: 'abc;', ReturnConsumedCapacity: 'hi', AttributesToGet: [],
        IndexName: 'abc;', Segment: -1, TotalSegments: -1, Select: 'hi', Limit: -1, ScanFilter: {a: {}, b: {ComparisonOperator: ''}},
        ConditionalOperator: 'AN', ExpressionAttributeNames: {}, ExpressionAttributeValues: {}, ProjectionExpression: ''}, [
          'Value \'hi\' at \'select\' failed to satisfy constraint: ' +
          'Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]',
          'Value \'abc;\' at \'indexName\' failed to satisfy constraint: ' +
          'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
          'Value \'-1\' at \'totalSegments\' failed to satisfy constraint: ' +
          'Member must have value greater than or equal to 1',
          'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
          'Member must satisfy enum value set: [INDEXES, TOTAL, NONE]',
          'Value \'abc;\' at \'tableName\' failed to satisfy constraint: ' +
          'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
          'Value \'AN\' at \'conditionalOperator\' failed to satisfy constraint: ' +
          'Member must satisfy enum value set: [OR, AND]',
          'Value null at \'scanFilter.a.member.comparisonOperator\' failed to satisfy constraint: ' +
          'Member must not be null',
          'Value \'\' at \'scanFilter.b.member.comparisonOperator\' failed to satisfy constraint: ' +
          'Member must satisfy enum value set: [IN, NULL, BETWEEN, LT, NOT_CONTAINS, EQ, GT, NOT_NULL, NE, LE, BEGINS_WITH, GE, CONTAINS]',
          'Value \'-1\' at \'segment\' failed to satisfy constraint: ' +
          'Member must have value greater than or equal to 0',
          'Value \'-1\' at \'limit\' failed to satisfy constraint: ' +
          'Member must have value greater than or equal to 1',
        ], done)
    })

    it('should return ValidationException if expression and non-expression', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{}]}},
        Segment: 1,
        Limit: 1,
        AttributesToGet: ['a', 'a'],
        ExclusiveStartKey: {a: {}},
        ConditionalOperator: 'OR',
        Select: 'SPECIFIC_ATTRIBUTES',
        FilterExpression: '',
        ProjectionExpression: '',
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
      }, 'Can not use both expression and non-expression parameters in the same request: ' +
        'Non-expression parameters: {AttributesToGet, ScanFilter, ConditionalOperator} ' +
        'Expression parameters: {ProjectionExpression, FilterExpression}', done)
    })

    it('should return ValidationException if ExpressionAttributeNames but no FilterExpression', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{}]}},
        Segment: 1,
        Limit: 1,
        AttributesToGet: ['a', 'a'],
        ExclusiveStartKey: {a: {}},
        ConditionalOperator: 'OR',
        Select: 'SPECIFIC_ATTRIBUTES',
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
      }, 'ExpressionAttributeNames can only be specified when using expressions', done)
    })

    it('should return ValidationException if ExpressionAttributeValues but no FilterExpression', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{}]}},
        Segment: 1,
        Limit: 1,
        AttributesToGet: ['a', 'a'],
        ExclusiveStartKey: {a: {}},
        ConditionalOperator: 'OR',
        Select: 'SPECIFIC_ATTRIBUTES',
        ExpressionAttributeValues: {},
      }, 'ExpressionAttributeValues can only be specified when using expressions: FilterExpression is null', done)
    })

    it('should return ValidationException for duplicate values in AttributesToGet', function(done) {
      assertValidation({
        TableName: 'abc',
        Segment: 1,
        ScanFilter: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{}, {a: ''}, {S: ''}]}},
        ExclusiveStartKey: {a: {}},
        AttributesToGet: ['a', 'a'],
      }, 'One or more parameter values were invalid: Duplicate value in attribute name: a', done)
    })

    it('should return ValidationException for bad attribute values in ScanFilter', function(done) {
      async.forEach([
        {},
        {a: ''},
      ], function(expr, cb) {
        assertValidation({
          TableName: 'abc',
          Segment: 1,
          ExclusiveStartKey: {a: {}},
          ScanFilter: {a: {ComparisonOperator: 'EQ', AttributeValueList: [expr, {S: ''}]}},
        }, 'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', cb)
      }, done)
    })

    it('should return ValidationException for invalid values in ScanFilter', function(done) {
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
          Segment: 1,
          ExclusiveStartKey: {a: {}},
          ScanFilter: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{N: '1'}, expr[0], {}]}},
        }, 'One or more parameter values were invalid: ' + expr[1], cb)
      }, done)
    })

    it('should return ValidationException for empty/invalid numbers in ScanFilter', function(done) {
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
          Segment: 1,
          ExclusiveStartKey: {a: {}},
          ScanFilter: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{N: '1'}, expr[0]]}},
        }, expr[1], cb)
      }, done)
    })

    it('should return ValidationException for multiple datatypes in ScanFilter', function(done) {
      assertValidation({
        TableName: 'abc',
        Segment: 1,
        ExclusiveStartKey: {a: {}},
        ScanFilter: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{N: '1'}, {S: 'a', N: '1'}]}},
      }, 'Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException for incorrect number of ScanFilter arguments', function(done) {
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
          Segment: 1,
          ExclusiveStartKey: {a: {}},
          ScanFilter: expr,
        }, 'One or more parameter values were invalid: Invalid number of argument(s) for the ' +
          expr.a.ComparisonOperator + ' ComparisonOperator', cb)
      }, done)
    })

    it('should return ValidationException for invalid ComparisonOperator types', function(done) {
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
          assertValidation({
            TableName: 'abc',
            Segment: 1,
            ExclusiveStartKey: {a: {}},
            ScanFilter: {a: {ComparisonOperator: cond, AttributeValueList: list}},
          }, 'One or more parameter values were invalid: ' +
            'ComparisonOperator ' + cond + ' is not valid for ' +
            Object.keys(list[0])[0] + ' AttributeValue type', cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException for invalid CONTAINS ComparisonOperator types', function(done) {
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
          assertValidation({
            TableName: 'abc',
            Segment: 1,
            ExclusiveStartKey: {a: {}},
            ScanFilter: {a: {ComparisonOperator: cond, AttributeValueList: list}},
          }, 'One or more parameter values were invalid: ' +
            'ComparisonOperator ' + cond + ' is not valid for ' +
            Object.keys(list[0])[0] + ' AttributeValue type', cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException for invalid BETWEEN ComparisonOperator types', function(done) {
      async.forEach([
        [{BOOL: true}, {BOOL: true}],
        [{NULL: true}, {NULL: true}],
        [{SS: ['a']}, {SS: ['a']}],
        [{NS: ['1']}, {NS: ['1']}],
        [{BS: ['abcd']}, {BS: ['abcd']}],
        [{M: {}}, {M: {}}],
        [{L: []}, {L: []}],
      ], function(list, cb) {
        assertValidation({
          TableName: 'abc',
          Segment: 1,
          ExclusiveStartKey: {a: {}},
          ScanFilter: {a: {ComparisonOperator: 'BETWEEN', AttributeValueList: list}},
        }, 'One or more parameter values were invalid: ' +
          'ComparisonOperator BETWEEN is not valid for ' +
          Object.keys(list[0])[0] + ' AttributeValue type', cb)
      }, done)
    })

    it('should return ValidationException for invalid BEGINS_WITH ComparisonOperator types', function(done) {
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
        assertValidation({
          TableName: 'abc',
          Segment: 1,
          ExclusiveStartKey: {a: {}},
          ScanFilter: {a: {ComparisonOperator: 'BEGINS_WITH', AttributeValueList: list}},
        }, 'One or more parameter values were invalid: ' +
          'ComparisonOperator BEGINS_WITH is not valid for ' +
          Object.keys(list[0])[0] + ' AttributeValue type', cb)
      }, done)
    })

    it('should return ValidationException on ExclusiveStartKey if ScanFilter ok with EQ on type SS when table does not exist', function(done) {
      assertValidation({
        TableName: 'abc',
        Segment: 1,
        ExclusiveStartKey: {a: {}},
        ScanFilter: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{SS: ['a']}]}},
      }, 'The provided starting key is invalid: Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException for unsupported datatype in ExclusiveStartKey', function(done) {
      async.forEach([
        {},
        {a: ''},
        {M: {a: {}}},
        {L: [{}]},
        {L: [{a: {}}]},
      ], function(expr, cb) {
        assertValidation({
          TableName: 'abc',
          Segment: 1,
          FilterExpression: '',
          ExpressionAttributeNames: {},
          ExpressionAttributeValues: {},
          ExclusiveStartKey: {a: expr},
        }, 'The provided starting key is invalid: ' +
          'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', cb)
      }, done)
    })

    it('should return ValidationException for invalid values in ExclusiveStartKey', function(done) {
      async.forEach([
        [{NULL: 'no'}, 'Null attribute value types must have the value of true'],
        [{SS: []}, 'An string set  may not be empty'],
        [{BS: []}, 'Binary sets should not be empty'],
      ], function(expr, cb) {
        assertValidation({
          TableName: 'abc',
          Segment: 1,
          FilterExpression: '',
          ExpressionAttributeNames: {},
          ExpressionAttributeValues: {},
          ExclusiveStartKey: {a: expr[0]},
        }, 'The provided starting key is invalid: ' +
          'One or more parameter values were invalid: ' + expr[1], cb)
      }, done)
    })

    it('should return ValidationException for invalid values in ExclusiveStartKey with no provided message', function(done) {
      async.forEach([
        [{NS: []}, 'An number set  may not be empty'],
        [{SS: ['a', 'a']}, 'Input collection [a, a] contains duplicates.'],
        [{BS: ['Yg==', 'Yg==']}, 'Input collection [Yg==, Yg==]of type BS contains duplicates.'],
      ], function(expr, cb) {
        assertValidation({
          TableName: 'abc',
          Segment: 1,
          FilterExpression: '',
          ExpressionAttributeNames: {},
          ExpressionAttributeValues: {},
          ExclusiveStartKey: {a: expr[0]},
        }, 'One or more parameter values were invalid: ' + expr[1], cb)
      }, done)
    })

    it('should return ValidationException for empty/invalid numbers in ExclusiveStartKey', function(done) {
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
          Segment: 1,
          FilterExpression: '',
          ExpressionAttributeNames: {},
          ExpressionAttributeValues: {},
          ExclusiveStartKey: {a: expr[0]},
        }, expr[1], cb)
      }, done)
    })

    it('should return ValidationException for multiple datatypes in ExclusiveStartKey', function(done) {
      assertValidation({
        TableName: 'abc',
        TotalSegments: 1,
        FilterExpression: '',
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
        ExclusiveStartKey: {a: {S: 'a', N: '1'}},
      }, 'The provided starting key is invalid: ' +
        'Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException for missing TotalSegments', function(done) {
      assertValidation({
        TableName: 'abc',
        Segment: 1,
        FilterExpression: '',
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
      }, 'The TotalSegments parameter is required but was not present in the request when Segment parameter is present', done)
    })

    it('should return ValidationException for missing Segment', function(done) {
      assertValidation({
        TableName: 'abc',
        TotalSegments: 1,
        FilterExpression: '',
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
      }, 'The Segment parameter is required but was not present in the request when parameter TotalSegments is present', done)
    })

    it('should return ValidationException for Segment more than TotalSegments', function(done) {
      assertValidation({
        TableName: 'abc',
        Segment: 1,
        TotalSegments: 1,
        FilterExpression: '',
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
      }, 'The Segment parameter is zero-based and must be less than parameter TotalSegments: Segment: 1 is not less than TotalSegments: 1', done)
    })

    it('should return ValidationException for empty ExpressionAttributeNames', function(done) {
      assertValidation({
        TableName: 'abc',
        FilterExpression: '',
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
      }, 'ExpressionAttributeNames must not be empty', done)
    })

    it('should return ValidationException for invalid ExpressionAttributeNames', function(done) {
      assertValidation({
        TableName: 'abc',
        FilterExpression: '',
        ExpressionAttributeNames: {'a': 'a'},
        ExpressionAttributeValues: {},
      }, 'ExpressionAttributeNames contains invalid key: Syntax error; key: "a"', done)
    })

    it('should return ValidationException for empty ExpressionAttributeValues', function(done) {
      assertValidation({
        TableName: 'abc',
        FilterExpression: '',
        ExpressionAttributeValues: {},
      }, 'ExpressionAttributeValues must not be empty', done)
    })

    it('should return ValidationException for invalid ExpressionAttributeValues', function(done) {
      assertValidation({
        TableName: 'abc',
        FilterExpression: '',
        ExpressionAttributeValues: {'a': {S: 'b'}},
      }, 'ExpressionAttributeValues contains invalid key: Syntax error; key: "a"', done)
    })

    it('should return ValidationException for empty FilterExpression', function(done) {
      assertValidation({
        TableName: 'abc',
        FilterExpression: '',
        ProjectionExpression: '',
        ExpressionAttributeValues: {':0': {S: 'b'}},
      }, 'Invalid FilterExpression: The expression can not be empty;', done)
    })

    it('should return ValidationException for empty ProjectionExpression', function(done) {
      assertValidation({
        TableName: 'abc',
        FilterExpression: 'a > b',
        ProjectionExpression: '',
      }, 'Invalid ProjectionExpression: The expression can not be empty;', done)
    })

    it('should return ValidationException for syntax errors', function(done) {
      var expressions = [
        'things are not gonna be ok',
        'a > 4',
        'attribute_exists(Pictures-RearView)',
        'attribute_exists("Pictures.RearView")',
        'attribute_exists(Pictures..RearView)',
        'attribute_exists(Pi#ctures.RearView)',
        'attribute_exists(asdf[a])',
        // 'a.:a < b.:b', // 500 error? com.amazon.coral.service#InternalFailure
        'a in b, c',
        'a > between',
        'a in b, c',
        'a in b',
        'a in (b,c,)',
        '(a)between(b.(c.d))and(c)',
        '(a)between(b.c.d)and((c)',
        '#$.b > a',
        'a > :things.stuff',
        'b[:a] > a',
        'b[#_] > a',
        'ü > a',
        '(a)between(b.c[4.5].#d)and(:a)',
        'size(a).b > a',
        '(size(a)).b > a',
        'size(a)[0] > a',
        '(size(a))[0] > a',
      ]
      async.forEach(expressions, function(expression, cb) {
        assertValidation({
          TableName: 'abc',
          FilterExpression: expression,
        }, /^Invalid FilterExpression: Syntax error; /, cb)
      }, done)
    })

    it('should return ValidationException for redundant parentheses', function(done) {
      var expressions = [
        'a=a and a > ((views))',
        '(a)between(((b.c)).d)and(c)',
        'a > whatever((:things), ((a)))',
        'a=a AND ((a=a AND a=a)) AND a=a',
        'a=a OR ((a=a OR a=a)) OR a=a',
        'a=a AND ((a=a AND (a=a AND a=a)))',
        'a=a OR ((a=a OR (a=a OR a=a)))',
      ]
      async.forEach(expressions, function(expression, cb) {
        assertValidation({
          TableName: 'abc',
          FilterExpression: expression,
        }, 'Invalid FilterExpression: The expression has redundant parentheses;', cb)
      }, done)
    })

    it('should return ValidationException for invalid function names', function(done) {
      var expressions = [
        ['a=a and whatever((:things)) > a', 'whatever'],
        ['attRIbute_exIsts((views), #a)', 'attRIbute_exIsts'],
      ]
      async.forEach(expressions, function(expr, cb) {
        assertValidation({
          TableName: 'abc',
          FilterExpression: expr[0],
        }, 'Invalid FilterExpression: Invalid function name; function: ' + expr[1], cb)
      }, done)
    })

    it('should return ValidationException for functions used incorrectly', function(done) {
      var expressions = [
        ['a=a and attribute_exists((views), (#a)) > b', 'attribute_exists'],
        ['attribute_not_exists(things) > b', 'attribute_not_exists'],
        ['attribute_type(things, :a) > b', 'attribute_type'],
        ['begins_with(things, a) > b', 'begins_with'],
        ['contains(:things, c) > b', 'contains'],
        ['size(contains(a, b)) > a', 'contains'],
        ['size(things)', 'size'],
        ['a between b and attribute_exists(things)', 'attribute_exists'],
        ['a in (b, attribute_exists(things))', 'attribute_exists'],
      ]
      async.forEach(expressions, function(expr, cb) {
        assertValidation({
          TableName: 'abc',
          FilterExpression: expr[0],
        }, 'Invalid FilterExpression: The function is not allowed to be used this way in an expression; function: ' + expr[1], cb)
      }, done)
    })

    it('should return ValidationException for reserved keywords', function(done) {
      var expressions = [
        ['attribute_exists(views, #a)', 'views'],
        [':a < abOrT', 'abOrT'],
      ]
      async.forEach(expressions, function(expr, cb) {
        assertValidation({
          TableName: 'abc',
          FilterExpression: expr[0],
        }, 'Invalid FilterExpression: Attribute name is a reserved keyword; reserved keyword: ' + expr[1], cb)
      }, done)
    })

    // All checks below here are done on a per-expression basis

    it('should return ValidationException for missing attribute names', function(done) {
      var expressions = [
        ['attribute_exists(#Pictures.RearView, :a) and a=a', '#Pictures'],
        ['begins_with(Pictures.#RearView)', '#RearView'],
        ['(#P between :lo and :hi) and (#PC in (:cat1, :cat2))', '#P'],
        ['#4g > a', '#4g'],
        ['#_ > a', '#_'],
        ['(a)between(b.c[45].#d)and(:a)', '#d'],
      ]
      async.forEach(expressions, function(expr, cb) {
        assertValidation({
          TableName: 'abc',
          FilterExpression: expr[0],
        }, 'Invalid FilterExpression: An expression attribute name used in the document path is not defined; attribute name: ' + expr[1], cb)
      }, done)
    })

    it('should return ValidationException for missing attribute values', function(done) {
      var expressions = [
        ['begins_with(:hello, #a, #b)', ':hello'],
        [':a < :b', ':a'],
        [':_ > a', ':_'],
      ]
      async.forEach(expressions, function(expr, cb) {
        assertValidation({
          TableName: 'abc',
          FilterExpression: expr[0],
        }, 'Invalid FilterExpression: An expression attribute value used in expression is not defined; attribute value: ' + expr[1], cb)
      }, done)
    })

    it('should return ValidationException for functions with incorrect operands', function(done) {
      var expressions = [
        ['attribute_exists(things, a) and a=a', 'attribute_exists', 2],
        ['attribute_not_exists(things, b)', 'attribute_not_exists', 2],
        ['attribute_type(things)', 'attribute_type', 1],
        ['begins_with(things)', 'begins_with', 1],
        ['begins_with(things, size(a), b)', 'begins_with', 3],
        ['contains(things)', 'contains', 1],
        ['size(things, a) > b', 'size', 2],
      ]
      async.forEach(expressions, function(expr, cb) {
        assertValidation({
          TableName: 'abc',
          FilterExpression: expr[0],
        }, 'Invalid FilterExpression: Incorrect number of operands for operator or function; operator or function: ' +
          expr[1] +', number of operands: ' + expr[2], cb)
      }, done)
    })

    it('should return ValidationException for functions with incorrect operand type', function(done) {
      var expressions = [
        // Order of the {...} args is non-deterministic
        // ['attribute_type(ab.bc[1].a, SS)', 'attribute_type', '{NS,SS,L,BS,N,M,B,BOOL,NULL,S}'],
        ['attribute_type(a, size(a)) and a=a and a=:a', 'attribute_type', {'N': '1'}],
        ['attribute_type(a, :a)', 'attribute_type', {'N': '1'}],
        ['attribute_type(a, :a)', 'attribute_type', {'B': 'YQ=='}],
        ['attribute_type(a, :a)', 'attribute_type', {'BOOL': true}],
        ['attribute_type(a, :a)', 'attribute_type', {'NULL': true}],
        ['attribute_type(a, :a)', 'attribute_type', {'L': []}],
        ['attribute_type(a, :a)', 'attribute_type', {'M': {}}],
        ['attribute_type(a, :a)', 'attribute_type', {'SS': ['1']}],
        ['attribute_type(a, :a)', 'attribute_type', {'NS': ['1']}],
        ['attribute_type(a, :a)', 'attribute_type', {'BS': ['YQ==']}],
        ['begins_with(a, size(a)) and a=:a', 'begins_with', {'N': '1'}],
        ['begins_with(a, :a)', 'begins_with', {'N': '1'}],
        ['begins_with(a, :a)', 'begins_with', {'BOOL': true}],
        ['begins_with(a, :a)', 'begins_with', {'NULL': true}],
        ['begins_with(a, :a)', 'begins_with', {'L': []}],
        ['begins_with(a, :a)', 'begins_with', {'M': {}}],
        ['begins_with(a, :a)', 'begins_with', {'SS': ['1']}],
        ['begins_with(a, :a)', 'begins_with', {'NS': ['1']}],
        ['begins_with(a, :a)', 'begins_with', {'BS': ['YQ==']}],
        ['begins_with(:a, a)', 'begins_with', {'N': '1'}],
        ['begins_with(:a, a)', 'begins_with', {'BOOL': true}],
        ['begins_with(:a, a)', 'begins_with', {'NULL': true}],
        ['begins_with(:a, a)', 'begins_with', {'L': []}],
        ['begins_with(:a, a)', 'begins_with', {'M': {}}],
        ['begins_with(:a, a)', 'begins_with', {'SS': ['1']}],
        ['begins_with(:a, a)', 'begins_with', {'NS': ['1']}],
        ['begins_with(:a, a)', 'begins_with', {'BS': ['YQ==']}],
        ['size(size(a)) > :a', 'size', {'N': '1'}],
        ['attribute_not_exists(size(:a))', 'size', {'N': '1'}],
        ['size(:a) > a', 'size', {'BOOL': true}],
        ['size(:a) > a', 'size', {'NULL': true}],
      ]
      async.forEach(expressions, function(expr, cb) {
        assertValidation({
          TableName: 'abc',
          FilterExpression: expr[0],
          ExpressionAttributeValues: {':a': expr[2]},
        }, 'Invalid FilterExpression: Incorrect operand type for operator or function; operator or function: ' +
          expr[1] + ', operand type: ' + Object.keys(expr[2])[0], cb)
      }, done)
    })

    it('should return ValidationException for attribute_type with incorrect value', function(done) {
      assertValidation({
        TableName: 'abc',
        FilterExpression: 'attribute_type(a, :a)',
        ExpressionAttributeValues: {':a': {'S': '1'}},
      }, /^Invalid FilterExpression: Invalid attribute type name found; type: 1, valid types: {((B|NULL|SS|BOOL|L|BS|N|NS|S|M),?){10}}$/, done)
    })

    it('should return ValidationException for functions with attr values instead of paths', function(done) {
      var expressions = [
        ['attribute_exists(:a) and a=a', 'attribute_exists'],
        ['attribute_not_exists(size(:a))', 'attribute_not_exists'],
      ]
      async.forEach(expressions, function(expr, cb) {
        assertValidation({
          TableName: 'abc',
          FilterExpression: expr[0],
          ExpressionAttributeValues: {':a': {'S': '1'}},
        }, 'Invalid FilterExpression: Operator or function requires a document path; operator or function: ' + expr[1], cb)
      }, done)
    })

    it('should return ValidationException for non-distinct expressions', function(done) {
      var expressions = [
        ['a = a AND #a = b AND :views > a', '=', '[a]'],
        ['#a <> a', '<>', '[a]'],
        ['a > #a', '>', '[a]'],
        ['((a=a) OR (a=a))', '=', '[a]'],
        ['((a=a) AND (a=a))', '=', '[a]'],
        ['contains(ab.bc[1].a, ab.bc[1].#a)', 'contains', '[ab, bc, [1], a]'],
        ['attribute_type(ab.bc[1].#a, ab.bc[1].a)', 'attribute_type', '[ab, bc, [1], a]'],
        ['begins_with(ab.bc[1].a, ab.bc[1].#a)', 'begins_with', '[ab, bc, [1], a]'],
        // ':a > :a', ... is ok
      ]
      async.forEach(expressions, function(expr, cb) {
        assertValidation({
          TableName: 'abc',
          FilterExpression: expr[0],
          ExpressionAttributeNames: {'#a': 'a'},
        }, 'Invalid FilterExpression: The first operand must be distinct from the remaining operands for this operator or function; operator: ' +
          expr[1] + ', first operand: ' + expr[2], cb)
      }, done)
    })

    it('should check table exists before checking key validity', function(done) {
      async.forEach([
        {},
        {b: {S: 'a'}},
        {a: {S: 'a'}, b: {S: 'a'}},
      ], function(expr, cb) {
        assertNotFound({
          TableName: 'abc',
          ExclusiveStartKey: expr,
        }, 'Requested resource not found', cb)
      }, done)
    })

    it('should return ValidationException if unknown index and bad ExclusiveStartKey in hash table', function(done) {
      async.forEach([
        {},
        // {z: {S: 'a'}}, // Returns a 500
        // {a: {S: 'a'}, b: {S: 'a'}}, // Returns a 500
        {a: {S: 'a'}, b: {S: 'a'}, c: {S: 'a'}},
        {z: {S: 'a'}, y: {S: 'a'}, x: {S: 'a'}},
      ], function(expr, cb) {
        assertValidation({
          TableName: helpers.testHashTable,
          IndexName: 'whatever',
          FilterExpression: 'attribute_exists(a.b.c)',
          ExclusiveStartKey: expr,
        }, 'The table does not have the specified index: whatever', cb)
      }, done)
    })

    it('should return ValidationException if unknown index and bad ExclusiveStartKey in range table', function(done) {
      async.forEach([
        {},
        {z: {S: 'a'}},
        // {a: {S: 'a'}, b: {S: 'a'}}, // Returns a 500
        {a: {S: 'a'}, b: {S: 'a'}, c: {S: 'a'}},
        {z: {S: 'a'}, y: {S: 'a'}, x: {S: 'a'}, w: {S: 'a'}},
      ], function(expr, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          IndexName: 'whatever',
          FilterExpression: 'attribute_exists(a.b.c)',
          ExclusiveStartKey: expr,
        }, 'The table does not have the specified index: whatever', cb)
      }, done)
    })

    it('should return ValidationException if ExclusiveStartKey is invalid for local index', function(done) {
      async.forEach([
        {},
        {z: {N: '1'}},
        {a: {B: 'abcd'}},
        {a: {S: 'a'}},
        {a: {S: 'a'}, b: {S: 'a'}},
        {a: {S: 'a'}, c: {S: 'a'}},
        {b: {S: 'a'}, c: {S: 'a'}},
        {a: {S: 'a'}, c: {N: '1'}},
        {a: {S: 'a'}, z: {S: '1'}},
        {a: {S: 'a'}, b: {S: '1'}, c: {S: '1'}, d: {S: '1'}},
      ], function(expr, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          IndexName: 'index1',
          ExclusiveStartKey: expr,
        }, 'The provided starting key is invalid', cb)
      }, done)
    })

    it('should return ValidationException if ExclusiveStartKey is invalid for global index', function(done) {
      async.forEach([
        {},
        {z: {N: '1'}},
        {a: {B: 'abcd'}},
        {a: {S: 'a'}},
        {c: {N: '1'}},
        {c: {S: '1'}},
        {a: {S: 'a'}, b: {S: 'a'}},
        {a: {S: 'a'}, c: {S: 'a'}},
        {a: {S: 'a'}, b: {S: 'a'}, z: {S: 'a'}},
        {a: {S: 'a'}, b: {S: 'a'}, c: {S: 'a'}, z: {S: 'a'}},
      ], function(expr, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          IndexName: 'index3',
          ExclusiveStartKey: expr,
        }, 'The provided starting key is invalid', cb)
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
        assertValidation({
          TableName: helpers.testRangeTable,
          IndexName: 'index4',
          Select: 'ALL_ATTRIBUTES',
          ExclusiveStartKey: expr,
        }, 'The provided starting key is invalid', cb)
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
          FilterExpression: 'attribute_exists(a.b.c)',
        }, 'The table does not have the specified index: whatever', cb)
      }, done)
    })

    it('should return ValidationException for specifying ALL_ATTRIBUTES when global index does not have ALL', function(done) {
      assertValidation({
        TableName: helpers.testRangeTable,
        FilterExpression: 'attribute_exists(a.b.c)',
        IndexName: 'index4',
        Select: 'ALL_ATTRIBUTES',
        ExclusiveStartKey: {x: {N: '1'}, y: {N: '1'}, c: {S: 'a'}, d: {S: 'a'}},
      }, 'One or more parameter values were invalid: ' +
        'Select type ALL_ATTRIBUTES is not supported for global secondary index index4 ' +
        'because its projection type is not ALL', done)
    })

    it('should return ValidationException if ExclusiveStartKey does not match schema for local index', function(done) {
      async.forEach([
        {a: {N: '1'}, x: {S: '1'}, y: {S: '1'}},
        {a: {B: 'YQ=='}, b: {S: '1'}, c: {S: '1'}},
        {a: {S: 'a'}, b: {N: '1'}, c: {N: '1'}},
        {a: {S: 'a'}, b: {B: 'YQ=='}, c: {N: '1'}},
        {a: {S: 'a'}, b: {S: 'a'}, c: {N: '1'}},
        {a: {S: 'a'}, b: {S: 'a'}, c: {B: 'YQ=='}},
      ], function(expr, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          IndexName: 'index1',
          ExclusiveStartKey: expr,
        }, 'The provided key element does not match the schema', cb)
      }, done)
    })

    it('should return ValidationException if ExclusiveStartKey does not match schema for global index', function(done) {
      async.forEach([
        {x: {S: '1'}, y: {S: '1'}, c: {N: '1'}},
        {a: {S: '1'}, b: {S: '1'}, c: {B: 'YQ=='}},
      ], function(expr, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          IndexName: 'index3',
          ExclusiveStartKey: expr,
        }, 'The provided key element does not match the schema', cb)
      }, done)
    })

    it('should return ValidationException if ExclusiveStartKey does not match schema for global compound index', function(done) {
      async.forEach([
        {x: {N: '1'}, y: {N: '1'}, c: {S: '1'}, d: {N: '1'}},
        {x: {N: '1'}, y: {N: '1'}, c: {N: '1'}, d: {S: '1'}},
      ], function(expr, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          IndexName: 'index4',
          ExclusiveStartKey: expr,
        }, 'The provided key element does not match the schema', cb)
      }, done)
    })

    it('should return ValidationException if ExclusiveStartKey does not match schema', function(done) {
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
        assertValidation({
          TableName: helpers.testHashTable,
          FilterExpression: 'attribute_exists(a.b.c)',
          ExclusiveStartKey: expr,
        }, 'The provided starting key is invalid: The provided key element does not match the schema', cb)
      }, done)
    })

    it('should return ValidationException if ExclusiveStartKey for range table is invalid', function(done) {
      async.forEach([
        {},
        {z: {N: '1'}},
        {b: {S: 'a'}, c: {S: 'b'}},
        {a: {B: 'abcd'}},
        {a: {S: 'a'}},
        {a: {N: '1'}, b: {S: 'a'}, c: {S: 'b'}},
        {a: {N: '1'}, b: {N: '1'}, z: {N: '1'}},
        {a: {N: '1'}, z: {S: 'a'}},
        {a: {B: 'YQ=='}, b: {S: 'a'}},
        {a: {S: 'a'}},
        {a: {S: 'a'}, c: {N: '1'}},
        {a: {S: 'a'}, z: {S: '1'}},
        {a: {S: 'a'}, b: {S: '1'}, c: {S: '1'}},
        {a: {S: 'a'}, b: {N: '1'}},
        {a: {S: 'a'}, b: {B: 'YQ=='}},
      ], function(expr, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          ExclusiveStartKey: expr,
        }, 'The provided starting key is invalid: The provided key element does not match the schema', cb)
      }, done)
    })

    it('should return ValidationException if range in ExclusiveStartKey is invalid, but hash and local are ok', function(done) {
      async.forEach([
        {a: {S: '1'}, b: {N: '1'}, c: {S: 'a'}},
        {a: {S: '1'}, b: {B: 'YQ=='}, c: {S: 'a'}},
      ], function(expr, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          IndexName: 'index1',
          ExclusiveStartKey: expr,
        }, 'The provided starting key is invalid: The provided key element does not match the schema', cb)
      }, done)
    })

    it('should return ValidationException if global hash in ExclusiveStartKey but bad in query', function(done) {
      async.forEach([
        {x: {N: '1'}, y: {N: '1'}, c: {S: 'a'}},
        {a: {N: '1'}, b: {S: '1'}, c: {S: 'a'}},
        {a: {S: '1'}, b: {N: '1'}, c: {S: 'a'}},
      ], function(expr, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          IndexName: 'index3',
          ExclusiveStartKey: expr,
        }, 'The provided starting key is invalid: The provided key element does not match the schema', cb)
      }, done)
    })

    it('should return ValidationException if global range in ExclusiveStartKey but bad in query', function(done) {
      async.forEach([
        {x: {N: '1'}, y: {N: '1'}, c: {S: 'a'}, d: {S: 'a'}},
        {a: {N: '1'}, b: {S: '1'}, c: {S: 'a'}, d: {S: 'a'}},
        {a: {S: '1'}, b: {N: '1'}, c: {S: 'a'}, d: {S: 'a'}},
      ], function(expr, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          IndexName: 'index4',
          ExclusiveStartKey: expr,
        }, 'The provided starting key is invalid: The provided key element does not match the schema', cb)
      }, done)
    })

    it('should return ValidationException if ExclusiveStartKey is from different segment', function(done) {
      var i, items = [], batchReq = {RequestItems: {}}

      for (i = 0; i < 10; i++)
        items.push({a: {S: String(i)}})

      batchReq.RequestItems[helpers.testHashTable] = items.map(function(item) { return {PutRequest: {Item: item}} })

      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        request(opts({TableName: helpers.testHashTable, Segment: 1, TotalSegments: 2}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Count.should.be.above(0)

          assertValidation({TableName: helpers.testHashTable,
            Segment: 0,
            TotalSegments: 2,
            FilterExpression: 'attribute_exists(a.b.c)',
            ExclusiveStartKey: {a: res.body.Items[0].a}},
            'The provided starting key is invalid: ' +
            'Invalid ExclusiveStartKey. Please use ExclusiveStartKey with correct Segment. ' +
            'TotalSegments: 2 Segment: 0', done)
        })
      })
    })

    it('should return ValidationException for non-scalar key access', function(done) {
      var expressions = [
        'attribute_exists(a.b.c) and #a = b',
        'attribute_exists(#a.b.c)',
        'attribute_exists(#a[0])',
      ]
      async.forEach(expressions, function(expression, cb) {
        assertValidation({
          TableName: helpers.testHashTable,
          FilterExpression: expression,
          ExpressionAttributeNames: {'#a': 'a'},
        }, 'Key attributes must be scalars; list random access \'[]\' and map lookup \'.\' are not allowed: Key: a', cb)
      }, done)
    })

  })

  describe('functionality', function() {

    it('should scan with no filter', function(done) {
      var item = {a: {S: helpers.randomString()}}
      request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({TableName: helpers.testHashTable}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Items.should.containEql(item)
          res.body.Count.should.be.above(0)
          res.body.ScannedCount.should.be.above(0)
          done()
        })
      })
    })

    it('should scan by id (type S)', function(done) {
      var item = {a: {S: helpers.randomString()}}
      request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          {ScanFilter: {a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]}}},
          {FilterExpression: 'a = :a', ExpressionAttributeValues: {':a': item.a}},
          {FilterExpression: '#a = :a', ExpressionAttributeValues: {':a': item.a}, ExpressionAttributeNames: {'#a': 'a'}},
        ], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.eql([item])
            res.body.Count.should.equal(1)
            res.body.ScannedCount.should.be.above(0)
            cb()
          })
        }, done)
      })
    })

    it('should return empty if no match', function(done) {
      var item = {a: {S: helpers.randomString()}}
      request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          {ScanFilter: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: helpers.randomString()}]}}},
          {FilterExpression: 'a = :a', ExpressionAttributeValues: {':a': {S: helpers.randomString()}}},
        ], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.eql([])
            res.body.Count.should.equal(0)
            res.body.ScannedCount.should.be.above(0)
            cb()
          })
        }, done)
      })
    })

    it('should scan by a non-id property (type N)', function(done) {
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
        async.forEach([
          {ScanFilter: {b: {ComparisonOperator: 'EQ', AttributeValueList: [item.b]}}},
          {FilterExpression: 'b = :b', ExpressionAttributeValues: {':b': item.b}},
        ], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item)
            res.body.Items.should.containEql(item2)
            res.body.Items.should.have.length(2)
            res.body.Count.should.equal(2)
            cb()
          })
        }, done)
      })
    })

    it('should scan by multiple properties', function(done) {
      var item = {a: {S: helpers.randomString()}, date: {N: helpers.randomNumber()}, c: {N: helpers.randomNumber()}},
          item2 = {a: {S: helpers.randomString()}, date: item.date, c: item.c},
          item3 = {a: {S: helpers.randomString()}, date: item.date, c: {N: helpers.randomNumber()}},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            date: {ComparisonOperator: 'EQ', AttributeValueList: [item.date]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: '#d = :date AND c = :c',
          ExpressionAttributeValues: {':date': item.date, ':c': item.c},
          ExpressionAttributeNames: {'#d': 'date'},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200, res.rawBody)
            res.body.Items.should.containEql(item)
            res.body.Items.should.containEql(item2)
            res.body.Items.should.have.length(2)
            res.body.Count.should.equal(2)
            cb()
          })
        }, done)
      })
    })

    it('should scan by EQ on type B', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {B: 'abcd'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {B: 'abcd'}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: 'Yg=='}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'EQ', AttributeValueList: [item.b]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'b = :b AND c = :c',
          ExpressionAttributeValues: {':b': item.b, ':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item)
            res.body.Items.should.containEql(item2)
            res.body.Items.should.have.length(2)
            res.body.Count.should.equal(2)
            cb()
          })
        }, done)
      })
    })

    it('should scan by EQ on type SS', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {SS: ['a', 'b']}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {SS: ['a', 'b']}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {SS: ['a', 'b', 'c']}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'EQ', AttributeValueList: [{SS: ['b', 'a']}]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'b = :b AND c = :c',
          ExpressionAttributeValues: {':b': {SS: ['b', 'a']}, ':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item)
            res.body.Items.should.containEql(item2)
            res.body.Items.should.have.length(2)
            res.body.Count.should.equal(2)
            cb()
          })
        }, done)
      })
    })

    it('should scan by EQ on type NS', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {NS: ['1', '2']}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {NS: ['1', '2']}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {NS: ['1', '2', '3']}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'EQ', AttributeValueList: [{NS: ['2', '1']}]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'b = :b AND c = :c',
          ExpressionAttributeValues: {':b': {NS: ['2', '1']}, ':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.forEach(function(item) {
              item.b.NS.should.have.length(2)
              item.b.NS.should.containEql('1')
              item.b.NS.should.containEql('2')
              delete item.b
            })
            delete item.b
            delete item2.b
            res.body.Items.should.containEql(item)
            res.body.Items.should.containEql(item2)
            res.body.Items.should.have.length(2)
            res.body.Count.should.equal(2)
            cb()
          })
        }, done)
      })
    })

    it('should scan by EQ on type BS', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {BS: ['Yg==', 'abcd']}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {BS: ['Yg==', 'abcd']}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {BS: ['Yg==', 'abcd', '1234']}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'EQ', AttributeValueList: [{BS: ['abcd', 'Yg==']}]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'b = :b AND c = :c',
          ExpressionAttributeValues: {':b': {BS: ['abcd', 'Yg==']}, ':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item)
            res.body.Items.should.containEql(item2)
            res.body.Items.should.have.length(2)
            res.body.Count.should.equal(2)
            cb()
          })
        }, done)
      })
    })

    it('should scan by EQ on different types', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: '1234'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {N: '1234'}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: '1234'}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'EQ', AttributeValueList: [item.b]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'b = :b AND c = :c',
          ExpressionAttributeValues: {':b': item.b, ':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.eql([item])
            res.body.Count.should.equal(1)
            cb()
          })
        }, done)
      })
    })

    it('should scan by NE on different types', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: '1234'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {N: '1234'}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: '1234'}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'NE', AttributeValueList: [item.b]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'b <> :b AND c = :c',
          ExpressionAttributeValues: {':b': item.b, ':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item2)
            res.body.Items.should.containEql(item3)
            res.body.Items.should.have.length(2)
            res.body.Count.should.equal(2)
            cb()
          })
        }, done)
      })
    })

    it('should scan by NE on type SS', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {SS: ['a', 'b']}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {SS: ['a', 'b']}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {SS: ['a', 'b', 'c']}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'NE', AttributeValueList: [{SS: ['b', 'a']}]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'b <> :b AND c = :c',
          ExpressionAttributeValues: {':b': {SS: ['b', 'a']}, ':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item3)
            res.body.Items.should.have.length(1)
            res.body.Count.should.equal(1)
            cb()
          })
        }, done)
      })
    })

    it('should scan by NE on type NS', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {NS: ['1', '2']}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {NS: ['1', '2']}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {NS: ['3', '2', '1']}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'NE', AttributeValueList: [{NS: ['2', '1']}]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'b <> :b AND c = :c',
          ExpressionAttributeValues: {':b': {NS: ['2', '1']}, ':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item3)
            res.body.Items.should.have.length(1)
            res.body.Count.should.equal(1)
            cb()
          })
        }, done)
      })
    })

    it('should scan by NE on type BS', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {BS: ['Yg==', 'abcd']}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {BS: ['Yg==', 'abcd']}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {BS: ['Yg==', 'abcd', '1234']}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'NE', AttributeValueList: [{BS: ['abcd', 'Yg==']}]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'b <> :b AND c = :c',
          ExpressionAttributeValues: {':b': {BS: ['abcd', 'Yg==']}, ':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item3)
            res.body.Items.should.have.length(1)
            res.body.Count.should.equal(1)
            cb()
          })
        }, done)
      })
    })

    it('should scan by LE on type S', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'abd'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {S: 'abc\xff'}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {S: 'abc'}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {S: 'abd\x00'}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {S: 'ab'}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
        {PutRequest: {Item: item4}},
        {PutRequest: {Item: item5}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'LE', AttributeValueList: [item.b]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'b <= :b AND c = :c',
          ExpressionAttributeValues: {':b': item.b, ':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item)
            res.body.Items.should.containEql(item2)
            res.body.Items.should.containEql(item3)
            res.body.Items.should.containEql(item5)
            res.body.Items.should.have.length(4)
            res.body.Count.should.equal(4)
            cb()
          })
        }, done)
      })
    })

    it('should scan by LE on type N with decimals', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {N: '2'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {N: '1.9999'}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {N: '1'}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {N: '2.00000001'}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {N: '-0.5'}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
        {PutRequest: {Item: item4}},
        {PutRequest: {Item: item5}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'LE', AttributeValueList: [item.b]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'b <= :b AND c = :c',
          ExpressionAttributeValues: {':b': item.b, ':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item)
            res.body.Items.should.containEql(item2)
            res.body.Items.should.containEql(item3)
            res.body.Items.should.containEql(item5)
            res.body.Items.should.have.length(4)
            res.body.Count.should.equal(4)
            cb()
          })
        }, done)
      })
    })

    it('should scan by LE on type N without decimals', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {N: '2'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {N: '19999'}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {N: '1'}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {N: '200000001'}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {N: '-5'}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
        {PutRequest: {Item: item4}},
        {PutRequest: {Item: item5}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'LE', AttributeValueList: [item.b]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'b <= :b AND c = :c',
          ExpressionAttributeValues: {':b': item.b, ':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item)
            res.body.Items.should.containEql(item3)
            res.body.Items.should.containEql(item5)
            res.body.Items.should.have.length(3)
            res.body.Count.should.equal(3)
            cb()
          })
        }, done)
      })
    })

    it('should scan by LE on type B', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {B: Buffer.from('ce', 'hex').toString('base64')}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('d0', 'hex').toString('base64')}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('cf', 'hex').toString('base64')}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('d000', 'hex').toString('base64')}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('cfff', 'hex').toString('base64')}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
        {PutRequest: {Item: item4}},
        {PutRequest: {Item: item5}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'LE', AttributeValueList: [item2.b]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'b <= :b AND c = :c',
          ExpressionAttributeValues: {':b': item2.b, ':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item)
            res.body.Items.should.containEql(item2)
            res.body.Items.should.containEql(item3)
            res.body.Items.should.containEql(item5)
            res.body.Items.should.have.length(4)
            res.body.Count.should.equal(4)
            cb()
          })
        }, done)
      })
    })

    it('should scan by LT on type S', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'abd'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {S: 'abc\xff'}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {S: 'abc'}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {S: 'abd\x00'}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {S: 'ab'}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
        {PutRequest: {Item: item4}},
        {PutRequest: {Item: item5}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'LT', AttributeValueList: [item.b]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'b < :b AND c = :c',
          ExpressionAttributeValues: {':b': item.b, ':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item2)
            res.body.Items.should.containEql(item3)
            res.body.Items.should.containEql(item5)
            res.body.Items.should.have.length(3)
            res.body.Count.should.equal(3)
            cb()
          })
        }, done)
      })
    })

    it('should scan by LT on type N', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {N: '2'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {N: '1.9999'}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {N: '1'}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {N: '2.00000001'}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {N: '-0.5'}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
        {PutRequest: {Item: item4}},
        {PutRequest: {Item: item5}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'LT', AttributeValueList: [item.b]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'b < :b AND c = :c',
          ExpressionAttributeValues: {':b': item.b, ':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item2)
            res.body.Items.should.containEql(item3)
            res.body.Items.should.containEql(item5)
            res.body.Items.should.have.length(3)
            res.body.Count.should.equal(3)
            cb()
          })
        }, done)
      })
    })

    it('should scan by LT on type B', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {B: Buffer.from('ce', 'hex').toString('base64')}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('d0', 'hex').toString('base64')}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('cf', 'hex').toString('base64')}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('d000', 'hex').toString('base64')}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('cfff', 'hex').toString('base64')}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
        {PutRequest: {Item: item4}},
        {PutRequest: {Item: item5}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'LT', AttributeValueList: [item2.b]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'b < :b AND c = :c',
          ExpressionAttributeValues: {':b': item2.b, ':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item)
            res.body.Items.should.containEql(item3)
            res.body.Items.should.containEql(item5)
            res.body.Items.should.have.length(3)
            res.body.Count.should.equal(3)
            cb()
          })
        }, done)
      })
    })

    it('should scan by GE on type S', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'abd'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {S: 'abc\xff'}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {S: 'abc'}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {S: 'abd\x00'}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {S: 'ab'}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
        {PutRequest: {Item: item4}},
        {PutRequest: {Item: item5}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'GE', AttributeValueList: [item3.b]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'b >= :b AND c = :c',
          ExpressionAttributeValues: {':b': item3.b, ':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item)
            res.body.Items.should.containEql(item2)
            res.body.Items.should.containEql(item3)
            res.body.Items.should.containEql(item4)
            res.body.Items.should.have.length(4)
            res.body.Count.should.equal(4)
            cb()
          })
        }, done)
      })
    })

    it('should scan by GE on type N', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {N: '2'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {N: '1.9999'}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {N: '1'}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {N: '2.00000001'}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {N: '-0.5'}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
        {PutRequest: {Item: item4}},
        {PutRequest: {Item: item5}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'GE', AttributeValueList: [item2.b]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'b >= :b AND c = :c',
          ExpressionAttributeValues: {':b': item2.b, ':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item)
            res.body.Items.should.containEql(item2)
            res.body.Items.should.containEql(item4)
            res.body.Items.should.have.length(3)
            res.body.Count.should.equal(3)
            cb()
          })
        }, done)
      })
    })

    it('should scan by GE on type B', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {B: Buffer.from('ce', 'hex').toString('base64')}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('d0', 'hex').toString('base64')}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('cf', 'hex').toString('base64')}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('d000', 'hex').toString('base64')}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('cfff', 'hex').toString('base64')}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
        {PutRequest: {Item: item4}},
        {PutRequest: {Item: item5}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'GE', AttributeValueList: [item3.b]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'b >= :b AND c = :c',
          ExpressionAttributeValues: {':b': item3.b, ':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item2)
            res.body.Items.should.containEql(item3)
            res.body.Items.should.containEql(item4)
            res.body.Items.should.containEql(item5)
            res.body.Items.should.have.length(4)
            res.body.Count.should.equal(4)
            cb()
          })
        }, done)
      })
    })

    it('should scan by GT on type S', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'abd'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {S: 'abc\xff'}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {S: 'abc'}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {S: 'abd\x00'}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {S: 'ab'}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
        {PutRequest: {Item: item4}},
        {PutRequest: {Item: item5}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'GT', AttributeValueList: [item3.b]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'b > :b AND c = :c',
          ExpressionAttributeValues: {':b': item3.b, ':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item)
            res.body.Items.should.containEql(item2)
            res.body.Items.should.containEql(item4)
            res.body.Items.should.have.length(3)
            res.body.Count.should.equal(3)
            cb()
          })
        }, done)
      })
    })

    it('should scan by GT on type N', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {N: '2'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {N: '1.9999'}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {N: '1'}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {N: '2.00000001'}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {N: '-0.5'}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
        {PutRequest: {Item: item4}},
        {PutRequest: {Item: item5}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'GT', AttributeValueList: [item2.b]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'b > :b AND c = :c',
          ExpressionAttributeValues: {':b': item2.b, ':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item)
            res.body.Items.should.containEql(item4)
            res.body.Items.should.have.length(2)
            res.body.Count.should.equal(2)
            cb()
          })
        }, done)
      })
    })

    it('should scan by GT on type B', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {B: Buffer.from('ce', 'hex').toString('base64')}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('d0', 'hex').toString('base64')}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('cf', 'hex').toString('base64')}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('d000', 'hex').toString('base64')}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('cfff', 'hex').toString('base64')}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
        {PutRequest: {Item: item4}},
        {PutRequest: {Item: item5}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'GT', AttributeValueList: [item3.b]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'b > :b AND c = :c',
          ExpressionAttributeValues: {':b': item3.b, ':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item2)
            res.body.Items.should.containEql(item4)
            res.body.Items.should.containEql(item5)
            res.body.Items.should.have.length(3)
            res.body.Count.should.equal(3)
            cb()
          })
        }, done)
      })
    })

    it('should scan by NOT_NULL', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: helpers.randomString()}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {S: helpers.randomString()}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'NOT_NULL'},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'attribute_exists(b) AND c = :c',
          ExpressionAttributeValues: {':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item)
            res.body.Items.should.containEql(item2)
            res.body.Items.should.have.length(2)
            res.body.Count.should.equal(2)
            cb()
          })
        }, done)
      })
    })

    it('should scan by NULL', function(done) {
      var item = {a: {S: helpers.randomString()}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {S: helpers.randomString()}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'NULL'},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'attribute_not_exists(b) AND c = :c',
          ExpressionAttributeValues: {':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item)
            res.body.Items.should.containEql(item2)
            res.body.Items.should.have.length(2)
            res.body.Count.should.equal(2)
            cb()
          })
        }, done)
      })
    })

    it('should scan by CONTAINS on type S', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'abdef'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {SS: ['abd', 'bde']}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('abdef').toString('base64')}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {BS: ['abcd', Buffer.from('bde').toString('base64')]}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {S: 'bde'}, c: item.c},
          item6 = {a: {S: helpers.randomString()}, b: {S: 'abd'}, c: item.c},
          item7 = {a: {S: helpers.randomString()}, b: {L: [{'N': '123'}, {'S': 'bde'}]}, c: item.c},
          item8 = {a: {S: helpers.randomString()}, b: {L: [{'S': 'abd'}]}, c: item.c},
          item9 = {a: {S: helpers.randomString()}, b: {L: [{'S': 'abde'}]}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
        {PutRequest: {Item: item4}},
        {PutRequest: {Item: item5}},
        {PutRequest: {Item: item6}},
        {PutRequest: {Item: item7}},
        {PutRequest: {Item: item8}},
        {PutRequest: {Item: item9}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'CONTAINS', AttributeValueList: [item5.b]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'contains(b, :b) AND c = :c',
          ExpressionAttributeValues: {':b': item5.b, ':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item)
            res.body.Items.should.containEql(item2)
            res.body.Items.should.containEql(item5)
            res.body.Items.should.containEql(item7)
            res.body.Items.should.have.length(4)
            res.body.Count.should.equal(4)
            cb()
          })
        }, done)
      })
    })

    it('should scan by CONTAINS on type N', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {N: '1234'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {NS: ['123', '234']}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('1234').toString('base64')}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {BS: [Buffer.from('234').toString('base64')]}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {SS: ['234']}, c: item.c},
          item6 = {a: {S: helpers.randomString()}, b: {L: [{'S': 'abd'}, {'N': '234'}]}, c: item.c},
          item7 = {a: {S: helpers.randomString()}, b: {L: [{'N': '123'}]}, c: item.c},
          item8 = {a: {S: helpers.randomString()}, b: {L: [{'N': '1234'}]}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
        {PutRequest: {Item: item4}},
        {PutRequest: {Item: item5}},
        {PutRequest: {Item: item6}},
        {PutRequest: {Item: item7}},
        {PutRequest: {Item: item8}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'CONTAINS', AttributeValueList: [{N: '234'}]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'contains(b, :b) AND c = :c',
          ExpressionAttributeValues: {':b': {N: '234'}, ':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item2)
            res.body.Items.should.containEql(item6)
            res.body.Items.should.have.lengthOf(2)
            res.body.Count.should.equal(2)
            cb()
          })
        }, done)
      })
    })

    it('should scan by CONTAINS on type B', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'abdef'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {SS: ['abd', 'bde']}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('abdef').toString('base64')}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {BS: [Buffer.from('bde').toString('base64'), 'abcd']}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('bde').toString('base64')}, c: item.c},
          item6 = {a: {S: helpers.randomString()}, b: {S: 'abd'}, c: item.c},
          item7 = {a: {S: helpers.randomString()}, b: {L: [{'N': '123'}, {'B': Buffer.from('bde').toString('base64')}]}, c: item.c},
          item8 = {a: {S: helpers.randomString()}, b: {L: [{'B': Buffer.from('abd').toString('base64')}]}, c: item.c},
          item9 = {a: {S: helpers.randomString()}, b: {L: [{'B': Buffer.from('abde').toString('base64')}]}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
        {PutRequest: {Item: item4}},
        {PutRequest: {Item: item5}},
        {PutRequest: {Item: item6}},
        {PutRequest: {Item: item7}},
        {PutRequest: {Item: item8}},
        {PutRequest: {Item: item9}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'CONTAINS', AttributeValueList: [item5.b]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'contains(b, :b) AND c = :c',
          ExpressionAttributeValues: {':b': item5.b, ':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item3)
            res.body.Items.should.containEql(item4)
            res.body.Items.should.containEql(item5)
            res.body.Items.should.containEql(item7)
            res.body.Items.should.have.length(4)
            res.body.Count.should.equal(4)
            cb()
          })
        }, done)
      })
    })

    it('should scan by NOT_CONTAINS on type S', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'abdef'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {SS: ['abd', 'bde']}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('abdef').toString('base64')}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {BS: [Buffer.from('bde').toString('base64'), 'abcd']}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {S: 'bde'}, c: item.c},
          item6 = {a: {S: helpers.randomString()}, b: {S: 'abd'}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
        {PutRequest: {Item: item4}},
        {PutRequest: {Item: item5}},
        {PutRequest: {Item: item6}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'NOT_CONTAINS', AttributeValueList: [item5.b]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'NOT contains(b, :b) AND c = :c',
          ExpressionAttributeValues: {':b': item5.b, ':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item3)
            res.body.Items.should.containEql(item4)
            res.body.Items.should.containEql(item6)
            res.body.Items.should.have.length(3)
            res.body.Count.should.equal(3)
            cb()
          })
        }, done)
      })
    })

    it('should scan by NOT_CONTAINS on type N', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {N: '1234'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {NS: ['123', '234']}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('1234').toString('base64')}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {BS: [Buffer.from('234').toString('base64')]}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {SS: ['234']}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
        {PutRequest: {Item: item4}},
        {PutRequest: {Item: item5}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'NOT_CONTAINS', AttributeValueList: [{N: '234'}]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'NOT contains(b, :b) AND c = :c',
          ExpressionAttributeValues: {':b': {N: '234'}, ':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item)
            res.body.Items.should.containEql(item3)
            res.body.Items.should.containEql(item4)
            res.body.Items.should.containEql(item5)
            res.body.Items.should.have.length(4)
            res.body.Count.should.equal(4)
            cb()
          })
        }, done)
      })
    })

    it('should scan by NOT_CONTAINS on type B', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'abdef'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {SS: ['abd', 'bde']}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('abdef').toString('base64')}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {BS: [Buffer.from('bde').toString('base64'), 'abcd']}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('bde').toString('base64')}, c: item.c},
          item6 = {a: {S: helpers.randomString()}, b: {S: 'abd'}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
        {PutRequest: {Item: item4}},
        {PutRequest: {Item: item5}},
        {PutRequest: {Item: item6}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'NOT_CONTAINS', AttributeValueList: [item5.b]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'NOT contains(b, :b) AND c = :c',
          ExpressionAttributeValues: {':b': item5.b, ':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item)
            res.body.Items.should.containEql(item2)
            res.body.Items.should.containEql(item6)
            res.body.Items.should.have.length(3)
            res.body.Count.should.equal(3)
            cb()
          })
        }, done)
      })
    })

    it('should scan by BEGINS_WITH on type S', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'abdef'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {SS: ['abd', 'bde']}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('abdef').toString('base64')}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {S: 'ab'}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {S: 'abd'}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
        {PutRequest: {Item: item4}},
        {PutRequest: {Item: item5}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'BEGINS_WITH', AttributeValueList: [item5.b]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'begins_with(b, :b) AND c = :c',
          ExpressionAttributeValues: {':b': item5.b, ':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item)
            res.body.Items.should.containEql(item5)
            res.body.Items.should.have.length(2)
            res.body.Count.should.equal(2)
            cb()
          })
        }, done)
      })
    })

    it('should scan by BEGINS_WITH on type B', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'abdef'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {SS: ['abd', 'bde']}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('abdef').toString('base64')}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {S: 'ab'}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('abd').toString('base64')}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
        {PutRequest: {Item: item4}},
        {PutRequest: {Item: item5}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'BEGINS_WITH', AttributeValueList: [item5.b]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'begins_with(b, :b) AND c = :c',
          ExpressionAttributeValues: {':b': item5.b, ':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item3)
            res.body.Items.should.containEql(item5)
            res.body.Items.should.have.length(2)
            res.body.Count.should.equal(2)
            cb()
          })
        }, done)
      })
    })

    it('should scan by IN on type S', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'abdef'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {SS: ['abd', 'bde']}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('abdef').toString('base64')}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {S: 'ab'}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {S: 'abd'}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
        {PutRequest: {Item: item4}},
        {PutRequest: {Item: item5}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'IN', AttributeValueList: [item5.b, item.b]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'b IN (:b, :d) AND c = :c',
          ExpressionAttributeValues: {':b': item5.b, ':c': item.c, ':d': item.b},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item)
            res.body.Items.should.containEql(item5)
            res.body.Items.should.have.length(2)
            res.body.Count.should.equal(2)
            cb()
          })
        }, done)
      })
    })

    it('should scan by IN on type N', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: '1234'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {NS: ['1234']}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('1234').toString('base64')}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {N: '1234'}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {N: '123.45'}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
        {PutRequest: {Item: item4}},
        {PutRequest: {Item: item5}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'IN', AttributeValueList: [item4.b, item5.b]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'b IN (:b, :d) AND c = :c',
          ExpressionAttributeValues: {':b': item4.b, ':c': item.c, ':d': item5.b},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item4)
            res.body.Items.should.containEql(item5)
            res.body.Items.should.have.length(2)
            res.body.Count.should.equal(2)
            cb()
          })
        }, done)
      })
    })

    it('should scan by IN on type B', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: '1234'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {BS: [Buffer.from('1234').toString('base64')]}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('1234').toString('base64')}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {N: '1234'}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('12345').toString('base64')}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
        {PutRequest: {Item: item4}},
        {PutRequest: {Item: item5}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'IN', AttributeValueList: [item3.b, item5.b]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'b IN (:b, :d) AND c = :c',
          ExpressionAttributeValues: {':b': item3.b, ':c': item.c, ':d': item5.b},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item3)
            res.body.Items.should.containEql(item5)
            res.body.Items.should.have.length(2)
            res.body.Count.should.equal(2)
            cb()
          })
        }, done)
      })
    })

    it('should scan by BETWEEN on type S', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'abc'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {S: 'abd'}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {S: 'abd\x00'}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {S: 'abe'}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {S: 'abe\x00'}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
        {PutRequest: {Item: item4}},
        {PutRequest: {Item: item5}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'BETWEEN', AttributeValueList: [item2.b, item4.b]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'b BETWEEN :b AND :d AND c = :c',
          ExpressionAttributeValues: {':b': item2.b, ':c': item.c, ':d': item4.b},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item2)
            res.body.Items.should.containEql(item3)
            res.body.Items.should.containEql(item4)
            res.body.Items.should.have.length(3)
            res.body.Count.should.equal(3)
            cb()
          })
        }, done)
      })
    })

    it('should scan by BETWEEN on type N', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {N: '123'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {N: '124'}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {N: '124.99999'}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {N: '125'}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {N: '125.000001'}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
        {PutRequest: {Item: item4}},
        {PutRequest: {Item: item5}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'BETWEEN', AttributeValueList: [item2.b, item4.b]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'b BETWEEN :b AND :d AND c = :c',
          ExpressionAttributeValues: {':b': item2.b, ':c': item.c, ':d': item4.b},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item2)
            res.body.Items.should.containEql(item3)
            res.body.Items.should.containEql(item4)
            res.body.Items.should.have.length(3)
            res.body.Count.should.equal(3)
            cb()
          })
        }, done)
      })
    })

    it('should scan by BETWEEN on type B', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {B: Buffer.from('ce', 'hex').toString('base64')}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('d0', 'hex').toString('base64')}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('cf', 'hex').toString('base64')}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('d000', 'hex').toString('base64')}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {B: Buffer.from('cfff', 'hex').toString('base64')}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
        {PutRequest: {Item: item4}},
        {PutRequest: {Item: item5}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            b: {ComparisonOperator: 'BETWEEN', AttributeValueList: [item5.b, item4.b]},
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
        }, {
          FilterExpression: 'b BETWEEN :b AND :d AND c = :c',
          ExpressionAttributeValues: {':b': item5.b, ':c': item.c, ':d': item4.b},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item2)
            res.body.Items.should.containEql(item4)
            res.body.Items.should.containEql(item5)
            res.body.Items.should.have.length(3)
            res.body.Count.should.equal(3)
            cb()
          })
        }, done)
      })
    })

    it('should scan by nested properties', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {M: {a: {M: {b: {S: helpers.randomString()}}}}}, c: {N: helpers.randomNumber()}}
      var item2 = {a: {S: helpers.randomString()}, b: {L: [{S: helpers.randomString()}, item.b]}, c: item.c}
      var item3 = {a: {S: helpers.randomString()}, b: item.b, c: {N: helpers.randomNumber()}}
      var item4 = {a: {S: helpers.randomString()}, b: {S: helpers.randomString()}, c: item.c}
      var item5 = {a: {S: helpers.randomString()}, c: item.c}
      var batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
        {PutRequest: {Item: item4}},
        {PutRequest: {Item: item5}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          FilterExpression: '(b[1].a.b = :b OR b.a.b = :b) AND c = :c',
          ExpressionAttributeValues: {':b': item.b.M.a.M.b, ':c': item.c},
        }, {
          FilterExpression: '(attribute_exists(b.a) OR attribute_exists(b[1])) AND c = :c',
          ExpressionAttributeValues: {':c': item.c},
        }, {
          FilterExpression: '(attribute_type(b.a, :m) OR attribute_type(b[1].a, :m)) AND c = :c',
          ExpressionAttributeValues: {':c': item.c, ':m': {S: 'M'}},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item)
            res.body.Items.should.containEql(item2)
            res.body.Items.should.have.length(2)
            res.body.Count.should.equal(2)
            cb()
          })
        }, done)
      })
    })

    it('should calculate size function correctly', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'abÿ'}, c: {N: helpers.randomNumber()}}
      var item2 = {a: {S: helpers.randomString()}, b: {N: '123'}, c: item.c}
      var item3 = {a: {S: helpers.randomString()}, b: {B: 'YWJj'}, c: item.c}
      var item4 = {a: {S: helpers.randomString()}, b: {SS: ['a', 'b', 'c']}, c: item.c}
      var item5 = {a: {S: helpers.randomString()}, b: {L: [{S: 'a'}, {S: 'a'}, {S: 'a'}]}, c: item.c}
      var item6 = {a: {S: helpers.randomString()}, b: {M: {a: {S: 'a'}, b: {S: 'a'}, c: {S: 'a'}}}, c: item.c}
      var item7 = {a: {S: helpers.randomString()}, b: {S: 'abcd'}, c: item.c}
      var batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
        {PutRequest: {Item: item4}},
        {PutRequest: {Item: item5}},
        {PutRequest: {Item: item6}},
        {PutRequest: {Item: item7}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          FilterExpression: 'size(b) = :b AND c = :c',
          ExpressionAttributeValues: {':b': {N: '3'}, ':c': item.c},
        }, {
          FilterExpression: '(size(b)) = :b AND c = :c',
          ExpressionAttributeValues: {':b': {N: '3'}, ':c': item.c},
        }, {
          FilterExpression: '((size(b)) = :b) AND c = :c',
          ExpressionAttributeValues: {':b': {N: '3'}, ':c': item.c},
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(item)
            res.body.Items.should.containEql(item3)
            res.body.Items.should.containEql(item4)
            res.body.Items.should.containEql(item5)
            res.body.Items.should.containEql(item6)
            res.body.Items.should.have.length(5)
            res.body.Count.should.equal(5)
            cb()
          })
        }, done)
      })
    })

    it('should only return requested attributes', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'b1'}, c: {S: helpers.randomString()}, d: {S: 'd1'}},
          item2 = {a: {S: helpers.randomString()}, b: {S: 'b2'}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {S: 'b3'}, c: item.c, d: {S: 'd3'}, e: {S: 'e3'}},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([{
          ScanFilter: {
            c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
          },
          AttributesToGet: ['b', 'd'],
        }, {
          FilterExpression: 'c = :c',
          ExpressionAttributeValues: {':c': item.c},
          ProjectionExpression: 'b, d',
        }, {
          FilterExpression: 'c = :c',
          ExpressionAttributeValues: {':c': item.c},
          ExpressionAttributeNames: {'#b': 'b', '#d': 'd'},
          ProjectionExpression: '#b, #d',
        }], function(scanOpts, cb) {
          scanOpts.TableName = helpers.testHashTable
          request(opts(scanOpts), function(err, res) {
            if (err) return cb(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql({b: {S: 'b1'}, d: {S: 'd1'}})
            res.body.Items.should.containEql({b: {S: 'b2'}})
            res.body.Items.should.containEql({b: {S: 'b3'}, d: {S: 'd3'}})
            res.body.Items.should.have.length(3)
            res.body.Count.should.equal(3)
            cb()
          })
        }, done)
      })
    })

    it('should return COUNT if requested', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: '1'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {N: '1'}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {S: '1'}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {S: '2'}, c: item.c},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
        {PutRequest: {Item: item4}},
        {PutRequest: {Item: item5}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'EQ', AttributeValueList: [item.b]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }, Select: 'COUNT'}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          should.not.exist(res.body.Items)
          res.body.Count.should.equal(2)
          res.body.ScannedCount.should.be.above(1)
          done()
        })
      })
    })

    it('should return after but not including ExclusiveStartKey', function(done) {
      var i, b = {S: helpers.randomString()}, items = [], batchReq = {RequestItems: {}},
          scanFilter = {b: {ComparisonOperator: 'EQ', AttributeValueList: [b]}}

      for (i = 0; i < 10; i++)
        items.push({a: {S: String(i)}, b: b})

      batchReq.RequestItems[helpers.testHashTable] = items.map(function(item) { return {PutRequest: {Item: item}} })

      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        request(opts({TableName: helpers.testHashTable, ScanFilter: scanFilter}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Count.should.equal(10)

          request(opts({TableName: helpers.testHashTable, ScanFilter: scanFilter, ExclusiveStartKey: {a: res.body.Items[0].a}}), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.Count.should.equal(9)
            done()
          })
        })
      })
    })

    it('should succeed even if ExclusiveStartKey does not match scan filter', function(done) {
      var hashes = [helpers.randomString(), helpers.randomString()].sort()
      request(opts({
        TableName: helpers.testHashTable,
        ExclusiveStartKey: {a: {S: hashes[1]}},
        ScanFilter: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: hashes[0]}]}},
      }), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.Count.should.equal(0)
        res.body.Items.should.eql([])
        done()
      })
    })

    it('should return LastEvaluatedKey if Limit not reached', function(done) {
      var i, b = {S: helpers.randomString()}, items = [], batchReq = {RequestItems: {}}

      for (i = 0; i < 5; i++)
        items.push({a: {S: String(i)}, b: b})

      batchReq.RequestItems[helpers.testHashTable] = items.map(function(item) { return {PutRequest: {Item: item}} })

      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        request(opts({TableName: helpers.testHashTable, Limit: 3, ReturnConsumedCapacity: 'INDEXES'}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.ScannedCount.should.equal(3)
          res.body.LastEvaluatedKey.a.S.should.not.be.empty // eslint-disable-line no-unused-expressions
          Object.keys(res.body.LastEvaluatedKey).should.have.length(1)
          done()
        })
      })
    })

    it('should return LastEvaluatedKey even if selecting Count', function(done) {
      var i, b = {S: helpers.randomString()}, items = [], batchReq = {RequestItems: {}}

      for (i = 0; i < 5; i++)
        items.push({a: {S: String(i)}, b: b})

      batchReq.RequestItems[helpers.testHashTable] = items.map(function(item) { return {PutRequest: {Item: item}} })

      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        request(opts({TableName: helpers.testHashTable, Limit: 3, Select: 'COUNT'}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.ScannedCount.should.equal(3)
          res.body.LastEvaluatedKey.a.S.should.not.be.empty // eslint-disable-line no-unused-expressions
          Object.keys(res.body.LastEvaluatedKey).should.have.length(1)
          done()
        })
      })
    })

    it('should return LastEvaluatedKey while filtering, even if Limit is smaller than the expected return list', function(done) {
      var i, items = [], batchReq = {RequestItems: {}}

      // This bug manifests itself when the sought after item is not among the first .Limit number of
      // items in the scan.  Because we can't guarantee the order of the returned scan items, we can't
      // guarantee that this test case will produce the bug.  Therefore, we will try to make it very
      // likely that this bug will be reproduced by adding as many items as we can.  The chances that
      // the sought after item (to be picked up by the filter) will be among the first .Limit number
      // of items should be small enough to give us practical assurance of correctness in this one
      // regard...
      for (i = 0; i < 25; i++)
        items.push({a: {S: 'item' + i}})

      batchReq.RequestItems[helpers.testHashTable] = items.map(function(item) { return {PutRequest: {Item: item}} })

      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        request(opts({
          TableName: helpers.testHashTable,
          ExpressionAttributeNames: {'#key': 'a'},
          ExpressionAttributeValues: {':value': {S: 'item12'}},
          FilterExpression: '#key = :value',
          Limit: 2,
        }), function(err, res) {
          if (err) return done(err)

          res.statusCode.should.equal(200)
          res.body.ScannedCount.should.equal(2)
          res.body.LastEvaluatedKey.a.S.should.not.be.empty // eslint-disable-line no-unused-expressions
          Object.keys(res.body.LastEvaluatedKey).should.have.length(1)
          helpers.clearTable(helpers.testHashTable, 'a', done)
        })
      })
    })

    it('should not return LastEvaluatedKey if Limit is large', function(done) {
      var i, b = {S: helpers.randomString()}, items = [], batchReq = {RequestItems: {}},
          scanFilter = {b: {ComparisonOperator: 'EQ', AttributeValueList: [b]}}

      for (i = 0; i < 5; i++)
        items.push({a: {S: String(i)}, b: b})

      batchReq.RequestItems[helpers.testHashTable] = items.map(function(item) { return {PutRequest: {Item: item}} })

      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        request(opts({TableName: helpers.testHashTable, AttributesToGet: ['a', 'b'], Limit: 100000}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Count.should.equal(res.body.ScannedCount)
          should.not.exist(res.body.LastEvaluatedKey)
          for (var i = 0, lastIx = 0; i < res.body.Count; i++) {
            if (res.body.Items[i].b.S == b.S) lastIx = i
          }
          var totalItems = res.body.Count
          request(opts({TableName: helpers.testHashTable, ScanFilter: scanFilter, Limit: lastIx}), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.Count.should.equal(4)
            res.body.LastEvaluatedKey.a.S.should.not.be.empty // eslint-disable-line no-unused-expressions
            request(opts({TableName: helpers.testHashTable, ScanFilter: scanFilter, Limit: lastIx + 1}), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)
              res.body.Count.should.equal(5)
              res.body.LastEvaluatedKey.a.S.should.not.be.empty // eslint-disable-line no-unused-expressions
              request(opts({TableName: helpers.testHashTable, ScanFilter: scanFilter, Limit: totalItems}), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)
                res.body.Count.should.equal(5)
                res.body.LastEvaluatedKey.a.S.should.not.be.empty  // eslint-disable-line no-unused-expressions
                request(opts({TableName: helpers.testHashTable, ScanFilter: scanFilter, Limit: totalItems + 1}), function(err, res) {
                  if (err) return done(err)
                  res.statusCode.should.equal(200)
                  res.body.Count.should.equal(5)
                  should.not.exist(res.body.LastEvaluatedKey)
                  done()
                })
              })
            })
          })
        })
      })
    })

    it('should return items in same segment order', function(done) {
      var i, b = {S: helpers.randomString()}, items = [],
          firstHalf, secondHalf, batchReq = {RequestItems: {}},
          scanFilter = {b: {ComparisonOperator: 'EQ', AttributeValueList: [b]}}

      for (i = 0; i < 20; i++)
        items.push({a: {S: String(i)}, b: b})

      batchReq.RequestItems[helpers.testHashTable] = items.map(function(item) { return {PutRequest: {Item: item}} })

      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        request(opts({TableName: helpers.testHashTable, Segment: 0, TotalSegments: 2, ScanFilter: scanFilter}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Count.should.be.above(0)

          firstHalf = res.body.Items

          request(opts({TableName: helpers.testHashTable, Segment: 1, TotalSegments: 2, ScanFilter: scanFilter}), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.Count.should.be.above(0)

            secondHalf = res.body.Items

            secondHalf.should.have.length(items.length - firstHalf.length)

            request(opts({TableName: helpers.testHashTable, Segment: 0, TotalSegments: 4, ScanFilter: scanFilter}), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)

              res.body.Items.forEach(function(item) { firstHalf.should.containEql(item) })

              request(opts({TableName: helpers.testHashTable, Segment: 1, TotalSegments: 4, ScanFilter: scanFilter}), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)

                res.body.Items.forEach(function(item) { firstHalf.should.containEql(item) })

                request(opts({TableName: helpers.testHashTable, Segment: 2, TotalSegments: 4, ScanFilter: scanFilter}), function(err, res) {
                  if (err) return done(err)
                  res.statusCode.should.equal(200)

                  res.body.Items.forEach(function(item) { secondHalf.should.containEql(item) })

                  request(opts({TableName: helpers.testHashTable, Segment: 3, TotalSegments: 4, ScanFilter: scanFilter}), function(err, res) {
                    if (err) return done(err)
                    res.statusCode.should.equal(200)

                    res.body.Items.forEach(function(item) { secondHalf.should.containEql(item) })

                    done()
                  })
                })
              })
            })
          })
        })
      })
    })

    // XXX: This is very brittle, relies on knowing the hashing scheme
    it('should return items in string hash order', function(done) {
      var i, b = {S: helpers.randomString()}, items = [],
          batchReq = {RequestItems: {}},
          scanFilter = {b: {ComparisonOperator: 'EQ', AttributeValueList: [b]}}

      for (i = 0; i < 10; i++)
        items.push({a: {S: String(i)}, b: b})

      items.push({a: {S: 'aardman'}, b: b})
      items.push({a: {S: 'hello'}, b: b})
      items.push({a: {S: 'zapf'}, b: b})
      items.push({a: {S: 'äáöü'}, b: b})

      batchReq.RequestItems[helpers.testHashTable] = items.map(function(item) { return {PutRequest: {Item: item}} })

      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        request(opts({TableName: helpers.testHashTable, ScanFilter: scanFilter}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Count.should.equal(14)
          var keys = res.body.Items.map(function(item) { return item.a.S })
          keys.should.eql(['2', '8', '9', '1', '6', 'hello', '0', '5', '4', 'äáöü', 'aardman', '7', '3', 'zapf'])
          done()
        })
      })
    })

    // XXX: This is very brittle, relies on knowing the hashing scheme
    it('should return items in number hash order', function(done) {
      var i, b = {S: helpers.randomString()}, items = [],
          batchReq = {RequestItems: {}},
          scanFilter = {b: {ComparisonOperator: 'EQ', AttributeValueList: [b]}}

      for (i = 0; i < 10; i++)
        items.push({a: {N: String(i)}, b: b})

      items.push({a: {N: '-0.09'}, b: b})
      items.push({a: {N: '999.9'}, b: b})
      items.push({a: {N: '0.012345'}, b: b})
      items.push({a: {N: '-999.9'}, b: b})

      batchReq.RequestItems[helpers.testHashNTable] = items.map(function(item) { return {PutRequest: {Item: item}} })

      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        request(opts({TableName: helpers.testHashNTable, ScanFilter: scanFilter}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Count.should.equal(14)
          var keys = res.body.Items.map(function(item) { return item.a.N })
          keys.should.eql(['7', '999.9', '8', '3', '2', '-999.9', '9', '4', '-0.09', '6', '1', '0', '0.012345', '5'])
          done()
        })
      })
    })

    // XXX: This is very brittle, relies on knowing the hashing scheme
    it('should return items from correct string hash segments', function(done) {
      var batchReq = {RequestItems: {}}, items = [
        {a: {S: '3635'}},
        {a: {S: '228'}},
        {a: {S: '1668'}},
        {a: {S: '3435'}},
      ]
      batchReq.RequestItems[helpers.testHashTable] = items.map(function(item) { return {PutRequest: {Item: item}} })

      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        request(opts({TableName: helpers.testHashTable, Segment: 0, TotalSegments: 4096}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Items.should.containEql(items[0])
          res.body.Items.should.containEql(items[1])
          request(opts({TableName: helpers.testHashTable, Segment: 1, TotalSegments: 4096}), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(items[2])
            request(opts({TableName: helpers.testHashTable, Segment: 4, TotalSegments: 4096}), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)
              res.body.Items.should.containEql(items[3])
              done()
            })
          })
        })
      })
    })

    // XXX: This is very brittle, relies on knowing the hashing scheme
    it('should return items from correct number hash segments', function(done) {
      var batchReq = {RequestItems: {}}, items = [
        {a: {N: '251'}},
        {a: {N: '2388'}},
      ]
      batchReq.RequestItems[helpers.testHashNTable] = items.map(function(item) { return {PutRequest: {Item: item}} })

      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        request(opts({TableName: helpers.testHashNTable, Segment: 1, TotalSegments: 4096}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Items.should.containEql(items[0])
          request(opts({TableName: helpers.testHashNTable, Segment: 4095, TotalSegments: 4096}), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.containEql(items[1])
            done()
          })
        })
      })
    })

    // TODO: Need high capacity to run this (~100 runs quickly)
    it.skip('should not return LastEvaluatedKey if just under limit for range table', function(done) {
      this.timeout(200000)

      var i, items = [], id = helpers.randomString(), e = new Array(41583).join('e'), eAttr = e.slice(0, 255)
      for (i = 0; i < 25; i++) {
        var item = {a: {S: id}, b: {S: ('000000' + i).slice(-6)}, c: {S: 'abcde'}}
        item[eAttr] = {S: e}
        items.push(item)
      }
      items[24][eAttr].S = new Array(41583).join('e')

      helpers.replaceTable(helpers.testRangeTable, ['a', 'b'], items, function(err) {
        if (err) return done(err)

        request(opts({
          TableName: helpers.testRangeTable,
          Select: 'COUNT',
          ReturnConsumedCapacity: 'INDEXES',
          Limit: 26,
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
    it.skip('should return LastEvaluatedKey if just over limit for range table', function(done) {
      this.timeout(200000)

      var i, items = [], id = helpers.randomString(), e = new Array(41597).join('e')
      for (i = 0; i < 25; i++)
        items.push({a: {S: id}, b: {S: ('00000' + i).slice(-5)}, c: {S: 'abcde'}, e: {S: e}})
      items[24].e.S = new Array(41598).join('e')

      helpers.replaceTable(helpers.testRangeTable, ['a', 'b'], items, function(err) {
        if (err) return done(err)

        request(opts({
          TableName: helpers.testRangeTable,
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
    it.skip('should not return LastEvaluatedKey if just under limit for number range table', function(done) {
      this.timeout(200000)

      var i, items = [], id = helpers.randomString(), e = new Array(41639).join('e'), eAttr = e.slice(0, 255)
      for (i = 0; i < 25; i++) {
        var item = {a: {S: id}, b: {N: ('00' + i).slice(-2)}, c: {S: 'abcde'}}
        item[eAttr] = {S: e}
        items.push(item)
      }
      items[24][eAttr].S = new Array(41653).join('e')

      helpers.replaceTable(helpers.testRangeNTable, ['a', 'b'], items, function(err) {
        if (err) return done(err)

        request(opts({
          TableName: helpers.testRangeNTable,
          Select: 'COUNT',
          ReturnConsumedCapacity: 'INDEXES',
          Limit: 26,
        }), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Count: 25,
            ScannedCount: 25,
            ConsumedCapacity: {
              CapacityUnits: 128,
              Table: {CapacityUnits: 128},
              TableName: helpers.testRangeNTable,
            },
          })
          helpers.clearTable(helpers.testRangeNTable, ['a', 'b'], done)
        })
      })
    })

    // TODO: Need high capacity to run this (~100 runs quickly)
    it.skip('should return LastEvaluatedKey if just over limit for number range table', function(done) {
      this.timeout(200000)

      var i, items = [], id = helpers.randomString(), e = new Array(41639).join('e')
      for (i = 0; i < 25; i++)
        items.push({a: {S: id}, b: {N: ('00' + i).slice(-2)}, c: {S: 'abcde'}, e: {S: e}})
      items[24].e.S = new Array(41654).join('e')

      helpers.replaceTable(helpers.testRangeNTable, ['a', 'b'], items, function(err) {
        if (err) return done(err)

        request(opts({
          TableName: helpers.testRangeNTable,
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
              TableName: helpers.testRangeNTable,
            },
            LastEvaluatedKey: {a: items[24].a, b: items[24].b},
          })
          helpers.clearTable(helpers.testRangeNTable, ['a', 'b'], done)
        })
      })
    })

    // TODO: Need high capacity to run this (~100 runs quickly)
    it.skip('should return all if just under limit with small attribute for hash table', function(done) {
      this.timeout(200000)

      var i, items = []
      for (i = 0; i < 25; i++)
        items.push({a: {S: ('0' + i).slice(-2)}})

      helpers.replaceTable(helpers.testHashTable, 'a', items, function(err) {
        if (err) return done(err)

        request(opts({TableName: helpers.testHashTable}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          items = res.body.Items
          items.should.have.length(25)

          var b = new Array(43412).join('b')

          for (i = 0; i < 25; i++) {
            if (i == 23) {
              // Second last item
              items[i].b = {S: b.slice(0, 43412 - 46)}
              items[i].c = {N: '12.3456'}
              items[i].d = {B: 'AQI='}
              items[i].e = {SS: ['a', 'bc']}
              items[i].f = {NS: ['1.23', '12.3']}
              items[i].g = {BS: ['AQI=', 'Ag==', 'AQ==']}
            } else if (i == 24) {
              items[i].b = {S: 'b'} // Last item doesn't matter
            } else {
              items[i].b = {S: b}
            }
          }

          helpers.replaceTable(helpers.testHashTable, 'a', items, 10, function(err) {
            if (err) return done(err)

            request(opts({TableName: helpers.testHashTable, Select: 'COUNT', ReturnConsumedCapacity: 'TOTAL'}), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)
              res.body.ScannedCount.should.equal(25)
              res.body.Count.should.equal(25)
              res.body.ConsumedCapacity.CapacityUnits.should.equal(127.5)
              helpers.clearTable(helpers.testHashTable, 'a', done)
            })
          })
        })
      })
    })

    // TODO: Need high capacity to run this (~100 runs quickly)
    it.skip('should return all if just under limit with large attribute', function(done) {
      this.timeout(200000)

      var i, items = []
      for (i = 0; i < 25; i++)
        items.push({a: {S: ('0' + i).slice(-2)}})

      helpers.replaceTable(helpers.testHashTable, 'a', items, function(err) {
        if (err) return done(err)

        request(opts({TableName: helpers.testHashTable}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          items = res.body.Items
          items.should.have.length(25)

          var b = new Array(43412).join('b'), bAttr = b.slice(0, 255)

          for (i = 0; i < 25; i++) {
            if (i == 23) {
              // Second last item
              items[i].bfasfdsfdsa = {S: b.slice(0, 43412 - 46)}
              items[i].cfadsfdsaafds = {N: '12.3456'}
              items[i].dfasdfdafdsa = {B: 'AQI='}
              items[i].efdasfdasfd = {SS: ['a', 'bc']}
              items[i].ffdsafsdfd = {NS: ['1.23', '12.3']}
              items[i].gfsdfdsaafds = {BS: ['AQI=', 'Ag==', 'AQ==']}
            } else if (i == 24) {
              items[i].b = {S: 'b'}
            } else {
              items[i][bAttr] = {S: b}
            }
          }

          helpers.replaceTable(helpers.testHashTable, 'a', items, 10, function(err) {
            if (err) return done(err)

            request(opts({TableName: helpers.testHashTable, Select: 'COUNT', ReturnConsumedCapacity: 'TOTAL'}), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)
              res.body.ScannedCount.should.equal(25)
              res.body.Count.should.equal(25)
              res.body.ConsumedCapacity.CapacityUnits.should.equal(128)
              helpers.clearTable(helpers.testHashTable, 'a', done)
            })
          })
        })
      })
    })

    // TODO: Need high capacity to run this (~100 runs quickly)
    it.skip('should return one less than all if just over limit with small attribute for hash table', function(done) {
      this.timeout(100000)

      var i, items = []
      for (i = 0; i < 25; i++)
        items.push({a: {S: ('0' + i).slice(-2)}})

      helpers.replaceTable(helpers.testHashTable, 'a', items, function(err) {
        if (err) return done(err)

        request(opts({TableName: helpers.testHashTable}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          items = res.body.Items
          items.should.have.length(25)

          var b = new Array(43412).join('b')

          for (i = 0; i < 25; i++) {
            if (i == 23) {
              // Second last item
              items[i].b = {S: b.slice(0, 43412 - 45)}
              items[i].c = {N: '12.3456'}
              items[i].d = {B: 'AQI='}
              items[i].e = {SS: ['a', 'bc']}
              items[i].f = {NS: ['1.23', '12.3']}
              items[i].g = {BS: ['AQI=', 'Ag==', 'AQ==']}
            } else if (i == 24) {
              items[i].b = {S: 'b'} // Last item doesn't matter
            } else {
              items[i].b = {S: b}
            }
          }

          helpers.replaceTable(helpers.testHashTable, 'a', items, 10, function(err) {
            if (err) return done(err)

            request(opts({TableName: helpers.testHashTable, Select: 'COUNT', ReturnConsumedCapacity: 'TOTAL'}), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)
              res.body.ScannedCount.should.equal(24)
              res.body.Count.should.equal(24)
              res.body.ConsumedCapacity.CapacityUnits.should.equal(127.5)
              helpers.clearTable(helpers.testHashTable, 'a', done)
            })
          })
        })
      })
    })

    // TODO: Need high capacity to run this (~100 runs quickly)
    it.skip('should return all if just under limit for range table', function(done) {
      this.timeout(200000)

      var i, items = []
      for (i = 0; i < 25; i++)
        items.push({a: {S: ('0' + i).slice(-2)}, b: {S: ('0' + i).slice(-2)}})

      helpers.replaceTable(helpers.testRangeTable, ['a', 'b'], items, function(err) {
        if (err) return done(err)

        request(opts({TableName: helpers.testRangeTable}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          items = res.body.Items
          items.should.have.length(25)

          var b = new Array(43381).join('b'), bAttr = b.slice(0, 255)

          for (i = 0; i < 25; i++) {
            if (i == 23) {
              // Second last item
              items[i].z = {S: b.slice(0, 43381 - 22)}
              items[i].y = {N: '12.3456'}
              items[i].x = {B: 'AQI='}
              items[i].w = {SS: ['a', 'bc']}
              items[i].v = {NS: ['1.23', '12.3']}
              items[i].u = {BS: ['AQI=', 'Ag==', 'AQ==']}
            } else if (i == 24) {
              items[i].z = {S: 'b'} // Last item doesn't matter
            } else {
              items[i][bAttr] = {S: b}
            }
          }

          helpers.replaceTable(helpers.testRangeTable, ['a', 'b'], items, 10, function(err) {
            if (err) return done(err)

            request(opts({TableName: helpers.testRangeTable, Select: 'COUNT', ReturnConsumedCapacity: 'TOTAL'}), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)
              res.body.ScannedCount.should.equal(25)
              res.body.Count.should.equal(25)
              res.body.ConsumedCapacity.CapacityUnits.should.equal(128)
              helpers.clearTable(helpers.testRangeTable, ['a', 'b'], done)
            })
          })
        })
      })
    })

    // TODO: Need high capacity to run this (~100 runs quickly)
    it.skip('should return all if just over limit with less items for range table', function(done) {
      this.timeout(200000)

      var i, items = []
      for (i = 0; i < 13; i++)
        items.push({a: {S: ('0' + i).slice(-2)}, b: {S: ('0000000' + i).slice(-7)}})

      helpers.replaceTable(helpers.testRangeTable, ['a', 'b'], items, function(err) {
        if (err) return done(err)

        request(opts({TableName: helpers.testRangeTable}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          items = res.body.Items
          items.should.have.length(13)

          var b = new Array(86648).join('b')

          for (i = 0; i < 13; i++) {
            if (i == 11) {
              // Second last item
              items[i].z = {S: b.slice(0, 86648 - 9)}
            } else if (i == 12) {
              items[i].z = {S: 'b'} // Last item doesn't matter, 127.5 capacity units
              // items[i][bAttr] = {S: b} // 138 capacity units
            } else {
              items[i].z = {S: b}
            }
          }

          helpers.replaceTable(helpers.testRangeTable, ['a', 'b'], items, 10, function(err) {
            if (err) return done(err)

            request(opts({TableName: helpers.testRangeTable, Select: 'COUNT', ReturnConsumedCapacity: 'TOTAL'}), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)
              res.body.ScannedCount.should.equal(12)
              res.body.Count.should.equal(12)
              res.body.ConsumedCapacity.CapacityUnits.should.equal(127)
              helpers.clearTable(helpers.testRangeTable, ['a', 'b'], done)
            })
          })
        })
      })
    })

    // TODO: Need high capacity to run this (~100 runs quickly)
    it.skip('should return all if just over limit for range table', function(done) {
      this.timeout(200000)

      var i, items = []
      for (i = 0; i < 25; i++)
        items.push({a: {S: ('0' + i).slice(-2)}, b: {S: ('0' + i).slice(-2)}})

      helpers.replaceTable(helpers.testRangeTable, ['a', 'b'], items, function(err) {
        if (err) return done(err)

        request(opts({TableName: helpers.testRangeTable}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          items = res.body.Items
          items.should.have.length(25)

          var b = new Array(43381).join('b')

          for (i = 0; i < 25; i++) {
            if (i == 23) {
              // Second last item
              items[i].z = {S: b.slice(0, 43381 - 21)}
              items[i].y = {N: '12.3456'}
              items[i].x = {B: 'AQI='}
              items[i].w = {SS: ['a', 'bc']}
              items[i].v = {NS: ['1.23', '12.3']}
              items[i].u = {BS: ['AQI=', 'Ag==', 'AQ==']}
            } else if (i == 24) {
              items[i].z = {S: 'b'} // Last item doesn't matter
            } else {
              items[i].z = {S: b}
            }
          }

          helpers.replaceTable(helpers.testRangeTable, ['a', 'b'], items, 10, function(err) {
            if (err) return done(err)

            request(opts({TableName: helpers.testRangeTable, Select: 'COUNT', ReturnConsumedCapacity: 'TOTAL'}), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)
              res.body.ScannedCount.should.equal(24)
              res.body.Count.should.equal(24)
              res.body.ConsumedCapacity.CapacityUnits.should.equal(127.5)
              helpers.clearTable(helpers.testRangeTable, ['a', 'b'], done)
            })
          })
        })
      })
    })

    // TODO: Need high capacity to run this (~100 runs quickly)
    it.skip('should return one less than all if just over limit with large attribute', function(done) {
      this.timeout(100000)

      var i, items = []
      for (i = 0; i < 25; i++)
        items.push({a: {S: ('0' + i).slice(-2)}})

      helpers.replaceTable(helpers.testHashTable, 'a', items, function(err) {
        if (err) return done(err)

        request(opts({TableName: helpers.testHashTable}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          items = res.body.Items
          items.should.have.length(25)

          var b = new Array(43412).join('b'), bAttr = b.slice(0, 255)

          for (i = 0; i < 25; i++) {
            if (i == 23) {
              // Second last item
              items[i].bfasfdsfdsa = {S: b.slice(0, 43412 - 45)}
              items[i].cfadsfdsaafds = {N: '12.3456'}
              items[i].dfasdfdafdsa = {B: 'AQI='}
              items[i].efdasfdasfd = {SS: ['a', 'bc']}
              items[i].ffdsafsdfd = {NS: ['1.23', '12.3']}
              items[i].gfsdfdsaafds = {BS: ['AQI=', 'Ag==', 'AQ==']}
            } else if (i == 24) {
              items[i][bAttr] = {S: new Array(100).join('b')} // Last item doesn't matter
            } else {
              items[i][bAttr] = {S: b}
            }
          }

          helpers.replaceTable(helpers.testHashTable, 'a', items, 10, function(err) {
            if (err) return done(err)

            request(opts({TableName: helpers.testHashTable, Select: 'COUNT', ReturnConsumedCapacity: 'TOTAL'}), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)
              res.body.ScannedCount.should.equal(24)
              res.body.Count.should.equal(24)
              res.body.ConsumedCapacity.CapacityUnits.should.equal(128)
              helpers.clearTable(helpers.testHashTable, 'a', done)
            })
          })
        })
      })
    })

    // Upper bound seems to vary – tends to return a 500 above 30000 args
    it('should allow scans at least for 27500 args to IN', function(done) {
      this.timeout(100000)
      var attrValList = [], i
      for (i = 0; i < 27500; i++) attrValList.push({S: 'a'})
      request(opts({TableName: helpers.testHashTable, ScanFilter: {
        a: {ComparisonOperator: 'IN', AttributeValueList: attrValList},
      }}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        done()
      })
    })

  })

})
