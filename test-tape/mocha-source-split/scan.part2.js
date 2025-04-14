var helpers = require('./helpers'),
  should = require('should'),
  async = require('async')

var target = 'Scan',
  request = helpers.request,
  opts = helpers.opts.bind(null, target),
  assertType = helpers.assertType.bind(null, target),
  assertValidation = helpers.assertValidation.bind(null, target),
  assertNotFound = helpers.assertNotFound.bind(null, target),
  runSlowTests = helpers.runSlowTests

describe('scan', function () {
  describe('validations', function () {

    it('should return ValidationException for no TableName', function (done) {
      assertValidation({},
        '1 validation error detected: ' +
        'Value null at \'tableName\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for empty TableName', function (done) {
      assertValidation({ TableName: '' }, [
        'Value \'\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        'Value \'\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 3',
      ], done)
    })

    it('should return ValidationException for short TableName', function (done) {
      assertValidation({ TableName: 'a;' }, [
        'Value \'a;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        'Value \'a;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 3',
      ], done)
    })

    it('should return ValidationException for long TableName', function (done) {
      var name = new Array(256 + 1).join('a')
      assertValidation({ TableName: name },
        '1 validation error detected: ' +
        'Value \'' + name + '\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 255', done)
    })

    it('should return ValidationException for incorrect attributes', function (done) {
      assertValidation({ TableName: 'abc;', ReturnConsumedCapacity: 'hi', AttributesToGet: [],
        IndexName: 'abc;', Segment: -1, TotalSegments: -1, Select: 'hi', Limit: -1, ScanFilter: { a: {}, b: { ComparisonOperator: '' } },
        ConditionalOperator: 'AN', ExpressionAttributeNames: {}, ExpressionAttributeValues: {}, ProjectionExpression: '' }, [
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

    it('should return ValidationException if expression and non-expression', function (done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: { a: { ComparisonOperator: 'EQ', AttributeValueList: [ {} ] } },
        Segment: 1,
        Limit: 1,
        AttributesToGet: [ 'a', 'a' ],
        ExclusiveStartKey: { a: {} },
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

    it('should return ValidationException if ExpressionAttributeNames but no FilterExpression', function (done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: { a: { ComparisonOperator: 'EQ', AttributeValueList: [ {} ] } },
        Segment: 1,
        Limit: 1,
        AttributesToGet: [ 'a', 'a' ],
        ExclusiveStartKey: { a: {} },
        ConditionalOperator: 'OR',
        Select: 'SPECIFIC_ATTRIBUTES',
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
      }, 'ExpressionAttributeNames can only be specified when using expressions', done)
    })

    it('should return ValidationException if ExpressionAttributeValues but no FilterExpression', function (done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: { a: { ComparisonOperator: 'EQ', AttributeValueList: [ {} ] } },
        Segment: 1,
        Limit: 1,
        AttributesToGet: [ 'a', 'a' ],
        ExclusiveStartKey: { a: {} },
        ConditionalOperator: 'OR',
        Select: 'SPECIFIC_ATTRIBUTES',
        ExpressionAttributeValues: {},
      }, 'ExpressionAttributeValues can only be specified when using expressions: FilterExpression is null', done)
    })

    it('should return ValidationException for duplicate values in AttributesToGet', function (done) {
      assertValidation({
        TableName: 'abc',
        Segment: 1,
        ScanFilter: { a: { ComparisonOperator: 'EQ', AttributeValueList: [ {}, { a: '' }, { S: '' } ] } },
        ExclusiveStartKey: { a: {} },
        AttributesToGet: [ 'a', 'a' ],
      }, 'One or more parameter values were invalid: Duplicate value in attribute name: a', done)
    })

    it('should return ValidationException for bad attribute values in ScanFilter', function (done) {
      async.forEach([
        {},
        { a: '' },
      ], function (expr, cb) {
        assertValidation({
          TableName: 'abc',
          Segment: 1,
          ExclusiveStartKey: { a: {} },
          ScanFilter: { a: { ComparisonOperator: 'EQ', AttributeValueList: [ expr, { S: '' } ] } },
        }, 'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', cb)
      }, done)
    })

    it('should return ValidationException for invalid values in ScanFilter', function (done) {
      async.forEach([
        [ { NULL: 'no' }, 'Null attribute value types must have the value of true' ],
        [ { SS: [] }, 'An string set  may not be empty' ],
        [ { NS: [] }, 'An number set  may not be empty' ],
        [ { BS: [] }, 'Binary sets should not be empty' ],
        [ { SS: [ 'a', 'a' ] }, 'Input collection [a, a] contains duplicates.' ],
        [ { BS: [ 'Yg==', 'Yg==' ] }, 'Input collection [Yg==, Yg==]of type BS contains duplicates.' ],
      ], function (expr, cb) {
        assertValidation({
          TableName: 'abc',
          Segment: 1,
          ExclusiveStartKey: { a: {} },
          ScanFilter: { a: { ComparisonOperator: 'EQ', AttributeValueList: [ { N: '1' }, expr[0], {} ] } },
        }, 'One or more parameter values were invalid: ' + expr[1], cb)
      }, done)
    })

    it('should return ValidationException for empty/invalid numbers in ScanFilter', function (done) {
      async.forEach([
        [ { S: '', N: '' }, 'The parameter cannot be converted to a numeric value' ],
        [ { S: 'a', N: '' }, 'The parameter cannot be converted to a numeric value' ],
        [ { S: 'a', N: 'b' }, 'The parameter cannot be converted to a numeric value: b' ],
        [ { NS: [ '1', '' ] }, 'The parameter cannot be converted to a numeric value' ],
        [ { NS: [ '1', 'b' ] }, 'The parameter cannot be converted to a numeric value: b' ],
        [ { NS: [ '1', '1' ] }, 'Input collection contains duplicates' ],
        [ { N: '123456789012345678901234567890123456789' }, 'Attempting to store more than 38 significant digits in a Number' ],
        [ { N: '-1.23456789012345678901234567890123456789' }, 'Attempting to store more than 38 significant digits in a Number' ],
        [ { N: '1e126' }, 'Number overflow. Attempting to store a number with magnitude larger than supported range' ],
        [ { N: '-1e126' }, 'Number overflow. Attempting to store a number with magnitude larger than supported range' ],
        [ { N: '1e-131' }, 'Number underflow. Attempting to store a number with magnitude smaller than supported range' ],
        [ { N: '-1e-131' }, 'Number underflow. Attempting to store a number with magnitude smaller than supported range' ],
      ], function (expr, cb) {
        assertValidation({
          TableName: 'abc',
          Segment: 1,
          ExclusiveStartKey: { a: {} },
          ScanFilter: { a: { ComparisonOperator: 'EQ', AttributeValueList: [ { N: '1' }, expr[0] ] } },
        }, expr[1], cb)
      }, done)
    })

    it('should return ValidationException for multiple datatypes in ScanFilter', function (done) {
      assertValidation({
        TableName: 'abc',
        Segment: 1,
        ExclusiveStartKey: { a: {} },
        ScanFilter: { a: { ComparisonOperator: 'EQ', AttributeValueList: [ { N: '1' }, { S: 'a', N: '1' } ] } },
      }, 'Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException for incorrect number of ScanFilter arguments', function (done) {
      async.forEach([
        { a: { ComparisonOperator: 'EQ' }, b: { ComparisonOperator: 'NULL' }, c: { ComparisonOperator: 'NULL' } },
        { a: { ComparisonOperator: 'EQ' } },
        { a: { ComparisonOperator: 'EQ', AttributeValueList: [] } },
        { a: { ComparisonOperator: 'NE' } },
        { a: { ComparisonOperator: 'LE' } },
        { a: { ComparisonOperator: 'LT' } },
        { a: { ComparisonOperator: 'GE' } },
        { a: { ComparisonOperator: 'GT' } },
        { a: { ComparisonOperator: 'CONTAINS' } },
        { a: { ComparisonOperator: 'NOT_CONTAINS' } },
        { a: { ComparisonOperator: 'BEGINS_WITH' } },
        { a: { ComparisonOperator: 'IN' } },
        { a: { ComparisonOperator: 'BETWEEN' } },
        { a: { ComparisonOperator: 'NULL', AttributeValueList: [ { S: 'a' } ] } },
        { a: { ComparisonOperator: 'NOT_NULL', AttributeValueList: [ { S: 'a' } ] } },
        { a: { ComparisonOperator: 'EQ', AttributeValueList: [ { S: 'a' }, { S: 'a' } ] } },
        { a: { ComparisonOperator: 'NE', AttributeValueList: [ { S: 'a' }, { S: 'a' } ] } },
        { a: { ComparisonOperator: 'LE', AttributeValueList: [ { S: 'a' }, { S: 'a' } ] } },
        { a: { ComparisonOperator: 'LT', AttributeValueList: [ { S: 'a' }, { S: 'a' } ] } },
        { a: { ComparisonOperator: 'GE', AttributeValueList: [ { S: 'a' }, { S: 'a' } ] } },
        { a: { ComparisonOperator: 'GT', AttributeValueList: [ { S: 'a' }, { S: 'a' } ] } },
        { a: { ComparisonOperator: 'CONTAINS', AttributeValueList: [ { S: 'a' }, { S: 'a' } ] } },
        { a: { ComparisonOperator: 'NOT_CONTAINS', AttributeValueList: [ { S: 'a' }, { S: 'a' } ] } },
        { a: { ComparisonOperator: 'NULL', AttributeValueList: [ { S: 'a' }, { S: 'a' } ] } },
        { a: { ComparisonOperator: 'NOT_NULL', AttributeValueList: [ { S: 'a' }, { S: 'a' } ] } },
        { a: { ComparisonOperator: 'BETWEEN', AttributeValueList: [ { S: 'a' }, { S: 'a' }, { S: 'a' } ] } },
      ], function (expr, cb) {
        assertValidation({
          TableName: 'abc',
          Segment: 1,
          ExclusiveStartKey: { a: {} },
          ScanFilter: expr,
        }, 'One or more parameter values were invalid: Invalid number of argument(s) for the ' +
          expr.a.ComparisonOperator + ' ComparisonOperator', cb)
      }, done)
    })

    it('should return ValidationException for invalid ComparisonOperator types', function (done) {
      async.forEach([
        'LT',
        'LE',
        'GT',
        'GE',
        'IN',
      ], function (cond, cb) {
        async.forEach([
          [ { BOOL: true } ],
          [ { NULL: true } ],
          [ { SS: [ 'a' ] } ],
          [ { NS: [ '1' ] } ],
          [ { BS: [ 'abcd' ] } ],
          [ { M: {} } ],
          [ { L: [] } ],
        ], function (list, cb) {
          assertValidation({
            TableName: 'abc',
            Segment: 1,
            ExclusiveStartKey: { a: {} },
            ScanFilter: { a: { ComparisonOperator: cond, AttributeValueList: list } },
          }, 'One or more parameter values were invalid: ' +
            'ComparisonOperator ' + cond + ' is not valid for ' +
            Object.keys(list[0])[0] + ' AttributeValue type', cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException for invalid CONTAINS ComparisonOperator types', function (done) {
      async.forEach([
        'CONTAINS',
        'NOT_CONTAINS',
      ], function (cond, cb) {
        async.forEach([
          [ { SS: [ 'a' ] } ],
          [ { NS: [ '1' ] } ],
          [ { BS: [ 'abcd' ] } ],
          [ { M: {} } ],
          [ { L: [] } ],
        ], function (list, cb) {
          assertValidation({
            TableName: 'abc',
            Segment: 1,
            ExclusiveStartKey: { a: {} },
            ScanFilter: { a: { ComparisonOperator: cond, AttributeValueList: list } },
          }, 'One or more parameter values were invalid: ' +
            'ComparisonOperator ' + cond + ' is not valid for ' +
            Object.keys(list[0])[0] + ' AttributeValue type', cb)
        }, cb)
      }, done)
    })

    it('should return ValidationException for invalid BETWEEN ComparisonOperator types', function (done) {
      async.forEach([
        [ { BOOL: true }, { BOOL: true } ],
        [ { NULL: true }, { NULL: true } ],
        [ { SS: [ 'a' ] }, { SS: [ 'a' ] } ],
        [ { NS: [ '1' ] }, { NS: [ '1' ] } ],
        [ { BS: [ 'abcd' ] }, { BS: [ 'abcd' ] } ],
        [ { M: {} }, { M: {} } ],
        [ { L: [] }, { L: [] } ],
      ], function (list, cb) {
        assertValidation({
          TableName: 'abc',
          Segment: 1,
          ExclusiveStartKey: { a: {} },
          ScanFilter: { a: { ComparisonOperator: 'BETWEEN', AttributeValueList: list } },
        }, 'One or more parameter values were invalid: ' +
          'ComparisonOperator BETWEEN is not valid for ' +
          Object.keys(list[0])[0] + ' AttributeValue type', cb)
      }, done)
    })

    it('should return ValidationException for invalid BEGINS_WITH ComparisonOperator types', function (done) {
      async.forEach([
        [ { N: '1' } ],
        // [{B: 'YQ=='}], // B is fine
        [ { BOOL: true } ],
        [ { NULL: true } ],
        [ { SS: [ 'a' ] } ],
        [ { NS: [ '1' ] } ],
        [ { BS: [ 'abcd' ] } ],
        [ { M: {} } ],
        [ { L: [] } ],
      ], function (list, cb) {
        assertValidation({
          TableName: 'abc',
          Segment: 1,
          ExclusiveStartKey: { a: {} },
          ScanFilter: { a: { ComparisonOperator: 'BEGINS_WITH', AttributeValueList: list } },
        }, 'One or more parameter values were invalid: ' +
          'ComparisonOperator BEGINS_WITH is not valid for ' +
          Object.keys(list[0])[0] + ' AttributeValue type', cb)
      }, done)
    })

    it('should return ValidationException on ExclusiveStartKey if ScanFilter ok with EQ on type SS when table does not exist', function (done) {
      assertValidation({
        TableName: 'abc',
        Segment: 1,
        ExclusiveStartKey: { a: {} },
        ScanFilter: { a: { ComparisonOperator: 'EQ', AttributeValueList: [ { SS: [ 'a' ] } ] } },
      }, 'The provided starting key is invalid: Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException for unsupported datatype in ExclusiveStartKey', function (done) {
      async.forEach([
        {},
        { a: '' },
        { M: { a: {} } },
        { L: [ {} ] },
        { L: [ { a: {} } ] },
      ], function (expr, cb) {
        assertValidation({
          TableName: 'abc',
          Segment: 1,
          FilterExpression: '',
          ExpressionAttributeNames: {},
          ExpressionAttributeValues: {},
          ExclusiveStartKey: { a: expr },
        }, 'The provided starting key is invalid: ' +
          'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', cb)
      }, done)
    })

    it('should return ValidationException for invalid values in ExclusiveStartKey', function (done) {
      async.forEach([
        [ { NULL: 'no' }, 'Null attribute value types must have the value of true' ],
        [ { SS: [] }, 'An string set  may not be empty' ],
        [ { BS: [] }, 'Binary sets should not be empty' ],
      ], function (expr, cb) {
        assertValidation({
          TableName: 'abc',
          Segment: 1,
          FilterExpression: '',
          ExpressionAttributeNames: {},
          ExpressionAttributeValues: {},
          ExclusiveStartKey: { a: expr[0] },
        }, 'The provided starting key is invalid: ' +
          'One or more parameter values were invalid: ' + expr[1], cb)
      }, done)
    })

    it('should return ValidationException for invalid values in ExclusiveStartKey with no provided message', function (done) {
      async.forEach([
        [ { NS: [] }, 'An number set  may not be empty' ],
        [ { SS: [ 'a', 'a' ] }, 'Input collection [a, a] contains duplicates.' ],
        [ { BS: [ 'Yg==', 'Yg==' ] }, 'Input collection [Yg==, Yg==]of type BS contains duplicates.' ],
      ], function (expr, cb) {
        assertValidation({
          TableName: 'abc',
          Segment: 1,
          FilterExpression: '',
          ExpressionAttributeNames: {},
          ExpressionAttributeValues: {},
          ExclusiveStartKey: { a: expr[0] },
        }, 'One or more parameter values were invalid: ' + expr[1], cb)
      }, done)
    })

    it('should return ValidationException for empty/invalid numbers in ExclusiveStartKey', function (done) {
      async.forEach([
        [ { S: '', N: '' }, 'The parameter cannot be converted to a numeric value' ],
        [ { S: 'a', N: '' }, 'The parameter cannot be converted to a numeric value' ],
        [ { S: 'a', N: 'b' }, 'The parameter cannot be converted to a numeric value: b' ],
        [ { NS: [ '1', '' ] }, 'The parameter cannot be converted to a numeric value' ],
        [ { NS: [ '1', 'b' ] }, 'The parameter cannot be converted to a numeric value: b' ],
        [ { NS: [ '1', '1' ] }, 'Input collection contains duplicates' ],
        [ { N: '123456789012345678901234567890123456789' }, 'Attempting to store more than 38 significant digits in a Number' ],
        [ { N: '-1.23456789012345678901234567890123456789' }, 'Attempting to store more than 38 significant digits in a Number' ],
        [ { N: '1e126' }, 'Number overflow. Attempting to store a number with magnitude larger than supported range' ],
        [ { N: '-1e126' }, 'Number overflow. Attempting to store a number with magnitude larger than supported range' ],
        [ { N: '1e-131' }, 'Number underflow. Attempting to store a number with magnitude smaller than supported range' ],
        [ { N: '-1e-131' }, 'Number underflow. Attempting to store a number with magnitude smaller than supported range' ],
      ], function (expr, cb) {
        assertValidation({
          TableName: 'abc',
          Segment: 1,
          FilterExpression: '',
          ExpressionAttributeNames: {},
          ExpressionAttributeValues: {},
          ExclusiveStartKey: { a: expr[0] },
        }, expr[1], cb)
      }, done)
    })

    it('should return ValidationException for multiple datatypes in ExclusiveStartKey', function (done) {
      assertValidation({
        TableName: 'abc',
        TotalSegments: 1,
        FilterExpression: '',
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
        ExclusiveStartKey: { a: { S: 'a', N: '1' } },
      }, 'The provided starting key is invalid: ' +
        'Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException for missing TotalSegments', function (done) {
      assertValidation({
        TableName: 'abc',
        Segment: 1,
        FilterExpression: '',
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
      }, 'The TotalSegments parameter is required but was not present in the request when Segment parameter is present', done)
    })

    it('should return ValidationException for missing Segment', function (done) {
      assertValidation({
        TableName: 'abc',
        TotalSegments: 1,
        FilterExpression: '',
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
      }, 'The Segment parameter is required but was not present in the request when parameter TotalSegments is present', done)
    })

    it('should return ValidationException for Segment more than TotalSegments', function (done) {
      assertValidation({
        TableName: 'abc',
        Segment: 1,
        TotalSegments: 1,
        FilterExpression: '',
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
      }, 'The Segment parameter is zero-based and must be less than parameter TotalSegments: Segment: 1 is not less than TotalSegments: 1', done)
    })

    it('should return ValidationException for empty ExpressionAttributeNames', function (done) {
      assertValidation({
        TableName: 'abc',
        FilterExpression: '',
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
      }, 'ExpressionAttributeNames must not be empty', done)
    })

    it('should return ValidationException for invalid ExpressionAttributeNames', function (done) {
      assertValidation({
        TableName: 'abc',
        FilterExpression: '',
        ExpressionAttributeNames: { 'a': 'a' },
        ExpressionAttributeValues: {},
      }, 'ExpressionAttributeNames contains invalid key: Syntax error; key: "a"', done)
    })

    it('should return ValidationException for empty ExpressionAttributeValues', function (done) {
      assertValidation({
        TableName: 'abc',
        FilterExpression: '',
        ExpressionAttributeValues: {},
      }, 'ExpressionAttributeValues must not be empty', done)
    })

    it('should return ValidationException for invalid ExpressionAttributeValues', function (done) {
      assertValidation({
        TableName: 'abc',
        FilterExpression: '',
        ExpressionAttributeValues: { 'a': { S: 'b' } },
      }, 'ExpressionAttributeValues contains invalid key: Syntax error; key: "a"', done)
    })

    it('should return ValidationException for empty FilterExpression', function (done) {
      assertValidation({
        TableName: 'abc',
        FilterExpression: '',
        ProjectionExpression: '',
        ExpressionAttributeValues: { ':0': { S: 'b' } },
      }, 'Invalid FilterExpression: The expression can not be empty;', done)
    })

    it('should return ValidationException for empty ProjectionExpression', function (done) {
      assertValidation({
        TableName: 'abc',
        FilterExpression: 'a > b',
        ProjectionExpression: '',
      }, 'Invalid ProjectionExpression: The expression can not be empty;', done)
    })

    it('should return ValidationException for syntax errors', function (done) {
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
      async.forEach(expressions, function (expression, cb) {
        assertValidation({
          TableName: 'abc',
          FilterExpression: expression,
        }, /^Invalid FilterExpression: Syntax error; /, cb)
      }, done)
    })

    it('should return ValidationException for redundant parentheses', function (done) {
      var expressions = [
        'a=a and a > ((views))',
        '(a)between(((b.c)).d)and(c)',
        'a > whatever((:things), ((a)))',
        'a=a AND ((a=a AND a=a)) AND a=a',
        'a=a OR ((a=a OR a=a)) OR a=a',
        'a=a AND ((a=a AND (a=a AND a=a)))',
        'a=a OR ((a=a OR (a=a OR a=a)))',
      ]
      async.forEach(expressions, function (expression, cb) {
        assertValidation({
          TableName: 'abc',
          FilterExpression: expression,
        }, 'Invalid FilterExpression: The expression has redundant parentheses;', cb)
      }, done)
    })

    it('should return ValidationException for invalid function names', function (done) {
      var expressions = [
        [ 'a=a and whatever((:things)) > a', 'whatever' ],
        [ 'attRIbute_exIsts((views), #a)', 'attRIbute_exIsts' ],
      ]
      async.forEach(expressions, function (expr, cb) {
        assertValidation({
          TableName: 'abc',
          FilterExpression: expr[0],
        }, 'Invalid FilterExpression: Invalid function name; function: ' + expr[1], cb)
      }, done)
    })

    it('should return ValidationException for functions used incorrectly', function (done) {
      var expressions = [
        [ 'a=a and attribute_exists((views), (#a)) > b', 'attribute_exists' ],
        [ 'attribute_not_exists(things) > b', 'attribute_not_exists' ],
        [ 'attribute_type(things, :a) > b', 'attribute_type' ],
        [ 'begins_with(things, a) > b', 'begins_with' ],
        [ 'contains(:things, c) > b', 'contains' ],
        [ 'size(contains(a, b)) > a', 'contains' ],
        [ 'size(things)', 'size' ],
        [ 'a between b and attribute_exists(things)', 'attribute_exists' ],
        [ 'a in (b, attribute_exists(things))', 'attribute_exists' ],
      ]
      async.forEach(expressions, function (expr, cb) {
        assertValidation({
          TableName: 'abc',
          FilterExpression: expr[0],
        }, 'Invalid FilterExpression: The function is not allowed to be used this way in an expression; function: ' + expr[1], cb)
      }, done)
    })

    it('should return ValidationException for reserved keywords', function (done) {
      var expressions = [
        [ 'attribute_exists(views, #a)', 'views' ],
        [ ':a < abOrT', 'abOrT' ],
      ]
      async.forEach(expressions, function (expr, cb) {
        assertValidation({
          TableName: 'abc',
          FilterExpression: expr[0],
        }, 'Invalid FilterExpression: Attribute name is a reserved keyword; reserved keyword: ' + expr[1], cb)
      }, done)
    })

    // All checks below here are done on a per-expression basis

    it('should return ValidationException for missing attribute names', function (done) {
      var expressions = [
        [ 'attribute_exists(#Pictures.RearView, :a) and a=a', '#Pictures' ],
        [ 'begins_with(Pictures.#RearView)', '#RearView' ],
        [ '(#P between :lo and :hi) and (#PC in (:cat1, :cat2))', '#P' ],
        [ '#4g > a', '#4g' ],
        [ '#_ > a', '#_' ],
        [ '(a)between(b.c[45].#d)and(:a)', '#d' ],
      ]
      async.forEach(expressions, function (expr, cb) {
        assertValidation({
          TableName: 'abc',
          FilterExpression: expr[0],
        }, 'Invalid FilterExpression: An expression attribute name used in the document path is not defined; attribute name: ' + expr[1], cb)
      }, done)
    })

    it('should return ValidationException for missing attribute values', function (done) {
      var expressions = [
        [ 'begins_with(:hello, #a, #b)', ':hello' ],
        [ ':a < :b', ':a' ],
        [ ':_ > a', ':_' ],
      ]
      async.forEach(expressions, function (expr, cb) {
        assertValidation({
          TableName: 'abc',
          FilterExpression: expr[0],
        }, 'Invalid FilterExpression: An expression attribute value used in expression is not defined; attribute value: ' + expr[1], cb)
      }, done)
    })

    it('should return ValidationException for functions with incorrect operands', function (done) {
      var expressions = [
        [ 'attribute_exists(things, a) and a=a', 'attribute_exists', 2 ],
        [ 'attribute_not_exists(things, b)', 'attribute_not_exists', 2 ],
        [ 'attribute_type(things)', 'attribute_type', 1 ],
        [ 'begins_with(things)', 'begins_with', 1 ],
        [ 'begins_with(things, size(a), b)', 'begins_with', 3 ],
        [ 'contains(things)', 'contains', 1 ],
        [ 'size(things, a) > b', 'size', 2 ],
      ]
      async.forEach(expressions, function (expr, cb) {
        assertValidation({
          TableName: 'abc',
          FilterExpression: expr[0],
        }, 'Invalid FilterExpression: Incorrect number of operands for operator or function; operator or function: ' +
          expr[1] + ', number of operands: ' + expr[2], cb)
      }, done)
    })

    it('should return ValidationException for functions with incorrect operand type', function (done) {
      var expressions = [
        // Order of the {...} args is non-deterministic
        // ['attribute_type(ab.bc[1].a, SS)', 'attribute_type', '{NS,SS,L,BS,N,M,B,BOOL,NULL,S}'],
        [ 'attribute_type(a, size(a)) and a=a and a=:a', 'attribute_type', { 'N': '1' } ],
        [ 'attribute_type(a, :a)', 'attribute_type', { 'N': '1' } ],
        [ 'attribute_type(a, :a)', 'attribute_type', { 'B': 'YQ==' } ],
        [ 'attribute_type(a, :a)', 'attribute_type', { 'BOOL': true } ],
        [ 'attribute_type(a, :a)', 'attribute_type', { 'NULL': true } ],
        [ 'attribute_type(a, :a)', 'attribute_type', { 'L': [] } ],
        [ 'attribute_type(a, :a)', 'attribute_type', { 'M': {} } ],
        [ 'attribute_type(a, :a)', 'attribute_type', { 'SS': [ '1' ] } ],
        [ 'attribute_type(a, :a)', 'attribute_type', { 'NS': [ '1' ] } ],
        [ 'attribute_type(a, :a)', 'attribute_type', { 'BS': [ 'YQ==' ] } ],
        [ 'begins_with(a, size(a)) and a=:a', 'begins_with', { 'N': '1' } ],
        [ 'begins_with(a, :a)', 'begins_with', { 'N': '1' } ],
        [ 'begins_with(a, :a)', 'begins_with', { 'BOOL': true } ],
        [ 'begins_with(a, :a)', 'begins_with', { 'NULL': true } ],
        [ 'begins_with(a, :a)', 'begins_with', { 'L': [] } ],
        [ 'begins_with(a, :a)', 'begins_with', { 'M': {} } ],
        [ 'begins_with(a, :a)', 'begins_with', { 'SS': [ '1' ] } ],
        [ 'begins_with(a, :a)', 'begins_with', { 'NS': [ '1' ] } ],
        [ 'begins_with(a, :a)', 'begins_with', { 'BS': [ 'YQ==' ] } ],
        [ 'begins_with(:a, a)', 'begins_with', { 'N': '1' } ],
        [ 'begins_with(:a, a)', 'begins_with', { 'BOOL': true } ],
        [ 'begins_with(:a, a)', 'begins_with', { 'NULL': true } ],
        [ 'begins_with(:a, a)', 'begins_with', { 'L': [] } ],
        [ 'begins_with(:a, a)', 'begins_with', { 'M': {} } ],
        [ 'begins_with(:a, a)', 'begins_with', { 'SS': [ '1' ] } ],
        [ 'begins_with(:a, a)', 'begins_with', { 'NS': [ '1' ] } ],
        [ 'begins_with(:a, a)', 'begins_with', { 'BS': [ 'YQ==' ] } ],
        [ 'size(size(a)) > :a', 'size', { 'N': '1' } ],
        [ 'attribute_not_exists(size(:a))', 'size', { 'N': '1' } ],
        [ 'size(:a) > a', 'size', { 'BOOL': true } ],
        [ 'size(:a) > a', 'size', { 'NULL': true } ],
      ]
      async.forEach(expressions, function (expr, cb) {
        assertValidation({
          TableName: 'abc',
          FilterExpression: expr[0],
          ExpressionAttributeValues: { ':a': expr[2] },
        }, 'Invalid FilterExpression: Incorrect operand type for operator or function; operator or function: ' +
          expr[1] + ', operand type: ' + Object.keys(expr[2])[0], cb)
      }, done)
    })

    it('should return ValidationException for attribute_type with incorrect value', function (done) {
      assertValidation({
        TableName: 'abc',
        FilterExpression: 'attribute_type(a, :a)',
        ExpressionAttributeValues: { ':a': { 'S': '1' } },
      }, /^Invalid FilterExpression: Invalid attribute type name found; type: 1, valid types: {((B|NULL|SS|BOOL|L|BS|N|NS|S|M),?){10}}$/, done)
    })

    it('should return ValidationException for functions with attr values instead of paths', function (done) {
      var expressions = [
        [ 'attribute_exists(:a) and a=a', 'attribute_exists' ],
        [ 'attribute_not_exists(size(:a))', 'attribute_not_exists' ],
      ]
      async.forEach(expressions, function (expr, cb) {
        assertValidation({
          TableName: 'abc',
          FilterExpression: expr[0],
          ExpressionAttributeValues: { ':a': { 'S': '1' } },
        }, 'Invalid FilterExpression: Operator or function requires a document path; operator or function: ' + expr[1], cb)
      }, done)
    })

    it('should return ValidationException for non-distinct expressions', function (done) {
      var expressions = [
        [ 'a = a AND #a = b AND :views > a', '=', '[a]' ],
        [ '#a <> a', '<>', '[a]' ],
        [ 'a > #a', '>', '[a]' ],
        [ '((a=a) OR (a=a))', '=', '[a]' ],
        [ '((a=a) AND (a=a))', '=', '[a]' ],
        [ 'contains(ab.bc[1].a, ab.bc[1].#a)', 'contains', '[ab, bc, [1], a]' ],
        [ 'attribute_type(ab.bc[1].#a, ab.bc[1].a)', 'attribute_type', '[ab, bc, [1], a]' ],
        [ 'begins_with(ab.bc[1].a, ab.bc[1].#a)', 'begins_with', '[ab, bc, [1], a]' ],
        // ':a > :a', ... is ok
      ]
      async.forEach(expressions, function (expr, cb) {
        assertValidation({
          TableName: 'abc',
          FilterExpression: expr[0],
          ExpressionAttributeNames: { '#a': 'a' },
        }, 'Invalid FilterExpression: The first operand must be distinct from the remaining operands for this operator or function; operator: ' +
          expr[1] + ', first operand: ' + expr[2], cb)
      }, done)
    })

    it('should check table exists before checking key validity', function (done) {
      async.forEach([
        {},
        { b: { S: 'a' } },
        { a: { S: 'a' }, b: { S: 'a' } },
      ], function (expr, cb) {
        assertNotFound({
          TableName: 'abc',
          ExclusiveStartKey: expr,
        }, 'Requested resource not found', cb)
      }, done)
    })

    it('should return ValidationException if unknown index and bad ExclusiveStartKey in hash table', function (done) {
      async.forEach([
        {},
        // {z: {S: 'a'}}, // Returns a 500
        // {a: {S: 'a'}, b: {S: 'a'}}, // Returns a 500
        { a: { S: 'a' }, b: { S: 'a' }, c: { S: 'a' } },
        { z: { S: 'a' }, y: { S: 'a' }, x: { S: 'a' } },
      ], function (expr, cb) {
        assertValidation({
          TableName: helpers.testHashTable,
          IndexName: 'whatever',
          FilterExpression: 'attribute_exists(a.b.c)',
          ExclusiveStartKey: expr,
        }, 'The table does not have the specified index: whatever', cb)
      }, done)
    })

    it('should return ValidationException if unknown index and bad ExclusiveStartKey in range table', function (done) {
      async.forEach([
        {},
        { z: { S: 'a' } },
        // {a: {S: 'a'}, b: {S: 'a'}}, // Returns a 500
        { a: { S: 'a' }, b: { S: 'a' }, c: { S: 'a' } },
        { z: { S: 'a' }, y: { S: 'a' }, x: { S: 'a' }, w: { S: 'a' } },
      ], function (expr, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          IndexName: 'whatever',
          FilterExpression: 'attribute_exists(a.b.c)',
          ExclusiveStartKey: expr,
        }, 'The table does not have the specified index: whatever', cb)
      }, done)
    })

    it('should return ValidationException if ExclusiveStartKey is invalid for local index', function (done) {
      async.forEach([
        {},
        { z: { N: '1' } },
        { a: { B: 'abcd' } },
        { a: { S: 'a' } },
        { a: { S: 'a' }, b: { S: 'a' } },
        { a: { S: 'a' }, c: { S: 'a' } },
        { b: { S: 'a' }, c: { S: 'a' } },
        { a: { S: 'a' }, c: { N: '1' } },
        { a: { S: 'a' }, z: { S: '1' } },
        { a: { S: 'a' }, b: { S: '1' }, c: { S: '1' }, d: { S: '1' } },
      ], function (expr, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          IndexName: 'index1',
          ExclusiveStartKey: expr,
        }, 'The provided starting key is invalid', cb)
      }, done)
    })

    it('should return ValidationException if ExclusiveStartKey is invalid for global index', function (done) {
      async.forEach([
        {},
        { z: { N: '1' } },
        { a: { B: 'abcd' } },
        { a: { S: 'a' } },
        { c: { N: '1' } },
        { c: { S: '1' } },
        { a: { S: 'a' }, b: { S: 'a' } },
        { a: { S: 'a' }, c: { S: 'a' } },
        { a: { S: 'a' }, b: { S: 'a' }, z: { S: 'a' } },
        { a: { S: 'a' }, b: { S: 'a' }, c: { S: 'a' }, z: { S: 'a' } },
      ], function (expr, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          IndexName: 'index3',
          ExclusiveStartKey: expr,
        }, 'The provided starting key is invalid', cb)
      }, done)
    })

    it('should return ValidationException if global range in ExclusiveStartKey is invalid', function (done) {
      async.forEach([
        { c: { S: '1' } },
        { a: { N: '1' }, c: { S: '1' } },
        { a: { N: '1' }, b: { N: '1' }, c: { S: '1' } },
        { a: { N: '1' }, b: { N: '1' }, c: { S: '1' }, e: { N: '1' } },
        { a: { S: 'a' }, b: { S: '1' }, c: { S: '1' }, d: { S: '1' }, e: { S: '1' } },
      ], function (expr, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          IndexName: 'index4',
          Select: 'ALL_ATTRIBUTES',
          ExclusiveStartKey: expr,
        }, 'The provided starting key is invalid', cb)
      }, done)
    })

    it('should return ValidationException for non-existent index name', function (done) {
      async.forEach([
        helpers.testHashTable,
        helpers.testRangeTable,
      ], function (table, cb) {
        assertValidation({
          TableName: table,
          IndexName: 'whatever',
          FilterExpression: 'attribute_exists(a.b.c)',
        }, 'The table does not have the specified index: whatever', cb)
      }, done)
    })

    it('should return ValidationException for specifying ALL_ATTRIBUTES when global index does not have ALL', function (done) {
      assertValidation({
        TableName: helpers.testRangeTable,
        FilterExpression: 'attribute_exists(a.b.c)',
        IndexName: 'index4',
        Select: 'ALL_ATTRIBUTES',
        ExclusiveStartKey: { x: { N: '1' }, y: { N: '1' }, c: { S: 'a' }, d: { S: 'a' } },
      }, 'One or more parameter values were invalid: ' +
        'Select type ALL_ATTRIBUTES is not supported for global secondary index index4 ' +
        'because its projection type is not ALL', done)
    })

    it('should return ValidationException if ExclusiveStartKey does not match schema for local index', function (done) {
      async.forEach([
        { a: { N: '1' }, x: { S: '1' }, y: { S: '1' } },
        { a: { B: 'YQ==' }, b: { S: '1' }, c: { S: '1' } },
        { a: { S: 'a' }, b: { N: '1' }, c: { N: '1' } },
        { a: { S: 'a' }, b: { B: 'YQ==' }, c: { N: '1' } },
        { a: { S: 'a' }, b: { S: 'a' }, c: { N: '1' } },
        { a: { S: 'a' }, b: { S: 'a' }, c: { B: 'YQ==' } },
      ], function (expr, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          IndexName: 'index1',
          ExclusiveStartKey: expr,
        }, 'The provided key element does not match the schema', cb)
      }, done)
    })

    it('should return ValidationException if ExclusiveStartKey does not match schema for global index', function (done) {
      async.forEach([
        { x: { S: '1' }, y: { S: '1' }, c: { N: '1' } },
        { a: { S: '1' }, b: { S: '1' }, c: { B: 'YQ==' } },
      ], function (expr, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          IndexName: 'index3',
          ExclusiveStartKey: expr,
        }, 'The provided key element does not match the schema', cb)
      }, done)
    })

    it('should return ValidationException if ExclusiveStartKey does not match schema for global compound index', function (done) {
      async.forEach([
        { x: { N: '1' }, y: { N: '1' }, c: { S: '1' }, d: { N: '1' } },
        { x: { N: '1' }, y: { N: '1' }, c: { N: '1' }, d: { S: '1' } },
      ], function (expr, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          IndexName: 'index4',
          ExclusiveStartKey: expr,
        }, 'The provided key element does not match the schema', cb)
      }, done)
    })

    it('should return ValidationException if ExclusiveStartKey does not match schema', function (done) {
      async.forEach([
        {},
        { b: { S: 'a' } },
        { a: { S: 'a' }, b: { S: 'a' } },
        { a: { B: 'abcd' } },
        { a: { N: '1' } },
        { a: { BOOL: true } },
        { a: { NULL: true } },
        { a: { SS: [ 'a' ] } },
        { a: { NS: [ '1' ] } },
        { a: { BS: [ 'aaaa' ] } },
        { a: { M: {} } },
        { a: { L: [] } },
      ], function (expr, cb) {
        assertValidation({
          TableName: helpers.testHashTable,
          FilterExpression: 'attribute_exists(a.b.c)',
          ExclusiveStartKey: expr,
        }, 'The provided starting key is invalid: The provided key element does not match the schema', cb)
      }, done)
    })

    it('should return ValidationException if ExclusiveStartKey for range table is invalid', function (done) {
      async.forEach([
        {},
        { z: { N: '1' } },
        { b: { S: 'a' }, c: { S: 'b' } },
        { a: { B: 'abcd' } },
        { a: { S: 'a' } },
        { a: { N: '1' }, b: { S: 'a' }, c: { S: 'b' } },
        { a: { N: '1' }, b: { N: '1' }, z: { N: '1' } },
        { a: { N: '1' }, z: { S: 'a' } },
        { a: { B: 'YQ==' }, b: { S: 'a' } },
        { a: { S: 'a' } },
        { a: { S: 'a' }, c: { N: '1' } },
        { a: { S: 'a' }, z: { S: '1' } },
        { a: { S: 'a' }, b: { S: '1' }, c: { S: '1' } },
        { a: { S: 'a' }, b: { N: '1' } },
        { a: { S: 'a' }, b: { B: 'YQ==' } },
      ], function (expr, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          ExclusiveStartKey: expr,
        }, 'The provided starting key is invalid: The provided key element does not match the schema', cb)
      }, done)
    })

    it('should return ValidationException if range in ExclusiveStartKey is invalid, but hash and local are ok', function (done) {
      async.forEach([
        { a: { S: '1' }, b: { N: '1' }, c: { S: 'a' } },
        { a: { S: '1' }, b: { B: 'YQ==' }, c: { S: 'a' } },
      ], function (expr, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          IndexName: 'index1',
          ExclusiveStartKey: expr,
        }, 'The provided starting key is invalid: The provided key element does not match the schema', cb)
      }, done)
    })

    it('should return ValidationException if global hash in ExclusiveStartKey but bad in query', function (done) {
      async.forEach([
        { x: { N: '1' }, y: { N: '1' }, c: { S: 'a' } },
        { a: { N: '1' }, b: { S: '1' }, c: { S: 'a' } },
        { a: { S: '1' }, b: { N: '1' }, c: { S: 'a' } },
      ], function (expr, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          IndexName: 'index3',
          ExclusiveStartKey: expr,
        }, 'The provided starting key is invalid: The provided key element does not match the schema', cb)
      }, done)
    })

    it('should return ValidationException if global range in ExclusiveStartKey but bad in query', function (done) {
      async.forEach([
        { x: { N: '1' }, y: { N: '1' }, c: { S: 'a' }, d: { S: 'a' } },
        { a: { N: '1' }, b: { S: '1' }, c: { S: 'a' }, d: { S: 'a' } },
        { a: { S: '1' }, b: { N: '1' }, c: { S: 'a' }, d: { S: 'a' } },
      ], function (expr, cb) {
        assertValidation({
          TableName: helpers.testRangeTable,
          IndexName: 'index4',
          ExclusiveStartKey: expr,
        }, 'The provided starting key is invalid: The provided key element does not match the schema', cb)
      }, done)
    })

    it('should return ValidationException if ExclusiveStartKey is from different segment', function (done) {
      var i, items = [], batchReq = { RequestItems: {} }

      for (i = 0; i < 10; i++)
        items.push({ a: { S: String(i) } })

      batchReq.RequestItems[helpers.testHashTable] = items.map(function (item) { return { PutRequest: { Item: item } } })

      request(helpers.opts('BatchWriteItem', batchReq), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        request(opts({ TableName: helpers.testHashTable, Segment: 1, TotalSegments: 2 }), function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Count.should.be.above(0)

          assertValidation({ TableName: helpers.testHashTable,
            Segment: 0,
            TotalSegments: 2,
            FilterExpression: 'attribute_exists(a.b.c)',
            ExclusiveStartKey: { a: res.body.Items[0].a } },
          'The provided starting key is invalid: ' +
            'Invalid ExclusiveStartKey. Please use ExclusiveStartKey with correct Segment. ' +
            'TotalSegments: 2 Segment: 0', done)
        })
      })
    })

    it('should return ValidationException for non-scalar key access', function (done) {
      var expressions = [
        'attribute_exists(a.b.c) and #a = b',
        'attribute_exists(#a.b.c)',
        'attribute_exists(#a[0])',
      ]
      async.forEach(expressions, function (expression, cb) {
        assertValidation({
          TableName: helpers.testHashTable,
          FilterExpression: expression,
          ExpressionAttributeNames: { '#a': 'a' },
        }, 'Key attributes must be scalars; list random access \'[]\' and map lookup \'.\' are not allowed: Key: a', cb)
      }, done)
    })

  })
})