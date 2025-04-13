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
  describe('validations', function () {

    it('should return ValidationException for no TableName', function (done) {
      assertValidation({}, [
        'Value null at \'tableName\' failed to satisfy constraint: ' +
        'Member must not be null',
        'Value null at \'key\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for empty TableName', function (done) {
      assertValidation({ TableName: '' }, [
        'Value \'\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        'Value \'\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 3',
        'Value null at \'key\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for short TableName', function (done) {
      assertValidation({ TableName: 'a;' }, [
        'Value \'a;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        'Value \'a;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 3',
        'Value null at \'key\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for long TableName', function (done) {
      var name = new Array(256 + 1).join('a')
      assertValidation({ TableName: name }, [
        'Value \'' + name + '\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 255',
        'Value null at \'key\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for incorrect attributes', function (done) {
      assertValidation({ TableName: 'abc;', ReturnConsumedCapacity: 'hi',
        ReturnItemCollectionMetrics: 'hi', ReturnValues: 'hi' }, [
        'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
          'Member must satisfy enum value set: [INDEXES, TOTAL, NONE]',
        'Value \'abc;\' at \'tableName\' failed to satisfy constraint: ' +
          'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        'Value \'hi\' at \'returnItemCollectionMetrics\' failed to satisfy constraint: ' +
          'Member must satisfy enum value set: [SIZE, NONE]',
        'Value \'hi\' at \'returnValues\' failed to satisfy constraint: ' +
          'Member must satisfy enum value set: [ALL_NEW, UPDATED_OLD, ALL_OLD, NONE, UPDATED_NEW]',
        'Value null at \'key\' failed to satisfy constraint: ' +
          'Member must not be null',
      ], done)
    })

    it('should return ValidationException if expression and non-expression', function (done) {
      assertValidation({
        TableName: 'abc',
        Key: { a: {} },
        Expected: { a: {} },
        AttributeUpdates: {},
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
        ConditionExpression: '',
        UpdateExpression: '',
      }, 'Can not use both expression and non-expression parameters in the same request: ' +
        'Non-expression parameters: {AttributeUpdates, Expected} Expression parameters: {UpdateExpression, ConditionExpression}', done)
    })

    it('should return ValidationException if ExpressionAttributeNames but no ConditionExpression', function (done) {
      assertValidation({
        TableName: 'abc',
        Key: { a: {} },
        Expected: { a: {} },
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
      }, 'ExpressionAttributeNames can only be specified when using expressions', done)
    })

    it('should return ValidationException if ExpressionAttributeValues but no ConditionExpression', function (done) {
      assertValidation({
        TableName: 'abc',
        Key: { a: {} },
        Expected: { a: {} },
        ExpressionAttributeValues: {},
      }, 'ExpressionAttributeValues can only be specified when using expressions: UpdateExpression and ConditionExpression are null', done)
    })

    it('should return ValidationException for unsupported datatype in Key', function (done) {
      async.forEach([
        {},
        { a: '' },
        { M: { a: {} } },
        { L: [ {} ] },
        { L: [ { a: {} } ] },
      ], function (expr, cb) {
        assertValidation({
          TableName: 'abc',
          Key: { a: expr },
          ConditionExpression: '',
          UpdateExpression: '',
          ExpressionAttributeNames: {},
          ExpressionAttributeValues: {},
        }, 'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', cb)
      }, done)
    })

    it('should return ValidationException for invalid values in Key', function (done) {
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
          Key: { a: expr[0] },
          Expected: { a: {} },
          AttributeUpdates: { a: { x: 'whatever' } },
        }, 'One or more parameter values were invalid: ' + expr[1], cb)
      }, done)
    })

    it('should return ValidationException for empty/invalid numbers in Key', function (done) {
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
        assertValidation({ TableName: 'abc', Key: { a: expr[0] } }, expr[1], cb)
      }, done)
    })

    it('should return ValidationException for multiple datatypes in Key', function (done) {
      assertValidation({ TableName: 'abc', Key: { 'a': { S: 'a', N: '1' } } },
        'Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException if update has no value', function (done) {
      assertValidation({
        TableName: 'abc',
        Key: {},
        Expected: { a: {} },
        AttributeUpdates: { a: { x: 'whatever' } },
      }, 'One or more parameter values were invalid: ' +
        'Only DELETE action is allowed when no attribute value is specified', done)
    })

    it('should return ValidationException if trying to delete incorrect types', function (done) {
      async.forEach([
        { S: '1' },
        { N: '1' },
        { B: 'Yg==' },
        { NULL: true },
        { BOOL: true },
        { M: {} },
        { L: [] },
      ], function (val, cb) {
        assertValidation({
          TableName: 'abc',
          Key: {},
          AttributeUpdates: { a: { Action: 'DELETE', Value: val } },
        }, 'One or more parameter values were invalid: ' +
          'DELETE action with value is not supported for the type ' + Object.keys(val)[0], cb)
      }, done)
    })

    it('should return ValidationException if trying to add incorrect types', function (done) {
      async.forEach([
        { S: '1' },
        { B: 'Yg==' },
        { NULL: true },
        { BOOL: true },
        { M: {} },
      ], function (val, cb) {
        assertValidation({
          TableName: 'abc',
          Key: {},
          AttributeUpdates: { a: { Action: 'ADD', Value: val } },
        }, 'One or more parameter values were invalid: ' +
          'ADD action is not supported for the type ' + Object.keys(val)[0], cb)
      }, done)
    })

    it('should return ValidationException if trying to add type B', function (done) {
      assertValidation({
        TableName: 'abc',
        Key: {},
        AttributeUpdates: { a: { Action: 'ADD', Value: { B: 'Yg==' } } },
      }, 'One or more parameter values were invalid: ' +
        'ADD action is not supported for the type B', done)
    })

    it('should return ValidationException if no value and no exists', function (done) {
      assertValidation({ TableName: 'abc', Key: {}, Expected: { a: {} } },
        'One or more parameter values were invalid: Value must be provided when Exists is null for Attribute: a', done)
    })

    it('should return ValidationException for Exists true with no value', function (done) {
      assertValidation({ TableName: 'abc', Key: {}, Expected: { a: { Exists: true } } },
        'One or more parameter values were invalid: Value must be provided when Exists is true for Attribute: a', done)
    })

    it('should return ValidationException for Exists false with value', function (done) {
      assertValidation({ TableName: 'abc', Key: {}, Expected: { a: { Exists: false, Value: { S: 'a' } } } },
        'One or more parameter values were invalid: Value cannot be used when Exists is false for Attribute: a', done)
    })

    it('should return ValidationException for empty ExpressionAttributeNames', function (done) {
      assertValidation({
        TableName: 'abc',
        Key: {},
        ExpressionAttributeNames: {},
        ExpressionAttributeValues: {},
        ConditionExpression: '',
        UpdateExpression: '',
      }, 'ExpressionAttributeNames must not be empty', done)
    })

    it('should return ValidationException for invalid ExpressionAttributeNames', function (done) {
      assertValidation({
        TableName: 'abc',
        Key: {},
        ExpressionAttributeNames: { 'a': 'a' },
        ExpressionAttributeValues: {},
        ConditionExpression: '',
        UpdateExpression: '',
      }, 'ExpressionAttributeNames contains invalid key: Syntax error; key: "a"', done)
    })

    it('should return ValidationException for empty ExpressionAttributeValues', function (done) {
      assertValidation({
        TableName: 'abc',
        Key: {},
        ExpressionAttributeValues: {},
        ConditionExpression: '',
        UpdateExpression: '',
      }, 'ExpressionAttributeValues must not be empty', done)
    })

    it('should return ValidationException for invalid keys in ExpressionAttributeValues', function (done) {
      assertValidation({
        TableName: 'abc',
        Key: {},
        ExpressionAttributeValues: { ':b': { a: '' }, 'b': { S: 'a' } },
        ConditionExpression: '',
        UpdateExpression: '',
      }, 'ExpressionAttributeValues contains invalid key: Syntax error; key: "b"', done)
    })

    it('should return ValidationException for unsupported datatype in ExpressionAttributeValues', function (done) {
      async.forEach([
        {},
        { a: '' },
        { M: { a: {} } },
        { L: [ {} ] },
        { L: [ { a: {} } ] },
      ], function (expr, cb) {
        assertValidation({
          TableName: 'abc',
          Key: {},
          ExpressionAttributeValues: { ':b': expr },
          ConditionExpression: '',
          UpdateExpression: '',
        }, 'ExpressionAttributeValues contains invalid value: ' +
          'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes for key :b', cb)
      }, done)
    })

    it('should return ValidationException for invalid values in ExpressionAttributeValues', function (done) {
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
          Key: {},
          ExpressionAttributeValues: { ':b': expr[0] },
          ConditionExpression: '',
          UpdateExpression: '',
        }, 'ExpressionAttributeValues contains invalid value: ' +
          'One or more parameter values were invalid: ' + expr[1] + ' for key :b', cb)
      }, done)
    })

    it('should return ValidationException for empty/invalid numbers in ExpressionAttributeValues', function (done) {
      async.forEach([
        [ { S: 'a', N: '' }, 'The parameter cannot be converted to a numeric value' ],
        [ { S: 'a', N: 'b' }, 'The parameter cannot be converted to a numeric value: b' ],
        [ { NS: [ '1', '' ] }, 'The parameter cannot be converted to a numeric value' ],
        [ { NS: [ '1', 'b' ] }, 'The parameter cannot be converted to a numeric value: b' ],
        [ { NS: [ '1', '1' ] }, 'Input collection contains duplicates' ],
      ], function (expr, cb) {
        assertValidation({
          TableName: 'abc',
          Key: {},
          ExpressionAttributeValues: { ':b': expr[0] },
          ConditionExpression: '',
          UpdateExpression: '',
        }, 'ExpressionAttributeValues contains invalid value: ' + expr[1] + ' for key :b', cb)
      }, done)
    })

    it('should return ValidationException for multiple datatypes in ExpressionAttributeValues', function (done) {
      assertValidation({
        TableName: 'abc',
        Key: {},
        ExpressionAttributeValues: { ':b': { S: 'a', N: '1' } },
        ConditionExpression: '',
        UpdateExpression: '',
      }, 'ExpressionAttributeValues contains invalid value: ' +
        'Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes for key :b', done)
    })

    it('should return ValidationException for empty UpdateExpression', function (done) {
      assertValidation({
        TableName: 'abc',
        Key: {},
        ConditionExpression: '',
        UpdateExpression: '',
      }, 'Invalid UpdateExpression: The expression can not be empty;', done)
    })

    it('should return ValidationException for empty ConditionExpression', function (done) {
      assertValidation({
        TableName: 'abc',
        Key: {},
        ConditionExpression: '',
      }, 'Invalid ConditionExpression: The expression can not be empty;', done)
    })

    it('should return ValidationException for syntax errors in UpdateExpression', function (done) {
      async.forEach([
        'whatever',
        'set a',
        'add a',
        'delete a',
        'set a=set',
        'remove a = b',
        'add a = b',
        'delete a = b',
        'delete a b',
        'delete a if_not_exist(b)',
        'add abort b',
        'add a if_not_exist(b)',
        'remove a b',
        'set a b',
        'set :a = b',
        'add :a b',
        'delete :a b',
        'remove :a',
        'set a = b / c',
        'set a = b * c',
        'set a[1] = "eight"',
        'set a[1] = 1',
        // 'set a[1] = b',
        // 'set a.b = b',
        'SET a = if_not_exist(a, 100)',
        // 'SET a = if_not_exists(a, b) + b',
        // 'SET a = if_not_exists(a, if_not_exists(a, b))',
        'SET if_not_exist(a, b) = a',
        'set (a = (b + c))',
        // 'set a = (b + c)',
        'set a = (b.c).d + e',
        'set a = (b.c)[0] + e',
        'set a = b + c + d',
        // 'set a = ((b.c.d)+(e))',
      ], function (updateOpts, cb) {
        assertValidation({
          TableName: 'abc',
          Key: {},
          UpdateExpression: updateOpts,
        }, /^Invalid UpdateExpression: Syntax error; /, cb)
      }, done)
    })

    it('should return ValidationException for reserved keywords', function (done) {
      async.forEach([
        [ ' set #c = :c set abOrt = true ', 'abOrt' ],
        [ ' remove Absolute ', 'Absolute' ],
      ], function (expr, cb) {
        assertValidation({
          TableName: 'abc',
          Key: {},
          UpdateExpression: expr[0],
          ExpressionAttributeValues: { ':a': { S: 'a' }, ':b': { S: 'a' } },
          ExpressionAttributeNames: { '#a': 'a', '#b': 'b' },
        }, 'Invalid UpdateExpression: Attribute name is a reserved keyword; reserved keyword: ' + expr[1], cb)
      }, done)
    })

    it('should return ValidationException for invalid functions in UpdateExpression', function (done) {
      async.forEach([
        'set #c = if_not_exist(:c) set c = d',
      ], function (updateOpts, cb) {
        assertValidation({
          TableName: 'abc',
          Key: {},
          UpdateExpression: updateOpts,
          ExpressionAttributeValues: { ':a': { S: 'a' }, ':b': { S: 'a' } },
          ExpressionAttributeNames: { '#a': 'a', '#b': 'b' },
        }, /^Invalid UpdateExpression: Invalid function name; function: /, cb)
      }, done)
    })

    it('should return ValidationException for multiple sections', function (done) {
      async.forEach([
        [ 'set a = #c set c = :d', 'SET' ],
        [ 'remove #d set a = b remove e', 'REMOVE' ],
        [ 'add #d :e set a = b add e :f', 'ADD' ],
        [ 'delete #d :e set a = b delete #e :f', 'DELETE' ],
      ], function (expr, cb) {
        assertValidation({
          TableName: 'abc',
          Key: {},
          UpdateExpression: expr[0],
          ExpressionAttributeValues: { ':a': { S: 'a' }, ':b': { S: 'a' } },
          ExpressionAttributeNames: { '#a': 'a', '#b': 'b' },
        }, 'Invalid UpdateExpression: The "' + expr[1] + '" section can only be used once in an update expression;', cb)
      }, done)
    })

    it('should return ValidationException for undefined attribute names in UpdateExpression', function (done) {
      async.forEach([
        'SET #c = if_not_exists(:c)',
      ], function (updateOpts, cb) {
        assertValidation({
          TableName: 'abc',
          Key: {},
          UpdateExpression: updateOpts,
          ExpressionAttributeValues: { ':a': { S: 'a' }, ':b': { S: 'a' } },
          ExpressionAttributeNames: { '#a': 'a', '#b': 'b' },
        }, /^Invalid UpdateExpression: An expression attribute name used in the document path is not defined; attribute name: #/, cb)
      }, done)
    })

    it('should return ValidationException for undefined attribute values in UpdateExpression', function (done) {
      async.forEach([
        'SET #a = if_not_exists(:c)',
      ], function (updateOpts, cb) {
        assertValidation({
          TableName: 'abc',
          Key: {},
          UpdateExpression: updateOpts,
          ExpressionAttributeValues: { ':a': { S: 'a' }, ':b': { S: 'a' } },
          ExpressionAttributeNames: { '#a': 'a', '#b': 'b' },
        }, /^Invalid UpdateExpression: An expression attribute value used in expression is not defined; attribute value: :/, cb)
      }, done)
    })

    it('should return ValidationException for overlapping paths in UpdateExpression', function (done) {
      async.forEach([
        [ 'set d[1] = a, d.b = a, c[1].a = a, #c = if_not_exists(a)', '[c, [1], a]', '[c]' ],
        // TODO: This changed at some point, now conflicts with [[3]] instead of [c]?
        // ['set c.b.#c = a, c = a, #d = a', '[c, b, c]', '[[3]]'],
        // TODO: This changed at some point, now conflicts with [[3]] instead of [a]?
        // ['set a = b remove a, #c, #d', '[a]', '[[3]]'],
        [ 'set #c[3].#d = a, #c[3] = a', '[c, [3], [3]]', '[c, [3]]' ],
        // TODO: This changed at some point, now conflicts with [[3]] instead of [c, a]?
        // ['remove c, #c.a, #d', '[c]', '[[3]]'],
        // TODO: This changed at some point, now conflicts with [[3]] instead of [a]?
        // ['remove a, #c, a, #d', '[a]', '[[3]]'],
      ], function (expr, cb) {
        assertValidation({
          TableName: 'abc',
          Key: {},
          UpdateExpression: expr[0],
          ExpressionAttributeNames: { '#c': 'c', '#d': '[3]' },
        }, 'Invalid UpdateExpression: Two document paths overlap with each other; ' +
          'must remove or rewrite one of these paths; path one: ' + expr[1] + ', path two: ' + expr[2], cb)
      }, done)
    })

    it('should return ValidationException for conflicting paths in UpdateExpression', function (done) {
      async.forEach([
        [ 'set #c[3].#d = a, #c.#d[3] = if_not_exists(a)', '[c, [3], [3]]', '[c, [3], [3]]' ],
        [ 'remove a.#c set a[1] = #d', '[a, c]', '[a, [1]]' ],
      ], function (expr, cb) {
        assertValidation({
          TableName: 'abc',
          Key: {},
          UpdateExpression: expr[0],
          ExpressionAttributeNames: { '#b': 'b', '#c': 'c', '#d': '[3]' },
        }, 'Invalid UpdateExpression: Two document paths conflict with each other; ' +
          'must remove or rewrite one of these paths; path one: ' + expr[1] + ', path two: ' + expr[2], cb)
      }, done)
    })

    it('should return ValidationException for incorrect types in UpdateExpression', function (done) {
      async.forEach([
        [ 'set b = list_append(:a, a), c = if_not_exists(a) add a.b :a', { S: 'a' }, 'ADD', 'STRING' ],
        [ 'delete a.b :a', { S: 'a' }, 'DELETE', 'STRING' ],
        [ 'add a.b :a', { NULL: '1' }, 'ADD', 'NULL' ],
        [ 'delete a.b :a', { NULL: 'yes' }, 'DELETE', 'NULL' ],
        [ 'add a.b :a', { BOOL: '0' }, 'ADD', 'BOOLEAN' ],
        [ 'delete a.b :a', { BOOL: 'false' }, 'DELETE', 'BOOLEAN' ],
        [ 'add a.b :a', { B: 'YQ==' }, 'ADD', 'BINARY' ],
        [ 'delete a.b :a', { B: 'YQ==' }, 'DELETE', 'BINARY' ],
        [ 'add a.b :a', { M: { a: { L: [ { N: '1' } ] } } }, 'ADD', 'MAP' ],
        [ 'delete a.b :a', { M: { a: { L: [ { N: '1' } ] } } }, 'DELETE', 'MAP' ],
        [ 'add a.b :a', { L: [ { N: '1' } ] }, 'ADD', 'LIST' ],
        [ 'delete a.b :a', { L: [ { N: '1' } ] }, 'DELETE', 'LIST' ],
        [ 'delete a.b :a', { N: '1' }, 'DELETE', 'NUMBER' ],
      ], function (updateOpts, cb) {
        assertValidation({
          TableName: 'abc',
          Key: {},
          UpdateExpression: updateOpts[0],
          ExpressionAttributeValues: { ':a': updateOpts[1], ':b': { S: 'a' } },
        }, 'Invalid UpdateExpression: Incorrect operand type for operator or function; operator: ' +
          updateOpts[2] + ', operand type: ' + updateOpts[3], cb)
      }, done)
    })

    it('should return ValidationException for incorrect number of operands to functions in UpdateExpression', function (done) {
      async.forEach([
        'set a = if_not_exists(c)',
        'set a = list_append(c)',
      ], function (expression, cb) {
        assertValidation({
          TableName: 'abc',
          Key: {},
          UpdateExpression: expression,
          ExpressionAttributeValues: { ':a': { S: 'a' }, ':b': { S: 'a' } },
          ExpressionAttributeNames: { '#a': 'a', '#b': 'b' },
        }, /^Invalid UpdateExpression: Incorrect number of operands for operator or function; operator or function: [a-z_]+, number of operands: \d+$/, cb)
      }, done)
    })

    it('should return ValidationException for incorrect operand path type to functions in UpdateExpression', function (done) {
      async.forEach([
        'set a = if_not_exists(:a, c)',
        'set a = if_not_exists(if_not_exists(a, b), c)',
      ], function (expression, cb) {
        assertValidation({
          TableName: 'abc',
          Key: {},
          UpdateExpression: expression,
          ExpressionAttributeValues: { ':a': { S: 'a' }, ':b': { S: 'a' } },
          ExpressionAttributeNames: { '#a': 'a', '#b': 'b' },
        }, /^Invalid UpdateExpression: Operator or function requires a document path; operator or function: [a-z_]+$/, cb)
      }, done)
    })

    it('should return ValidationException for incorrect types to functions in UpdateExpression', function (done) {
      async.forEach([
        [ 'set a = list_append(:a, a)', 'list_append', 'S' ],
        [ 'set a = list_append(a, :b)', 'list_append', 'N' ],
        [ 'set a = list_append(:c, a)', 'list_append', 'B' ],
        [ 'set a = list_append(:d, a)', 'list_append', 'BOOL' ],
        [ 'set a = list_append(:e, a)', 'list_append', 'NULL' ],
        [ 'set a = list_append(:f, a)', 'list_append', 'SS' ],
        [ 'set a = list_append(:g, a)', 'list_append', 'NS' ],
        [ 'set a = list_append(:h, a)', 'list_append', 'BS' ],
        [ 'set a = list_append(:i, a)', 'list_append', 'M' ],
        [ 'set a = a + :a', '+', 'S' ],
        [ 'set a = :a + :c', '+', 'S' ],
        [ 'set a = :c + a', '+', 'B' ],
        [ 'set a = a + :d', '+', 'BOOL' ],
        [ 'set a = a + :e', '+', 'NULL' ],
        [ 'set a = a + :f', '+', 'SS' ],
        [ 'set a = a + :g', '+', 'NS' ],
        [ 'set a = a + :h', '+', 'BS' ],
        [ 'set a = a + :i', '+', 'M' ],
        [ 'set a = a + :j', '+', 'L' ],
        [ 'set a = a - :a', '-', 'S' ],
        [ 'set a = :a - :c', '-', 'S' ],
        [ 'set a = :c - a', '-', 'B' ],
        [ 'set a = a - :d', '-', 'BOOL' ],
        [ 'set a = a - :e', '-', 'NULL' ],
        [ 'set a = a - :f', '-', 'SS' ],
        [ 'set a = a - :g', '-', 'NS' ],
        [ 'set a = a - :h', '-', 'BS' ],
        [ 'set a = a - :i', '-', 'M' ],
        [ 'set a = a - :j', '-', 'L' ],
      ], function (expr, cb) {
        assertValidation({
          TableName: 'abc',
          Key: {},
          UpdateExpression: expr[0],
          ExpressionAttributeValues: {
            ':a': { S: 'a' },
            ':b': { N: '1' },
            ':c': { B: 'YQ==' },
            ':d': { BOOL: 'no' },
            ':e': { NULL: 'true' },
            ':f': { SS: [ 'a' ] },
            ':g': { NS: [ '1' ] },
            ':h': { BS: [ 'YQ==' ] },
            ':i': { M: {} },
            ':j': { L: [] },
          },
          ExpressionAttributeNames: { '#a': 'a', '#b': 'b' },
        }, 'Invalid UpdateExpression: Incorrect operand type for operator or function; ' +
          'operator or function: ' + expr[1] + ', operand type: ' + expr[2], cb)
      }, done)
    })

    it('should return ValidationException for extra ExpressionAttributeNames', function (done) {
      assertValidation({
        TableName: 'abc',
        Key: {},
        UpdateExpression: 'remove a set b = list_append(b, if_not_exists(a, :a))',
        ConditionExpression: 'a = :b',
        ExpressionAttributeValues: { ':a': { S: 'a' }, ':b': { S: 'a' } },
        ExpressionAttributeNames: { '#a': 'a', '#b': 'b' },
      }, /^Value provided in ExpressionAttributeNames unused in expressions: keys: {(#a, #b|#b, #a)}$/, done)
    })

    it('should return ValidationException for extra ExpressionAttributeValues', function (done) {
      assertValidation({
        TableName: 'abc',
        Key: {},
        UpdateExpression: 'remove a',
        ConditionExpression: 'a = :b',
        ExpressionAttributeValues: { ':a': { S: 'a' }, ':b': { S: 'a' }, ':c': { S: 'a' } },
      }, /^Value provided in ExpressionAttributeValues unused in expressions: keys: {(:c, :a|:a, :c)}$/, done)
    })

    it('should return ResourceNotFoundException if key is empty and table does not exist', function (done) {
      assertNotFound({ TableName: helpers.randomString(), Key: {} },
        'Requested resource not found', done)
    })

    it('should return ValidationException if Key does not match schema', function (done) {
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
        assertValidation({ TableName: helpers.testHashTable, Key: expr },
          'The provided key element does not match the schema', cb)
      }, done)
    })

    it('should return ValidationException if Key does not match range schema', function (done) {
      assertValidation({ TableName: helpers.testRangeTable, Key: { a: { S: 'a' } } },
        'The provided key element does not match the schema', done)
    })

    it('should return ResourceNotFoundException if table is being created', function (done) {
      var table = {
        TableName: randomName(),
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
      }
      request(helpers.opts('CreateTable', table), function (err) {
        if (err) return done(err)
        assertNotFound({ TableName: table.TableName, Key: { a: { S: 'a' } } },
          'Requested resource not found', done)
        helpers.deleteWhenActive(table.TableName)
      })
    })

    it('should return ValidationException if trying to update key', function (done) {
      async.forEach([
        { AttributeUpdates: { a: { Value: { S: helpers.randomString() } } } },
        { UpdateExpression: 'add a.b :a', ExpressionAttributeValues: { ':a': { N: '1' } } },
        { UpdateExpression: 'delete a :a', ExpressionAttributeValues: { ':a': { NS: [ '1' ] } } },
        { UpdateExpression: 'remove d set b = :a, a = :a', ExpressionAttributeValues: { ':a': { N: '1' } } },
        { UpdateExpression: 'delete b :a remove a', ExpressionAttributeValues: { ':a': { NS: [ '1' ] } } },
        { UpdateExpression: 'set a = a.b + a[1]' },
      ], function (updateOpts, cb) {
        updateOpts.TableName = helpers.testHashTable
        updateOpts.Key = { a: { S: helpers.randomString() } }
        assertValidation(updateOpts, 'One or more parameter values were invalid: ' +
          'Cannot update attribute a. This attribute is part of the key', cb)
      }, done)
    })

    it('should return ValidationException if trying to update range key', function (done) {
      async.forEach([
        { AttributeUpdates: { d: { Value: { N: helpers.randomNumber() } }, b: { Value: { S: helpers.randomString() } } } },
        { UpdateExpression: 'set d[1] = :a add b.b :a', ExpressionAttributeValues: { ':a': { N: '1' } } },
      ], function (updateOpts, cb) {
        updateOpts.TableName = helpers.testRangeTable
        updateOpts.Key = { a: { S: helpers.randomString() }, b: { S: helpers.randomString() } }
        assertValidation(updateOpts, 'One or more parameter values were invalid: ' +
          'Cannot update attribute b. This attribute is part of the key', cb)
      }, done)
    })

    it('should return ValidationException if trying to update wrong type on index', function (done) {
      async.forEach([
        { AttributeUpdates: { d: { Value: { N: helpers.randomNumber() } }, c: { Value: { N: helpers.randomNumber() } } } },
        { UpdateExpression: 'set d.a = a add c :a', ExpressionAttributeValues: { ':a': { N: '1' } } },
        { UpdateExpression: 'set e = c[1], c = a + :a', ExpressionAttributeValues: { ':a': { N: '1' } } },
      ], function (updateOpts, cb) {
        updateOpts.TableName = helpers.testRangeTable
        updateOpts.Key = { a: { S: helpers.randomString() }, b: { S: helpers.randomString() } }
        assertValidation(updateOpts, new RegExp('^One or more parameter values were invalid: ' +
          'Type mismatch for Index Key c Expected: S Actual: N IndexName: index\\d$'), cb)
      }, done)
    })

    it('should return ValidationException if trying to update index map', function (done) {
      async.forEach([
        { UpdateExpression: 'add d.b :a', ExpressionAttributeValues: { ':a': { N: '1' } } },
        { UpdateExpression: 'set d[1] = :a', ExpressionAttributeValues: { ':a': { N: '1' } } },
        { UpdateExpression: 'set e = list_append(a, b), f = d[1]' },
      ], function (updateOpts, cb) {
        updateOpts.TableName = helpers.testRangeTable
        updateOpts.Key = { a: { S: helpers.randomString() }, b: { S: helpers.randomString() } }
        assertValidation(updateOpts, 'Key attributes must be scalars; ' +
          'list random access \'[]\' and map lookup \'.\' are not allowed: IndexKey: d', cb)
      }, done)
    })

    it('should return ValidationException if trying to delete/add incorrect types', function (done) {
      var key = { a: { S: helpers.randomString() } }
      var updates = { b: { Value: { SS: [ '1' ] } }, c: { Value: { N: '1' } }, d: { Value: { NS: [ '1' ] } } }
      request(opts({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates }), function (err) {
        if (err) return done(err)
        async.forEach([
          { AttributeUpdates: { b: { Action: 'DELETE', Value: { NS: [ '1' ] } } } },
          { AttributeUpdates: { b: { Action: 'DELETE', Value: { BS: [ 'YQ==' ] } } } },
          { AttributeUpdates: { c: { Action: 'DELETE', Value: { NS: [ '1' ] } } } },
          { AttributeUpdates: { b: { Action: 'ADD', Value: { NS: [ '1' ] } } } },
          { AttributeUpdates: { d: { Action: 'ADD', Value: { N: '1' } } } },
        ], function (updateOpts, cb) {
          updateOpts.TableName = helpers.testHashTable
          updateOpts.Key = key
          assertValidation(updateOpts, 'Type mismatch for attribute to update', cb)
        }, done)
      })
    })

    it('should return ValidationException if using expression to delete/add incorrect types', function (done) {
      var key = { a: { S: helpers.randomString() } }
      request(opts({
        TableName: helpers.testHashTable,
        Key: key,
        UpdateExpression: 'set b = :b, c = :c, d = :d',
        ExpressionAttributeValues: { ':b': { M: { a: { SS: [ '1' ] } } }, ':c': { N: '1' }, ':d': { NS: [ '1' ] } },
      }), function (err) {
        if (err) return done(err)
        async.forEach([
          { UpdateExpression: 'delete d :a', ExpressionAttributeValues: { ':a': { SS: [ '1' ] } } },
          { UpdateExpression: 'delete b :a', ExpressionAttributeValues: { ':a': { NS: [ '1' ] } } },
          { UpdateExpression: 'delete b.a :a', ExpressionAttributeValues: { ':a': { NS: [ '1' ] } } },
          { UpdateExpression: 'delete b.a :a', ExpressionAttributeValues: { ':a': { BS: [ 'YQ==' ] } } },
          { UpdateExpression: 'delete c :a', ExpressionAttributeValues: { ':a': { NS: [ '1' ] } } },
          { UpdateExpression: 'add b :a', ExpressionAttributeValues: { ':a': { NS: [ '1' ] } } },
          { UpdateExpression: 'add d :a', ExpressionAttributeValues: { ':a': { N: '1' } } },
          { UpdateExpression: 'set e = a + :a', ExpressionAttributeValues: { ':a': { N: '1' } } },
          { UpdateExpression: 'set e = b - :a', ExpressionAttributeValues: { ':a': { N: '1' } } },
          { UpdateExpression: 'set e = list_append(d, if_not_exists(f, :a))', ExpressionAttributeValues: { ':a': { L: [] } } },
        ], function (updateOpts, cb) {
          updateOpts.TableName = helpers.testHashTable
          updateOpts.Key = key
          assertValidation(updateOpts, 'An operand in the update expression has an incorrect data type', cb)
        }, done)
      })
    })

    it('should return ValidationException if trying to reference non-existent attribute', function (done) {
      async.forEach([
        'set c = b',
        'set e = list_append(b, c)',
      ], function (expr, cb) {
        assertValidation({
          TableName: helpers.testHashTable,
          Key: { a: { S: helpers.randomString() } },
          UpdateExpression: expr,
        }, 'The provided expression refers to an attribute that does not exist in the item', cb)
      }, done)
    })

    it('should return ValidationException if trying to update non-existent nested attribute in non-existent item', function (done) {
      async.forEach([
        'set b.a = a',
        'set b[1] = a',
      ], function (expression, cb) {
        assertValidation({
          TableName: helpers.testHashTable,
          Key: { a: { S: helpers.randomString() } },
          UpdateExpression: expression,
        }, 'The document path provided in the update expression is invalid for update', cb)
      }, done)
    })

    it('should return ValidationException if trying to update non-existent nested attribute in existing item', function (done) {
      var key = { a: { S: helpers.randomString() } }
      request(opts({
        TableName: helpers.testHashTable,
        Key: key,
        UpdateExpression: 'set b = a, c = :c, d = :d',
        ExpressionAttributeValues: { ':c': { M: { 1: { S: 'a' } } }, ':d': { L: [ { S: 'a' }, { S: 'b' } ] } },
      }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          'set b.a = :a, #1 = :a',
          'set b[1] = :a, #1 = :a',
          'set y.a = :a, #1 = :a',
          'set y[1] = :a, #1 = :a',
          'set c[1] = :a, #1 = :a',
          'set d.#1 = :a',
          'remove b.a set #1 = :a',
          'remove b[1] set #1 = :a',
          'remove y.a set #1 = :a',
          'remove y[1] set #1 = :a',
          'remove c[1] set #1 = :a',
          'remove d.#1 set #1 = :a',
          'delete b.a :a set #1 = :a',
          'delete b[1] :a set #1 = :a',
          'delete y.a :a set #1 = :a',
          'delete y[1] :a set #1 = :a',
          'delete c[1] :a set #1 = :a',
          'delete d.#1 :a',
          'add b.a :a set #1 = :a',
          'add b[1] :a set #1 = :a',
          'add y.a :a set #1 = :a',
          'add y[1] :a set #1 = :a',
          'add c[1] :a set #1 = :a',
          'add d.#1 :a',
        ], function (expression, cb) {
          assertValidation({
            TableName: helpers.testHashTable,
            Key: key,
            UpdateExpression: expression,
            ExpressionAttributeNames: { '#1': '1' },
            ExpressionAttributeValues: { ':a': { SS: [ 'a' ] } },
          }, 'The document path provided in the update expression is invalid for update', cb)
        }, done)
      })
    })

    it('should return ValidationException if trying to update existing index', function (done) {
      var key = { a: { S: helpers.randomString() }, b: { S: helpers.randomString() } }
      request(opts({
        TableName: helpers.testRangeTable,
        Key: key,
        UpdateExpression: 'set e = :a',
        ExpressionAttributeValues: { ':a': { N: '1' } },
      }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        async.forEach([
          { UpdateExpression: 'set c = e' },
          { UpdateExpression: 'set d = e' },
        ], function (updateOpts, cb) {
          updateOpts.TableName = helpers.testRangeTable
          updateOpts.Key = key
          assertValidation(updateOpts, 'The update expression attempted to update the secondary index key to unsupported type', cb)
        }, done)
      })
    })

    it('should return ValidationException if update item is too big', function (done) {
      var key = { a: { S: helpers.randomString() } }
      var updates = {
        b: { Action: 'PUT', Value: { S: new Array(helpers.MAX_SIZE).join('a') } },
        c: { Action: 'PUT', Value: { N: new Array(38 + 1).join('1') + new Array(89).join('0') } },
      }
      assertValidation({ TableName: helpers.testHashTable, Key: key, AttributeUpdates: updates },
        'Item size to update has exceeded the maximum allowed size', done)
    })

  })
})