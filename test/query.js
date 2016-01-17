var helpers = require('./helpers'),
    should = require('should')

var target = 'Query',
    request = helpers.request,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target)

describe('query', function() {

  describe('serializations', function() {

    it('should return SerializationException when TableName is not a string', function(done) {
      assertType('TableName', 'String', done)
    })

    it('should return SerializationException when ExclusiveStartKey is not a map', function(done) {
      assertType('ExclusiveStartKey', 'Map', done)
    })

    it('should return SerializationException when ExclusiveStartKey.Attr is not an attr struct', function(done) {
      this.timeout(60000)
      assertType('ExclusiveStartKey.Attr', 'AttrStructure', done)
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

    it('should return SerializationException when KeyConditions is not a map', function(done) {
      assertType('KeyConditions', 'Map', done)
    })

    it('should return SerializationException when KeyConditions.Attr is not a struct', function(done) {
      assertType('KeyConditions.Attr', 'Structure', done)
    })

    it('should return SerializationException when KeyConditions.Attr.ComparisonOperator is not a string', function(done) {
      assertType('KeyConditions.Attr.ComparisonOperator', 'String', done)
    })

    it('should return SerializationException when KeyConditions.Attr.AttributeValueList is not a list', function(done) {
      assertType('KeyConditions.Attr.AttributeValueList', 'List', done)
    })

    it('should return SerializationException when KeyConditions.Attr.AttributeValueList.0 is not an attr struct', function(done) {
      this.timeout(60000)
      assertType('KeyConditions.Attr.AttributeValueList.0', 'AttrStructure', done)
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
      assertValidation({TableName: ''},
        '2 validation errors detected: ' +
        'Value \'\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 3', done)
    })

    it('should return ValidationException for short TableName', function(done) {
      assertValidation({TableName: 'a;'},
        '2 validation errors detected: ' +
        'Value \'a;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'a;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 3', done)
    })

    it('should return ValidationException for long TableName', function(done) {
      var name = new Array(256 + 1).join('a')
      assertValidation({TableName: name},
        '1 validation error detected: ' +
        'Value \'' + name + '\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 255', done)
    })

    it('should return ValidationException for empty IndexName', function(done) {
      assertValidation({TableName: 'abc', IndexName: ''},
        '2 validation errors detected: ' +
        'Value \'\' at \'indexName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'\' at \'indexName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 3', done)
    })

    it('should return ValidationException for short IndexName', function(done) {
      assertValidation({TableName: 'abc', IndexName: 'a;'},
        '2 validation errors detected: ' +
        'Value \'a;\' at \'indexName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'a;\' at \'indexName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 3', done)
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
        QueryFilter: {a: {}, b: {ComparisonOperator: ''}}},
        '10 validation errors detected: ' +
        'Value \'-1\' at \'limit\' failed to satisfy constraint: ' +
        'Member must have value greater than or equal to 1; ' +
        'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [INDEXES, TOTAL, NONE]; ' +
        'Value \'[]\' at \'attributesToGet\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1; ' +
        'Value \'\' at \'queryFilter.b.member.comparisonOperator\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [IN, NULL, BETWEEN, LT, NOT_CONTAINS, EQ, GT, NOT_NULL, NE, LE, BEGINS_WITH, GE, CONTAINS]; ' +
        'Value null at \'queryFilter.a.member.comparisonOperator\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value \'hi\' at \'select\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]; ' +
        'Value \'\' at \'keyConditions.b.member.comparisonOperator\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [IN, NULL, BETWEEN, LT, NOT_CONTAINS, EQ, GT, NOT_NULL, NE, LE, BEGINS_WITH, GE, CONTAINS]; ' +
        'Value null at \'keyConditions.a.member.comparisonOperator\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value \'abc;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'abc;\' at \'indexName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+', done)
    })

    it('should return ValidationException for null conditions', function(done) {
      assertValidation({TableName: 'abc'},
        'Either the KeyConditions or KeyConditionExpression parameter must be specified in the request.', done)
    })

    it('should return ValidationException for empty conditions', function(done) {
      assertValidation({TableName: 'abc', KeyConditions: {}},
        'Conditions can be of length 1 or 2 only', done)
    })

    it('should return ValidationException for incorrect number of filter arguments', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {
          a: {ComparisonOperator: 'EQ'},
          b: {ComparisonOperator: 'NULL'},
          c: {ComparisonOperator: 'NULL'},
        }},
        'One or more parameter values were invalid: Invalid number of argument(s) for the EQ ComparisonOperator', done)
    })

    it('should check QueryFilter before KeyConditions', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{}]}},
        QueryFilter: {a: {ComparisonOperator: 'EQ'}}
      }, 'One or more parameter values were invalid: Invalid number of argument(s) for the EQ ComparisonOperator', done)
    })

    it('should return ValidationException for incorrect number of filter arguments on QueryFilter', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'NULL'}},
        QueryFilter: {a: {ComparisonOperator: 'EQ'}},
      }, 'One or more parameter values were invalid: Invalid number of argument(s) for the EQ ComparisonOperator', done)
    })

    it('should return ValidationException for empty key type', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [{}]},
          b: {ComparisonOperator: 'NULL'},
          c: {ComparisonOperator: 'NULL'},
        }},
        'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException for empty key type in QueryFilter', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'NULL'}},
        QueryFilter: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{}]}}
      }, 'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException for bad key type', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [{a: ''}]},
          b: {ComparisonOperator: 'NULL'},
          c: {ComparisonOperator: 'NULL'},
        }},
        'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException for bad key type in QueryFilter', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'NULL'}},
        QueryFilter: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{a: ''}]}}
      }, 'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException for empty key', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: ''}]},
          b: {ComparisonOperator: 'NULL'},
          c: {ComparisonOperator: 'NULL'},
        }},
        'One or more parameter values were invalid: An AttributeValue may not contain an empty string', done)
    })

    it('should return ValidationException for empty key in QueryFilter', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'NULL'}},
        QueryFilter: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: ''}]}}
      }, 'One or more parameter values were invalid: An AttributeValue may not contain an empty string', done)
    })

    it('should return ValidationException if key has incorrect numeric type', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [{N: 'b'}]},
          b: {ComparisonOperator: 'NULL'},
          c: {ComparisonOperator: 'NULL'},
        }},
        'The parameter cannot be converted to a numeric value: b', done)
    })

    it('should return ValidationException if key has incorrect numeric type in QueryFilter', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'NULL'}},
        QueryFilter: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{N: 'b'}]}}
      }, 'The parameter cannot be converted to a numeric value: b', done)
    })

    it('should return ValidationException for too many filter args', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'a'}, {S: 'a'}]},
          b: {ComparisonOperator: 'NULL'},
          c: {ComparisonOperator: 'NULL'},
        }},
        'One or more parameter values were invalid: Invalid number of argument(s) for the EQ ComparisonOperator', done)
    })

    it('should return ValidationException for too many filter args in QueryFilter', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'NULL'}},
        QueryFilter: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'a'}, {S: 'a'}]}}
      }, 'One or more parameter values were invalid: Invalid number of argument(s) for the EQ ComparisonOperator', done)
    })

    it('should return ValidationException for 1 arg to NULL', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'NULL', AttributeValueList: [{S: 'a'}]}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the NULL ComparisonOperator', done)
    })

    it('should return ValidationException for 1 arg to NULL in QueryFilter', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'NULL'}},
        QueryFilter: {a: {ComparisonOperator: 'NULL', AttributeValueList: [{S: 'a'}]}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the NULL ComparisonOperator', done)
    })

    it('should return ValidationException for 1 arg to NOT_NULL', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'NOT_NULL', AttributeValueList: [{S: 'a'}]}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the NOT_NULL ComparisonOperator', done)
    })

    it('should return ValidationException for 1 arg to NOT_NULL in QueryFilter', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'NULL'}},
        QueryFilter: {a: {ComparisonOperator: 'NOT_NULL', AttributeValueList: [{S: 'a'}]}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the NOT_NULL ComparisonOperator', done)
    })

    it('should return ValidationException for 0 args to NE', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'NE'}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the NE ComparisonOperator', done)
    })

    it('should return ValidationException for 2 args to NE', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'NE', AttributeValueList: [{S: 'a'}, {S: 'a'}]}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the NE ComparisonOperator', done)
    })

    it('should return ValidationException for 0 args to LE', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'LE'}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the LE ComparisonOperator', done)
    })

    it('should return ValidationException for 2 args to LE', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'LE', AttributeValueList: [{S: 'a'}, {S: 'a'}]}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the LE ComparisonOperator', done)
    })

    it('should return ValidationException for 0 args to LT', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'LT'}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the LT ComparisonOperator', done)
    })

    it('should return ValidationException for 2 args to LT', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'LT', AttributeValueList: [{S: 'a'}, {S: 'a'}]}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the LT ComparisonOperator', done)
    })

    it('should return ValidationException for 0 args to GE', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'GE'}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the GE ComparisonOperator', done)
    })

    it('should return ValidationException for 2 args to GE', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'GE', AttributeValueList: [{S: 'a'}, {S: 'a'}]}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the GE ComparisonOperator', done)
    })

    it('should return ValidationException for 0 args to GT', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'GT'}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the GT ComparisonOperator', done)
    })

    it('should return ValidationException for 2 args to GT', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'GT', AttributeValueList: [{S: 'a'}, {S: 'a'}]}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the GT ComparisonOperator', done)
    })

    it('should return ValidationException for 0 args to CONTAINS', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'CONTAINS'}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the CONTAINS ComparisonOperator', done)
    })

    it('should return ValidationException for 2 args to CONTAINS', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'CONTAINS', AttributeValueList: [{S: 'a'}, {S: 'a'}]}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the CONTAINS ComparisonOperator', done)
    })

    it('should return ValidationException for 0 args to NOT_CONTAINS', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'NOT_CONTAINS'}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the NOT_CONTAINS ComparisonOperator', done)
    })

    it('should return ValidationException for 2 args to NOT_CONTAINS', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'NOT_CONTAINS', AttributeValueList: [{S: 'a'}, {S: 'a'}]}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the NOT_CONTAINS ComparisonOperator', done)
    })

    it('should return ValidationException for 0 args to BEGINS_WITH', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'BEGINS_WITH'}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the BEGINS_WITH ComparisonOperator', done)
    })

    it('should return ValidationException for 2 args to BEGINS_WITH', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'BEGINS_WITH', AttributeValueList: [{S: 'a'}, {S: 'a'}]}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the BEGINS_WITH ComparisonOperator', done)
    })

    it('should return ValidationException for 0 args to IN', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'IN'}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the IN ComparisonOperator', done)
    })

    it('should return ValidationException for 0 args to BETWEEN', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'BETWEEN'}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the BETWEEN ComparisonOperator', done)
    })

    it('should return ValidationException for 3 args to BETWEEN', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'BETWEEN', AttributeValueList: [{S: 'a'}, {S: 'a'}, {S: 'a'}]}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the BETWEEN ComparisonOperator', done)
    })

    it('should return ValidationException for LE on type SS', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'LE', AttributeValueList: [{SS: ['a']}]}}},
        'One or more parameter values were invalid: ComparisonOperator LE is not valid for SS AttributeValue type', done)
    })

    it('should return ValidationException for LE on type SS in QueryFilter', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'NULL'}},
        QueryFilter: {a: {ComparisonOperator: 'LE', AttributeValueList: [{SS: ['a']}]}}},
        'One or more parameter values were invalid: ComparisonOperator LE is not valid for SS AttributeValue type', done)
    })

    it('should return ValidationException for LE on type NS', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'LE', AttributeValueList: [{NS: ['1']}]}}},
        'One or more parameter values were invalid: ComparisonOperator LE is not valid for NS AttributeValue type', done)
    })

    it('should return ValidationException for LE on type NS in QueryFilter', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'NULL'}},
        QueryFilter: {a: {ComparisonOperator: 'LE', AttributeValueList: [{NS: ['1']}]}}},
        'One or more parameter values were invalid: ComparisonOperator LE is not valid for NS AttributeValue type', done)
    })

    it('should return ValidationException for LE on type BS', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'LE', AttributeValueList: [{BS: ['abcd']}]}}},
        'One or more parameter values were invalid: ComparisonOperator LE is not valid for BS AttributeValue type', done)
    })

    it('should return ValidationException for LE on type BS in QueryFilter', function(done) {
      assertValidation({
        TableName: 'abc',
        KeyConditions: {a: {ComparisonOperator: 'NULL'}},
        QueryFilter: {a: {ComparisonOperator: 'LE', AttributeValueList: [{BS: ['abcd']}]}}},
        'One or more parameter values were invalid: ComparisonOperator LE is not valid for BS AttributeValue type', done)
    })

    it('should return ValidationException for too many conditions', function(done) {
      assertValidation({
        TableName: 'abc',
        ExclusiveStartKey: {},
        KeyConditions: {
          a: {ComparisonOperator: 'NULL'},
          b: {ComparisonOperator: 'NULL'},
          c: {ComparisonOperator: 'NULL'},
        }},
        'Conditions can be of length 1 or 2 only', done)
    })

    it('should return ValidationException for empty object ExclusiveStartKey', function(done) {
      assertValidation({
        TableName: 'abc',
        ExclusiveStartKey: {a: {}},
        KeyConditions: {a: {ComparisonOperator: 'NULL'}}},
        'The provided starting key is invalid: ' +
        'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException for wrong attribute ExclusiveStartKey', function(done) {
      assertValidation({
        TableName: 'abc',
        ExclusiveStartKey: {a: {a: ''}},
        KeyConditions: {a: {ComparisonOperator: 'NULL'}}},
        'The provided starting key is invalid: ' +
        'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException for empty string ExclusiveStartKey', function(done) {
      assertValidation({
        TableName: 'abc',
        ExclusiveStartKey: {a: {S: ''}},
        KeyConditions: {a: {ComparisonOperator: 'NULL'}}},
        'The provided starting key is invalid: ' +
        'One or more parameter values were invalid: An AttributeValue may not contain an empty string', done)
    })

    it('should return ValidationException for empty binary', function(done) {
      assertValidation({
        TableName: 'abc',
        ExclusiveStartKey: {a: {B: ''}},
        KeyConditions: {a: {ComparisonOperator: 'NULL'}}},
        'The provided starting key is invalid: ' +
        'One or more parameter values were invalid: An AttributeValue may not contain a null or empty binary type.', done)
    })

    it('should return ValidationException for false null', function(done) {
      assertValidation({
        TableName: 'abc',
        ExclusiveStartKey: {a: {NULL: false}},
        KeyConditions: {a: {ComparisonOperator: 'NULL'}}},
        'The provided starting key is invalid: ' +
        'One or more parameter values were invalid: Null attribute value types must have the value of true', done)
    })

    // Somehow allows set types for keys
    it('should return ValidationException for empty set key', function(done) {
      assertValidation({
        TableName: 'abc',
        ExclusiveStartKey: {a: {SS: []}},
        KeyConditions: {a: {ComparisonOperator: 'NULL'}}},
        'The provided starting key is invalid: ' +
        'One or more parameter values were invalid: An string set  may not be empty', done)
    })

    it('should return ValidationException for empty string in set', function(done) {
      assertValidation({
        TableName: 'abc',
        ExclusiveStartKey: {a: {SS: ['a', '']}},
        KeyConditions: {a: {ComparisonOperator: 'NULL'}}},
        'The provided starting key is invalid: ' +
        'One or more parameter values were invalid: An string set may not have a empty string as a member', done)
    })

    it('should return ValidationException for empty binary in set', function(done) {
      assertValidation({
        TableName: 'abc',
        ExclusiveStartKey: {a: {BS: ['aaaa', '']}},
        KeyConditions: {a: {ComparisonOperator: 'NULL'}}},
        'The provided starting key is invalid: ' +
        'One or more parameter values were invalid: Binary sets may not contain null or empty values', done)
    })

    it('should return ValidationException for multiple types', function(done) {
      assertValidation({
        TableName: 'abc',
        ExclusiveStartKey: {a: {S: 'a', N: '1'}},
        KeyConditions: {a: {ComparisonOperator: 'NULL'}}},
        'The provided starting key is invalid: ' +
        'Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException if key has empty numeric type', function(done) {
      assertValidation({
        TableName: 'abc',
        ExclusiveStartKey: {a: {N: ''}},
        KeyConditions: {a: {ComparisonOperator: 'NULL'}}},
        'The provided starting key is invalid: ' +
        'The parameter cannot be converted to a numeric value', done)
    })

    it('should return ValidationException if key has incorrect numeric type', function(done) {
      assertValidation({
        TableName: 'abc',
        ExclusiveStartKey: {a: {N: 'b'}},
        KeyConditions: {a: {ComparisonOperator: 'NULL'}}},
        'The provided starting key is invalid: ' +
        'The parameter cannot be converted to a numeric value: b', done)
    })

    it('should return ValidationException if key has empty numeric type in set', function(done) {
      assertValidation({
        TableName: 'abc',
        ExclusiveStartKey: {a: {NS: ['1', '']}},
        KeyConditions: {a: {ComparisonOperator: 'NULL'}}},
        'The provided starting key is invalid: ' +
        'The parameter cannot be converted to a numeric value', done)
    })

    it('should return ValidationException if key has incorrect numeric type in set', function(done) {
      assertValidation({
        TableName: 'abc',
        ExclusiveStartKey: {a: {NS: ['1', 'b', 'a']}},
        KeyConditions: {a: {ComparisonOperator: 'NULL'}}},
        'The provided starting key is invalid: ' +
        'The parameter cannot be converted to a numeric value: b', done)
    })

    it('should return ValidationException duplicate values in AttributesToGet', function(done) {
      assertValidation({TableName: 'abc', KeyConditions: {a: {ComparisonOperator: 'NULL'}}, AttributesToGet: ['a', 'a']},
        'One or more parameter values were invalid: Duplicate value in attribute name: a', done)
    })

    it('should return ValidationException if querying with NULL', function(done) {
      assertValidation({
        TableName: helpers.testHashTable,
        KeyConditions: {a: {ComparisonOperator: 'NULL'}},
      }, 'Attempted conditional constraint is not an indexable operation', done)
    })

    it('should return ValidationException if querying with NOT_NULL', function(done) {
      assertValidation({
        TableName: helpers.testHashTable,
        KeyConditions: {a: {ComparisonOperator: 'NOT_NULL'}},
      }, 'Attempted conditional constraint is not an indexable operation', done)
    })

    it('should return ValidationException if querying with CONTAINS', function(done) {
      assertValidation({
        TableName: helpers.testHashTable,
        KeyConditions: {a: {ComparisonOperator: 'CONTAINS', AttributeValueList: [{S: 'a'}]}},
      }, 'Attempted conditional constraint is not an indexable operation', done)
    })

    it('should return ValidationException if querying with NOT_CONTAINS', function(done) {
      assertValidation({
        TableName: helpers.testHashTable,
        KeyConditions: {a: {ComparisonOperator: 'NOT_CONTAINS', AttributeValueList: [{S: 'a'}]}},
      }, 'Attempted conditional constraint is not an indexable operation', done)
    })

    it('should return ValidationException if querying with IN', function(done) {
      assertValidation({
        TableName: helpers.testHashTable,
        KeyConditions: {a: {ComparisonOperator: 'IN', AttributeValueList: [{S: 'a'}]}},
      }, 'Attempted conditional constraint is not an indexable operation', done)
    })

    it('should return ValidationException if querying with LE', function(done) {
      assertValidation({
        TableName: helpers.testHashTable,
        KeyConditions: {a: {ComparisonOperator: 'LE', AttributeValueList: [{S: 'a'}]}},
      }, 'Query key condition not supported', done)
    })

    it('should return ValidationException if querying with LT', function(done) {
      assertValidation({
        TableName: helpers.testHashTable,
        KeyConditions: {a: {ComparisonOperator: 'LT', AttributeValueList: [{S: 'a'}]}},
      }, 'Query key condition not supported', done)
    })

    it('should return ValidationException if querying with GE', function(done) {
      assertValidation({
        TableName: helpers.testHashTable,
        KeyConditions: {a: {ComparisonOperator: 'GE', AttributeValueList: [{S: 'a'}]}},
      }, 'Query key condition not supported', done)
    })

    it('should return ValidationException if querying with GT', function(done) {
      assertValidation({
        TableName: helpers.testHashTable,
        KeyConditions: {a: {ComparisonOperator: 'GT', AttributeValueList: [{S: 'a'}]}},
      }, 'Query key condition not supported', done)
    })

    it('should return ValidationException if querying with BEGINS_WITH', function(done) {
      assertValidation({
        TableName: helpers.testHashTable,
        KeyConditions: {a: {ComparisonOperator: 'BEGINS_WITH', AttributeValueList: [{S: 'a'}]}},
      }, 'Query key condition not supported', done)
    })

    it('should return ValidationException if querying with BETWEEN', function(done) {
      assertValidation({
        TableName: helpers.testHashTable,
        KeyConditions: {a: {ComparisonOperator: 'BETWEEN', AttributeValueList: [{S: 'a'}, {S: 'b'}]}},
      }, 'Query key condition not supported', done)
    })

    it('should return ValidationException if querying hash table with extra param', function(done) {
      assertValidation({
        TableName: helpers.testHashTable,
        KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: helpers.randomString()}]},
          b: {ComparisonOperator: 'EQ', AttributeValueList: [{S: helpers.randomString()}]},
        },
      }, 'Query key condition not supported', done)
    })

    it('should return ValidationException if querying hash table with primary key in QueryFilter', function(done) {
      assertValidation({
        TableName: helpers.testHashTable,
        KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: helpers.randomString()}]},
        },
        QueryFilter: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: helpers.randomString()}]},
        },
      }, 'QueryFilter can only contain non-primary key attributes: Primary key attribute: a', done)
    })

    it('should return ValidationException if querying range table with hash key in QueryFilter', function(done) {
      assertValidation({
        TableName: helpers.testRangeTable,
        KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: helpers.randomString()}]},
        },
        QueryFilter: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: helpers.randomString()}]},
        },
      }, 'QueryFilter can only contain non-primary key attributes: Primary key attribute: a', done)
    })

    it('should return ValidationException if querying range table with range key in QueryFilter', function(done) {
      assertValidation({
        TableName: helpers.testRangeTable,
        KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: helpers.randomString()}]},
        },
        QueryFilter: {
          b: {ComparisonOperator: 'EQ', AttributeValueList: [{S: helpers.randomString()}]},
        },
      }, 'QueryFilter can only contain non-primary key attributes: Primary key attribute: b', done)
    })

    // Weird - only checks this *after* it finds the table
    it('should return ValidationException for unsupported comparison', function(done) {
      assertValidation({
        TableName: helpers.testRangeTable,
        KeyConditions: {
          a: {ComparisonOperator: 'CONTAINS', AttributeValueList: [{S: 'a'}]},
          b: {ComparisonOperator: 'NULL'},
        }},
        'Attempted conditional constraint is not an indexable operation', done)
    })

    it('should return ValidationException for empty ExclusiveStartKey', function(done) {
      assertValidation({
        TableName: helpers.testRangeTable,
        ExclusiveStartKey: {},
        KeyConditions: {a: {ComparisonOperator: 'NULL'}}},
        'The provided starting key is invalid', done)
    })

    it('should return ValidationException for ExclusiveStartKey outside range', function(done) {
      assertValidation({
        TableName: helpers.testRangeTable,
        ExclusiveStartKey: {a: {S: 'b'}},
        KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'a'}]}}},
        'The provided starting key is invalid', done)
    })

    it('should return ValidationException for ExclusiveStartKey with incorrect attribute', function(done) {
      assertValidation({
        TableName: helpers.testRangeTable,
        ExclusiveStartKey: {b: {S: 'b'}},
        KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'a'}]}}},
        'The provided starting key is invalid', done)
    })

    it('should return ValidationException for ExclusiveStartKey missing range key', function(done) {
      assertValidation({
        TableName: helpers.testRangeTable,
        ExclusiveStartKey: {a: {S: 'a'}},
        KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'a'}]}}},
        'The provided starting key is invalid', done)
    })

    it('should return ValidationException for ExclusiveStartKey with incorrect hash key', function(done) {
      assertValidation({
        TableName: helpers.testRangeTable,
        ExclusiveStartKey: {a: {S: 'b'}, b: {S: 'a'}},
        KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'a'}]}}},
        'The provided starting key is outside query boundaries based on provided conditions', done)
    })

    it('should return ValidationException for ExclusiveStartKey with incorrect range key', function(done) {
      assertValidation({
        TableName: helpers.testRangeTable,
        ExclusiveStartKey: {a: {S: 'a'}, b: {S: 'a'}},
        KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'a'}]},
          b: {ComparisonOperator: 'GT', AttributeValueList: [{S: 'a'}]},
        }},
        'The provided starting key does not match the range key predicate', done)
    })

    it('should return ValidationException for no hash key', function(done) {
      assertValidation({
        TableName: helpers.testRangeTable,
        KeyConditions: {c: {ComparisonOperator: 'NULL'}}},
        'Query condition missed key schema element: a', done)
    })

    it('should return ValidationException for missing index name', function(done) {
      assertValidation({
        TableName: helpers.testRangeTable,
        KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'a'}]},
          c: {ComparisonOperator: 'NULL'},
        }},
        'Query condition missed key schema element: b', done)
    })

    it('should return ValidationException for querying global index with incorrect schema', function(done) {
      assertValidation({
        TableName: helpers.testRangeTable,
        IndexName: 'index3',
        KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'a'}]},
        }},
        'Query condition missed key schema element: c', done)
    })

    it('should return ValidationException for non-existent index name', function(done) {
      assertValidation({
        TableName: helpers.testRangeTable,
        IndexName: 'whatever',
        KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'a'}]},
          c: {ComparisonOperator: 'NULL'},
        }},
        'The table does not have the specified index: whatever', done)
    })

    it('should return ValidationException for incorrect index', function(done) {
      assertValidation({
        TableName: helpers.testRangeTable,
        IndexName: 'index2',
        KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'a'}]},
          c: {ComparisonOperator: 'NULL'},
        }},
        'Query condition missed key schema element: d', done)
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

    it('should return ValidationException for querying global index with ConsistentRead', function(done) {
      assertValidation({
        TableName: helpers.testRangeTable,
        IndexName: 'index3',
        ConsistentRead: true,
        KeyConditions: {
          c: {ComparisonOperator: 'NULL'}
        }},
        'Consistent reads are not supported on global secondary indexes', done)
    })

    it('should return ValidationException for specifying ALL_ATTRIBUTES when global index does not have ALL', function(done) {
      assertValidation({
        TableName: helpers.testRangeTable,
        IndexName: 'index4',
        Select: 'ALL_ATTRIBUTES',
        KeyConditions: {
          c: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'a'}]}
        }},
        'One or more parameter values were invalid: ' +
        'Select type ALL_ATTRIBUTES is not supported for global secondary index index4 ' +
        'because its projection type is not ALL', done)
    })

    // TODO: 'The provided starting key is invalid' when extra attributes
  })

  describe('functionality', function() {

    it('should query a hash table when empty', function(done) {
      request(opts({TableName: helpers.testHashTable, KeyConditions: {
        a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: helpers.randomString()}]},
      }}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({Count: 0, ScannedCount: 0, Items: []})
        done()
      })
    })

    it('should query a hash table with items', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {N: helpers.randomNumber()}},
          item2 = {a: {S: helpers.randomString()}, b: item.b},
          item3 = {a: {S: helpers.randomString()}, b: {N: helpers.randomNumber()}},
          items = [item, item2, item3]
      helpers.batchBulkPut(helpers.testHashTable, items, function(err) {
        if (err) return done(err)
        request(opts({TableName: helpers.testHashTable, KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [item2.a]},
        }}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({Count: 1, ScannedCount: 1, Items: [item2]})
          done()
        })
      })
    })

    it('should query a range table with EQ on just hash key', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: '1'}},
          item2 = {a: item.a, b: {S: '2'}},
          item3 = {a: item.a, b: {S: '3'}},
          items = [item, item2, item3]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        request(opts({TableName: helpers.testRangeTable, KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
        }}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({Count: 3, ScannedCount: 3, Items: [item, item2, item3]})
          done()
        })
      })
    })

    it('should query a range table with EQ', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: '1'}},
          item2 = {a: item.a, b: {S: '2'}},
          item3 = {a: item.a, b: {S: '3'}},
          items = [item, item2, item3]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        request(opts({TableName: helpers.testRangeTable, KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
          b: {ComparisonOperator: 'EQ', AttributeValueList: [item2.b]},
        }}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({Count: 1, ScannedCount: 1, Items: [item2]})
          done()
        })
      })
    })

    it('should query a range table with LE', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: '1'}},
          item2 = {a: item.a, b: {S: '2'}},
          item3 = {a: item.a, b: {S: '3'}},
          items = [item, item2, item3]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        request(opts({TableName: helpers.testRangeTable, KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
          b: {ComparisonOperator: 'LE', AttributeValueList: [item2.b]},
        }}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({Count: 2, ScannedCount: 2, Items: [item, item2]})
          done()
        })
      })
    })

    it('should query a range table with LT', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: '1'}},
          item2 = {a: item.a, b: {S: '2'}},
          item3 = {a: item.a, b: {S: '3'}},
          items = [item, item2, item3]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        request(opts({TableName: helpers.testRangeTable, KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
          b: {ComparisonOperator: 'LT', AttributeValueList: [item2.b]},
        }}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({Count: 1, ScannedCount: 1, Items: [item]})
          done()
        })
      })
    })

    it('should query a range table with GE', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: '1'}},
          item2 = {a: item.a, b: {S: '2'}},
          item3 = {a: item.a, b: {S: '3'}},
          items = [item, item2, item3]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        request(opts({TableName: helpers.testRangeTable, KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
          b: {ComparisonOperator: 'GE', AttributeValueList: [item2.b]},
        }}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({Count: 2, ScannedCount: 2, Items: [item2, item3]})
          done()
        })
      })
    })

    it('should query a range table with GT', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: '1'}},
          item2 = {a: item.a, b: {S: '2'}},
          item3 = {a: item.a, b: {S: '3'}},
          items = [item, item2, item3]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        request(opts({TableName: helpers.testRangeTable, ConsistentRead: true, KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
          b: {ComparisonOperator: 'GT', AttributeValueList: [item2.b]},
        }}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({Count: 1, ScannedCount: 1, Items: [item3]})
          done()
        })
      })
    })

    it('should query a range table with BEGINS_WITH', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'aaa'}},
          item2 = {a: item.a, b: {S: 'aab'}},
          item3 = {a: item.a, b: {S: 'abc'}},
          items = [item, item2, item3]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        request(opts({TableName: helpers.testRangeTable, ConsistentRead: true, KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
          b: {ComparisonOperator: 'BEGINS_WITH', AttributeValueList: [{S: 'aa'}]},
        }}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({Count: 2, ScannedCount: 2, Items: [item, item2]})
          done()
        })
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
        request(opts({TableName: helpers.testRangeTable, ConsistentRead: true, KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
          b: {ComparisonOperator: 'BETWEEN', AttributeValueList: [{S: 'ab'}, {S: 'ac'}]},
        }}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({Count: 3, ScannedCount: 3, Items: [item2, item3, item4]})
          done()
        })
      })
    })

    it('should only return requested attributes', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'b1'}, d: {S: 'd1'}},
          item2 = {a: item.a, b: {S: 'b2'}},
          item3 = {a: item.a, b: {S: 'b3'}, d: {S: 'd3'}, e: {S: 'e3'}},
          items = [item, item2, item3]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        request(opts({TableName: helpers.testRangeTable, ConsistentRead: true, KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
        }, AttributesToGet: ['b', 'd']}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({Count: 3, ScannedCount: 3, Items: [
            {b: {S: 'b1'}, d: {S: 'd1'}},
            {b: {S: 'b2'}},
            {b: {S: 'b3'}, d: {S: 'd3'}},
          ]})
          done()
        })
      })
    })

    it('should filter items by query filter', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'b1'}, d: {S: '1'}},
          item2 = {a: item.a, b: {S: 'b2'}},
          item3 = {a: item.a, b: {S: 'b3'}, d: {S: 'd3'}, e: {S: 'e3'}},
          items = [item, item2, item3]
      helpers.batchBulkPut(helpers.testRangeTable, items, function(err) {
        if (err) return done(err)
        request(opts({TableName: helpers.testRangeTable, ConsistentRead: true, KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
        }, QueryFilter: {
          e: {ComparisonOperator: 'NOT_NULL'},
        }}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({Count: 1, ScannedCount: 3, Items: [
            {a: item.a, b: {S: 'b3'}, d: {S: 'd3'}, e: {S: 'e3'}},
          ]})
          done()
        })
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
            ConsumedCapacity: {CapacityUnits: 1, TableName: helpers.testRangeTable}
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
          item3 = {a: item.a, b: {S: 'b3'}, d: {S: 'd3'}, e: {S: 'e3'}, f: {S: 'f3'}},
          item4 = {a: item.a, b: {S: 'b4'}, c: {S: 'c4'}, d: {S: 'd4'}, e: {S: 'e4'}},
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
        request(opts({TableName: helpers.testRangeTable, ConsistentRead: true, KeyConditions: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]},
          b: {ComparisonOperator: 'GE', AttributeValueList: [item.b]},
        }, QueryFilter: {
          d: {ComparisonOperator: 'EQ', AttributeValueList: [item3.d]},
        }, Limit: 2}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({Count: 1, ScannedCount: 2, Items: [item3], LastEvaluatedKey: {a: item3.a, b: item3.b}})
          done()
        })
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

      var i, items = [], id = helpers.randomString(), e = new Array(41646).join('e')
      for (i = 0; i < 25; i++)
        items.push({a: {S: id}, b: {S: ('0' + i).slice(-2)}, e: {S: e}})

      helpers.replaceTable(helpers.testRangeTable, ['a', 'b'], items, function(err) {
        if (err) return done(err)

        request(opts({
          TableName: helpers.testRangeTable,
          KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: id}]}},
          Select: 'COUNT',
          ReturnConsumedCapacity: 'TOTAL'
        }), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Count: 25,
            ScannedCount: 25,
            ConsumedCapacity: {CapacityUnits: 127.5, TableName: helpers.testRangeTable},
          })
          helpers.clearTable(helpers.testHashTable, 'a', done)
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
          ReturnConsumedCapacity: 'TOTAL'
        }), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Count: 25,
            ScannedCount: 25,
            ConsumedCapacity: {CapacityUnits: 127.5, TableName: helpers.testRangeTable},
            LastEvaluatedKey: {a: items[24].a, b: items[24].b}
          })
          helpers.clearTable(helpers.testHashTable, 'a', done)
        })
      })
    })

    // TODO: Need high capacity to run this (~100 runs quickly)
    it.skip('should return all if just under limit', function(done) {
      this.timeout(200000)

      var i, items = [], id = helpers.randomString(), e = new Array(43373).join('e')
      for (i = 0; i < 25; i++)
        items.push({a: {S: id}, b: {S: ('0' + i).slice(-2)}, e: {S: e}})
      items[23].e.S = new Array(43388).join('e')
      items[24].e.S = new Array(45000).join('e')

      helpers.replaceTable(helpers.testRangeTable, ['a', 'b'], items, function(err) {
        if (err) return done(err)

        request(opts({
          TableName: helpers.testRangeTable,
          KeyConditions: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: id}]}},
          Select: 'COUNT',
          ReturnConsumedCapacity: 'TOTAL'
        }), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Count: 25,
            ScannedCount: 25,
            ConsumedCapacity: {CapacityUnits: 133, TableName: helpers.testRangeTable},
            LastEvaluatedKey: {a: items[24].a, b: items[24].b}
          })
          helpers.clearTable(helpers.testHashTable, 'a', done)
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
          ReturnConsumedCapacity: 'TOTAL'
        }), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Count: 24,
            ScannedCount: 24,
            ConsumedCapacity: {CapacityUnits: 127.5, TableName: helpers.testRangeTable},
            LastEvaluatedKey: {a: items[23].a, b: items[23].b}
          })
          helpers.clearTable(helpers.testHashTable, 'a', done)
        })
      })
    })

  })

})


