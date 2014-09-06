var helpers = require('./helpers'),
    should = require('should')

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
      assertType('ExclusiveStartKey', 'Map', done)
    })

    it('should return SerializationException when ExclusiveStartKey.Attr is not a struct', function(done) {
      assertType('ExclusiveStartKey.Attr', 'Structure', done)
    })

    it('should return SerializationException when ExclusiveStartKey.Attr.S is not a string', function(done) {
      assertType('ExclusiveStartKey.Attr.S', 'String', done)
    })

    it('should return SerializationException when ExclusiveStartKey.Attr.B is not a blob', function(done) {
      assertType('ExclusiveStartKey.Attr.B', 'Blob', done)
    })

    it('should return SerializationException when ExclusiveStartKey.Attr.N is not a string', function(done) {
      assertType('ExclusiveStartKey.Attr.N', 'String', done)
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

    it('should return SerializationException when TotalSegments is not an integer', function(done) {
      assertType('TotalSegments', 'Integer', done)
    })

    it('should return SerializationException when ScanFilter is not a map', function(done) {
      assertType('ScanFilter', 'Map', done)
    })

    it('should return SerializationException when ScanFilter.Attr is not a struct', function(done) {
      assertType('ScanFilter.Attr', 'Structure', done)
    })

    it('should return SerializationException when ScanFilter.Attr.ComparisonOperator is not a string', function(done) {
      assertType('ScanFilter.Attr.ComparisonOperator', 'String', done)
    })

    it('should return SerializationException when ScanFilter.Attr.AttributeValueList is not a list', function(done) {
      assertType('ScanFilter.Attr.AttributeValueList', 'List', done)
    })

    it('should return SerializationException when ScanFilter.Attr.AttributeValueList.0 is not a struct', function(done) {
      assertType('ScanFilter.Attr.AttributeValueList.0', 'Structure', done)
    })

    it('should return SerializationException when ScanFilter.Attr.AttributeValueList.0.S is not a string', function(done) {
      assertType('ScanFilter.Attr.AttributeValueList.0.S', 'String', done)
    })

    it('should return SerializationException when ScanFilter.Attr.AttributeValueList.0.B is not a blob', function(done) {
      assertType('ScanFilter.Attr.AttributeValueList.0.B', 'Blob', done)
    })

    it('should return SerializationException when ScanFilter.Attr.AttributeValueList.0.N is not a string', function(done) {
      assertType('ScanFilter.Attr.AttributeValueList.0.N', 'String', done)
    })

    it('should return SerializationException when ScanFilter.Attr.AttributeValueList.0.SS is not a list', function(done) {
      assertType('ScanFilter.Attr.AttributeValueList.0.SS', 'List', done)
    })

    it('should return SerializationException when ScanFilter.Attr.AttributeValueList.0.SS.0 is not a string', function(done) {
      assertType('ScanFilter.Attr.AttributeValueList.0.SS.0', 'String', done)
    })

    it('should return SerializationException when ScanFilter.Attr.AttributeValueList.0.NS is not a list', function(done) {
      assertType('ScanFilter.Attr.AttributeValueList.0.NS', 'List', done)
    })

    it('should return SerializationException when ScanFilter.Attr.AttributeValueList.0.NS.0 is not a string', function(done) {
      assertType('ScanFilter.Attr.AttributeValueList.0.NS.0', 'String', done)
    })

    it('should return SerializationException when ScanFilter.Attr.AttributeValueList.0.BS is not a list', function(done) {
      assertType('ScanFilter.Attr.AttributeValueList.0.BS', 'List', done)
    })

    it('should return SerializationException when ScanFilter.Attr.AttributeValueList.0.BS.0 is not a blob', function(done) {
      assertType('ScanFilter.Attr.AttributeValueList.0.BS.0', 'Blob', done)
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

    it('should return ValidationException for incorrect attributes', function(done) {
      assertValidation({TableName: 'abc;', ReturnConsumedCapacity: 'hi', AttributesToGet: [],
        Segment: -1, TotalSegments: -1, Select: 'hi', Limit: -1, ScanFilter: {a: {}, b: {ComparisonOperator: ''}}},
        '9 validation errors detected: ' +
        'Value \'-1\' at \'limit\' failed to satisfy constraint: ' +
        'Member must have value greater than or equal to 1; ' +
        'Value \'-1\' at \'totalSegments\' failed to satisfy constraint: ' +
        'Member must have value greater than or equal to 1; ' +
        'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [INDEXES, TOTAL, NONE]; ' +
        'Value \'[]\' at \'attributesToGet\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1; ' +
        'Value \'hi\' at \'select\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]; ' +
        'Value \'abc;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'-1\' at \'segment\' failed to satisfy constraint: ' +
        'Member must have value greater than or equal to 0; ' +
        'Value \'\' at \'scanFilter.b.member.comparisonOperator\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [IN, NULL, BETWEEN, LT, NOT_CONTAINS, EQ, GT, NOT_NULL, NE, LE, BEGINS_WITH, GE, CONTAINS]; ' +
        'Value null at \'scanFilter.a.member.comparisonOperator\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for incorrect number of filter arguments', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {
          a: {ComparisonOperator: 'EQ'},
          b: {ComparisonOperator: 'NULL'},
          c: {ComparisonOperator: 'NULL'},
        }},
        'One or more parameter values were invalid: Invalid number of argument(s) for the EQ ComparisonOperator', done)
    })

    it('should return ValidationException for bad key type', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [{}]},
          b: {ComparisonOperator: 'NULL'},
          c: {ComparisonOperator: 'NULL'},
        }},
        'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException for bad key type', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [{a: ''}]},
          b: {ComparisonOperator: 'NULL'},
          c: {ComparisonOperator: 'NULL'},
        }},
        'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException for empty key', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: ''}]},
          b: {ComparisonOperator: 'NULL'},
          c: {ComparisonOperator: 'NULL'},
        }},
        'One or more parameter values were invalid: An AttributeValue may not contain an empty string.', done)
    })

    it('should return empty response if key has incorrect numeric type', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [{N: 'b'}]},
          b: {ComparisonOperator: 'NULL'},
          c: {ComparisonOperator: 'NULL'},
        }},
        'The parameter cannot be converted to a numeric value: b', done)
    })

    it('should return ValidationException for too many filter args', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: 'a'}, {S: 'a'}]},
          b: {ComparisonOperator: 'NULL'},
          c: {ComparisonOperator: 'NULL'},
        }},
        'One or more parameter values were invalid: Invalid number of argument(s) for the EQ ComparisonOperator', done)
    })

    it('should return ResourceNotFoundException for EQ on type SS when table does not exist', function(done) {
      assertNotFound({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{SS: ['a']}]}}},
        'Requested resource not found', done)
    })

    it('should return ValidationException for 1 arg to NULL', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'NULL', AttributeValueList: [{S: 'a'}]}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the NULL ComparisonOperator', done)
    })

    it('should return ValidationException for 1 arg to NOT_NULL', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'NOT_NULL', AttributeValueList: [{S: 'a'}]}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the NOT_NULL ComparisonOperator', done)
    })

    it('should return ValidationException for 0 args to NE', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'NE'}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the NE ComparisonOperator', done)
    })

    it('should return ValidationException for 2 args to NE', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'NE', AttributeValueList: [{S: 'a'}, {S: 'a'}]}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the NE ComparisonOperator', done)
    })

    it('should return ValidationException for 0 args to LE', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'LE'}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the LE ComparisonOperator', done)
    })

    it('should return ValidationException for 2 args to LE', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'LE', AttributeValueList: [{S: 'a'}, {S: 'a'}]}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the LE ComparisonOperator', done)
    })

    it('should return ValidationException for LE on type SS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'LE', AttributeValueList: [{SS: ['a']}]}}},
        'One or more parameter values were invalid: ComparisonOperator LE is not valid for SS AttributeValue type', done)
    })

    it('should return ValidationException for LE on type NS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'LE', AttributeValueList: [{NS: ['1']}]}}},
        'One or more parameter values were invalid: ComparisonOperator LE is not valid for NS AttributeValue type', done)
    })

    it('should return ValidationException for LE on type BS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'LE', AttributeValueList: [{BS: ['abcd']}]}}},
        'One or more parameter values were invalid: ComparisonOperator LE is not valid for BS AttributeValue type', done)
    })

    it('should return ValidationException for 0 args to LT', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'LT'}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the LT ComparisonOperator', done)
    })

    it('should return ValidationException for 2 args to LT', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'LT', AttributeValueList: [{S: 'a'}, {S: 'a'}]}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the LT ComparisonOperator', done)
    })

    it('should return ValidationException for LT on type SS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'LT', AttributeValueList: [{SS: ['a']}]}}},
        'One or more parameter values were invalid: ComparisonOperator LT is not valid for SS AttributeValue type', done)
    })

    it('should return ValidationException for LT on type NS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'LT', AttributeValueList: [{NS: ['1']}]}}},
        'One or more parameter values were invalid: ComparisonOperator LT is not valid for NS AttributeValue type', done)
    })

    it('should return ValidationException for LT on type BS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'LT', AttributeValueList: [{BS: ['abcd']}]}}},
        'One or more parameter values were invalid: ComparisonOperator LT is not valid for BS AttributeValue type', done)
    })

    it('should return ValidationException for 0 args to GE', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'GE'}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the GE ComparisonOperator', done)
    })

    it('should return ValidationException for 2 args to GE', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'GE', AttributeValueList: [{S: 'a'}, {S: 'a'}]}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the GE ComparisonOperator', done)
    })

    it('should return ValidationException for GE on type SS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'GE', AttributeValueList: [{SS: ['a']}]}}},
        'One or more parameter values were invalid: ComparisonOperator GE is not valid for SS AttributeValue type', done)
    })

    it('should return ValidationException for GE on type NS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'GE', AttributeValueList: [{NS: ['1']}]}}},
        'One or more parameter values were invalid: ComparisonOperator GE is not valid for NS AttributeValue type', done)
    })

    it('should return ValidationException for GE on type BS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'GE', AttributeValueList: [{BS: ['abcd']}]}}},
        'One or more parameter values were invalid: ComparisonOperator GE is not valid for BS AttributeValue type', done)
    })

    it('should return ValidationException for 0 args to GT', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'GT'}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the GT ComparisonOperator', done)
    })

    it('should return ValidationException for 2 args to GT', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'GT', AttributeValueList: [{S: 'a'}, {S: 'a'}]}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the GT ComparisonOperator', done)
    })

    it('should return ValidationException for GT on type SS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'GT', AttributeValueList: [{SS: ['a']}]}}},
        'One or more parameter values were invalid: ComparisonOperator GT is not valid for SS AttributeValue type', done)
    })

    it('should return ValidationException for GT on type NS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'GT', AttributeValueList: [{NS: ['1']}]}}},
        'One or more parameter values were invalid: ComparisonOperator GT is not valid for NS AttributeValue type', done)
    })

    it('should return ValidationException for GT on type BS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'GT', AttributeValueList: [{BS: ['abcd']}]}}},
        'One or more parameter values were invalid: ComparisonOperator GT is not valid for BS AttributeValue type', done)
    })

    it('should return ValidationException for 0 args to CONTAINS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'CONTAINS'}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the CONTAINS ComparisonOperator', done)
    })

    it('should return ValidationException for 2 args to CONTAINS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'CONTAINS', AttributeValueList: [{S: 'a'}, {S: 'a'}]}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the CONTAINS ComparisonOperator', done)
    })

    it('should return ValidationException for CONTAINS on type SS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'CONTAINS', AttributeValueList: [{SS: ['a']}]}}},
        'One or more parameter values were invalid: ComparisonOperator CONTAINS is not valid for SS AttributeValue type', done)
    })

    it('should return ValidationException for CONTAINS on type NS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'CONTAINS', AttributeValueList: [{NS: ['1']}]}}},
        'One or more parameter values were invalid: ComparisonOperator CONTAINS is not valid for NS AttributeValue type', done)
    })

    it('should return ValidationException for CONTAINS on type BS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'CONTAINS', AttributeValueList: [{BS: ['abcd']}]}}},
        'One or more parameter values were invalid: ComparisonOperator CONTAINS is not valid for BS AttributeValue type', done)
    })

    it('should return ValidationException for 0 args to NOT_CONTAINS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'NOT_CONTAINS'}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the NOT_CONTAINS ComparisonOperator', done)
    })

    it('should return ValidationException for 2 args to NOT_CONTAINS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'NOT_CONTAINS', AttributeValueList: [{S: 'a'}, {S: 'a'}]}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the NOT_CONTAINS ComparisonOperator', done)
    })

    it('should return ValidationException for NOT_CONTAINS on type SS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'NOT_CONTAINS', AttributeValueList: [{SS: ['a']}]}}},
        'One or more parameter values were invalid: ComparisonOperator NOT_CONTAINS is not valid for SS AttributeValue type', done)
    })

    it('should return ValidationException for NOT_CONTAINS on type NS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'NOT_CONTAINS', AttributeValueList: [{NS: ['1']}]}}},
        'One or more parameter values were invalid: ComparisonOperator NOT_CONTAINS is not valid for NS AttributeValue type', done)
    })

    it('should return ValidationException for NOT_CONTAINS on type BS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'NOT_CONTAINS', AttributeValueList: [{BS: ['abcd']}]}}},
        'One or more parameter values were invalid: ComparisonOperator NOT_CONTAINS is not valid for BS AttributeValue type', done)
    })

    it('should return ValidationException for 0 args to BEGINS_WITH', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'BEGINS_WITH'}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the BEGINS_WITH ComparisonOperator', done)
    })

    it('should return ValidationException for 2 args to BEGINS_WITH', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'BEGINS_WITH', AttributeValueList: [{S: 'a'}, {S: 'a'}]}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the BEGINS_WITH ComparisonOperator', done)
    })

    it('should return ValidationException for BEGINS_WITH on type N', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'BEGINS_WITH', AttributeValueList: [{N: '1'}]}}},
        'One or more parameter values were invalid: ComparisonOperator BEGINS_WITH is not valid for N AttributeValue type', done)
    })

    it('should return ValidationException for BEGINS_WITH on type SS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'BEGINS_WITH', AttributeValueList: [{SS: ['a']}]}}},
        'One or more parameter values were invalid: ComparisonOperator BEGINS_WITH is not valid for SS AttributeValue type', done)
    })

    it('should return ValidationException for BEGINS_WITH on type NS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'BEGINS_WITH', AttributeValueList: [{NS: ['1']}]}}},
        'One or more parameter values were invalid: ComparisonOperator BEGINS_WITH is not valid for NS AttributeValue type', done)
    })

    it('should return ValidationException for BEGINS_WITH on type BS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'BEGINS_WITH', AttributeValueList: [{BS: ['abcd']}]}}},
        'One or more parameter values were invalid: ComparisonOperator BEGINS_WITH is not valid for BS AttributeValue type', done)
    })

    it('should return ValidationException for 0 args to IN', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'IN'}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the IN ComparisonOperator', done)
    })

    it('should return ValidationException for IN on type SS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'IN', AttributeValueList: [{SS: ['a']}]}}},
        'One or more parameter values were invalid: ComparisonOperator IN is not valid for SS AttributeValue type', done)
    })

    it('should return ValidationException for IN on type NS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'IN', AttributeValueList: [{NS: ['1']}]}}},
        'One or more parameter values were invalid: ComparisonOperator IN is not valid for NS AttributeValue type', done)
    })

    it('should return ValidationException for IN on type BS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'IN', AttributeValueList: [{BS: ['abcd']}]}}},
        'One or more parameter values were invalid: ComparisonOperator IN is not valid for BS AttributeValue type', done)
    })

    it('should return ValidationException for 0 args to BETWEEN', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'BETWEEN'}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the BETWEEN ComparisonOperator', done)
    })

    it('should return ValidationException for 3 args to BETWEEN', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'BETWEEN', AttributeValueList: [{S: 'a'}, {S: 'a'}, {S: 'a'}]}}},
        'One or more parameter values were invalid: Invalid number of argument(s) for the BETWEEN ComparisonOperator', done)
    })

    it('should return ValidationException for BETWEEN on type SS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'BETWEEN', AttributeValueList: [{S: 'a'}, {SS: ['a']}]}}},
        'One or more parameter values were invalid: ComparisonOperator BETWEEN is not valid for SS AttributeValue type', done)
    })

    it('should return ValidationException for BETWEEN on type NS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'BETWEEN', AttributeValueList: [{S: 'a'}, {NS: ['1']}]}}},
        'One or more parameter values were invalid: ComparisonOperator BETWEEN is not valid for NS AttributeValue type', done)
    })

    it('should return ValidationException for BETWEEN on type BS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'BETWEEN', AttributeValueList: [{S: 'a'}, {BS: ['abcd']}]}}},
        'One or more parameter values were invalid: ComparisonOperator BETWEEN is not valid for BS AttributeValue type', done)
    })

    it('should return ValidationException for empty object ExclusiveStartKey', function(done) {
      assertValidation({
        TableName: 'abc',
        ExclusiveStartKey: {a: {}},
        ScanFilter: {a: {ComparisonOperator: 'NULL'}}},
        'The provided starting key is invalid: ' +
        'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException for wrong attribute ExclusiveStartKey', function(done) {
      assertValidation({
        TableName: 'abc',
        ExclusiveStartKey: {a: {a: ''}},
        ScanFilter: {a: {ComparisonOperator: 'NULL'}}},
        'The provided starting key is invalid: ' +
        'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException for empty string ExclusiveStartKey', function(done) {
      assertValidation({
        TableName: 'abc',
        ExclusiveStartKey: {a: {S: ''}},
        ScanFilter: {a: {ComparisonOperator: 'NULL'}}},
        'The provided starting key is invalid: ' +
        'One or more parameter values were invalid: An AttributeValue may not contain an empty string.', done)
    })

    it('should return ValidationException for empty binary', function(done) {
      assertValidation({
        TableName: 'abc',
        ExclusiveStartKey: {a: {B: ''}},
        ScanFilter: {a: {ComparisonOperator: 'NULL'}}},
        'The provided starting key is invalid: ' +
        'One or more parameter values were invalid: An AttributeValue may not contain a null or empty binary type.', done)
    })

    // Somehow allows set types for keys
    it('should return ValidationException for empty set key', function(done) {
      assertValidation({
        TableName: 'abc',
        ExclusiveStartKey: {a: {SS: []}},
        ScanFilter: {a: {ComparisonOperator: 'NULL'}}},
        'The provided starting key is invalid: ' +
        'One or more parameter values were invalid: An string set  may not be empty', done)
    })

    it('should return ValidationException for empty string in set', function(done) {
      assertValidation({
        TableName: 'abc',
        ExclusiveStartKey: {a: {SS: ['a', '']}},
        ScanFilter: {a: {ComparisonOperator: 'NULL'}}},
        'The provided starting key is invalid: ' +
        'One or more parameter values were invalid: An string set may not have a empty string as a member', done)
    })

    it('should return ValidationException for empty binary in set', function(done) {
      assertValidation({
        TableName: 'abc',
        ExclusiveStartKey: {a: {BS: ['aaaa', '']}},
        ScanFilter: {a: {ComparisonOperator: 'NULL'}}},
        'The provided starting key is invalid: ' +
        'One or more parameter values were invalid: Binary sets may not contain null or empty values', done)
    })

    it('should return ValidationException for multiple types', function(done) {
      assertValidation({
        TableName: 'abc',
        ExclusiveStartKey: {a: {S: 'a', N: '1'}},
        ScanFilter: {a: {ComparisonOperator: 'NULL'}}},
        'The provided starting key is invalid: ' +
        'Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes', done)
    })

    it('should return ValidationException if key has empty numeric type', function(done) {
      assertValidation({
        TableName: 'abc',
        ExclusiveStartKey: {a: {N: ''}},
        ScanFilter: {a: {ComparisonOperator: 'NULL'}}},
        'The provided starting key is invalid: ' +
        'The parameter cannot be converted to a numeric value', done)
    })

    it('should return ValidationException if key has incorrect numeric type', function(done) {
      assertValidation({
        TableName: 'abc',
        ExclusiveStartKey: {a: {N: 'b'}},
        ScanFilter: {a: {ComparisonOperator: 'NULL'}}},
        'The provided starting key is invalid: ' +
        'The parameter cannot be converted to a numeric value: b', done)
    })

    it('should return ValidationException if key has empty numeric type in set', function(done) {
      assertValidation({
        TableName: 'abc',
        ExclusiveStartKey: {a: {NS: ['1', '']}},
        ScanFilter: {a: {ComparisonOperator: 'NULL'}}},
        'The provided starting key is invalid: ' +
        'The parameter cannot be converted to a numeric value', done)
    })

    it('should return ValidationException if key has incorrect numeric type in set', function(done) {
      assertValidation({
        TableName: 'abc',
        ExclusiveStartKey: {a: {NS: ['1', 'b', 'a']}},
        ScanFilter: {a: {ComparisonOperator: 'NULL'}}},
        'The provided starting key is invalid: ' +
        'The parameter cannot be converted to a numeric value: b', done)
    })

    it('should return ValidationException duplicate values in AttributesToGet', function(done) {
      assertValidation({TableName: 'abc', ScanFilter: {a: {ComparisonOperator: 'NULL'}}, AttributesToGet: ['a', 'a']},
        'One or more parameter values were invalid: Duplicate value in attribute name: a', done)
    })

    it('should return ValidationException for empty ExclusiveStartKey', function(done) {
      assertValidation({
        TableName: helpers.testHashTable,
        ExclusiveStartKey: {}},
        'The provided starting key is invalid: The provided key element does not match the schema', done)
    })

    it('should return ValidationException for ExclusiveStartKey with incorrect schema', function(done) {
      assertValidation({
        TableName: helpers.testRangeTable,
        ExclusiveStartKey: {a: {S: 'b'}}},
        'The provided starting key is invalid: The provided key element does not match the schema', done)
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
            ExclusiveStartKey: {a: res.body.Items[0].a}},
            'The provided starting key is invalid: ' +
            'Invalid ExclusiveStartKey. Please use ExclusiveStartKey with correct Segment. ' +
            'TotalSegments: 2 Segment: 0', done)
        })
      })
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
          res.body.Items.should.includeEql(item)
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [item.a]}
        }}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Items.should.eql([item])
          res.body.Count.should.equal(1)
          res.body.ScannedCount.should.be.above(0)
          done()
        })
      })
    })

    it('should return empty if no match', function(done) {
      var item = {a: {S: helpers.randomString()}}
      request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: helpers.randomString()}]}
        }}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          /* jshint -W030 */
          res.body.Items.should.be.empty
          res.body.Count.should.equal(0)
          res.body.ScannedCount.should.be.above(0)
          done()
        })
      })
    })

    // Upper bound seems to be at least greater than 1000000!!!
    it('should allow scans at least for 100000 args to IN', function(done) {
      this.timeout(100000)
      var attrValList = [], i
      for (i = 0; i < 100000; i++) attrValList.push({S: 'a'})
      request(opts({TableName: helpers.testHashTable, ScanFilter: {
        a: {ComparisonOperator: 'IN', AttributeValueList: attrValList}
      }}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        done()
      })
    })

    it('should scan by a non-id property (type N)', function(done) {
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'EQ', AttributeValueList: [item.b]}
        }}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Items.should.includeEql(item)
          res.body.Items.should.includeEql(item2)
          res.body.Items.should.have.length(2)
          res.body.Count.should.equal(2)
          done()
        })
      })
    })

    it('should scan by multiple properties', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {N: helpers.randomString()}, c: {N: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: item.b, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: item.b, c: {N: helpers.randomString()}},
          batchReq = {RequestItems: {}}
      batchReq.RequestItems[helpers.testHashTable] = [
        {PutRequest: {Item: item}},
        {PutRequest: {Item: item2}},
        {PutRequest: {Item: item3}},
      ]
      request(helpers.opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'EQ', AttributeValueList: [item.b]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Items.should.includeEql(item)
          res.body.Items.should.includeEql(item2)
          res.body.Items.should.have.length(2)
          res.body.Count.should.equal(2)
          done()
        })
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'EQ', AttributeValueList: [item.b]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Items.should.includeEql(item)
          res.body.Items.should.includeEql(item2)
          res.body.Items.should.have.length(2)
          res.body.Count.should.equal(2)
          done()
        })
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'EQ', AttributeValueList: [{SS: ['b', 'a']}]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Items.should.includeEql(item)
          res.body.Items.should.includeEql(item2)
          res.body.Items.should.have.length(2)
          res.body.Count.should.equal(2)
          done()
        })
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'EQ', AttributeValueList: [{NS: ['2', '1']}]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Items.should.includeEql(item)
          res.body.Items.should.includeEql(item2)
          res.body.Items.should.have.length(2)
          res.body.Count.should.equal(2)
          done()
        })
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'EQ', AttributeValueList: [{BS: ['abcd', 'Yg==']}]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Items.should.includeEql(item)
          res.body.Items.should.includeEql(item2)
          res.body.Items.should.have.length(2)
          res.body.Count.should.equal(2)
          done()
        })
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'EQ', AttributeValueList: [item.b]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Items.should.eql([item])
          res.body.Count.should.equal(1)
          done()
        })
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'NE', AttributeValueList: [item.b]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.body.Items.should.includeEql(item2)
          res.body.Items.should.includeEql(item3)
          res.body.Items.should.have.length(2)
          res.body.Count.should.equal(2)
          done()
        })
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'NE', AttributeValueList: [{SS: ['b', 'a']}]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Items.should.includeEql(item3)
          res.body.Items.should.have.length(1)
          res.body.Count.should.equal(1)
          done()
        })
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'NE', AttributeValueList: [{NS: ['2', '1']}]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Items.should.includeEql(item3)
          res.body.Items.should.have.length(1)
          res.body.Count.should.equal(1)
          done()
        })
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'NE', AttributeValueList: [{BS: ['abcd', 'Yg==']}]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Items.should.includeEql(item3)
          res.body.Items.should.have.length(1)
          res.body.Count.should.equal(1)
          done()
        })
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'LE', AttributeValueList: [item.b]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.body.Items.should.includeEql(item)
          res.body.Items.should.includeEql(item2)
          res.body.Items.should.includeEql(item3)
          res.body.Items.should.includeEql(item5)
          res.body.Items.should.have.length(4)
          res.body.Count.should.equal(4)
          done()
        })
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'LE', AttributeValueList: [item.b]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.body.Items.should.includeEql(item)
          res.body.Items.should.includeEql(item2)
          res.body.Items.should.includeEql(item3)
          res.body.Items.should.includeEql(item5)
          res.body.Items.should.have.length(4)
          res.body.Count.should.equal(4)
          done()
        })
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'LE', AttributeValueList: [item.b]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.body.Items.should.includeEql(item)
          res.body.Items.should.includeEql(item3)
          res.body.Items.should.includeEql(item5)
          res.body.Items.should.have.length(3)
          res.body.Count.should.equal(3)
          done()
        })
      })
    })

    it('should scan by LE on type B', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {B: new Buffer('abd').toString('base64')}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {B: new Buffer('abc\xff').toString('base64')}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: new Buffer('abc').toString('base64')}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {B: new Buffer('abd\x00').toString('base64')}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {B: new Buffer('ab').toString('base64')}, c: item.c},
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
          b: {ComparisonOperator: 'LE', AttributeValueList: [item.b]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.body.Items.should.includeEql(item)
          res.body.Items.should.includeEql(item2)
          res.body.Items.should.includeEql(item3)
          res.body.Items.should.includeEql(item5)
          res.body.Items.should.have.length(4)
          res.body.Count.should.equal(4)
          done()
        })
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'LT', AttributeValueList: [item.b]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.body.Items.should.includeEql(item2)
          res.body.Items.should.includeEql(item3)
          res.body.Items.should.includeEql(item5)
          res.body.Items.should.have.length(3)
          res.body.Count.should.equal(3)
          done()
        })
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'LT', AttributeValueList: [item.b]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.body.Items.should.includeEql(item2)
          res.body.Items.should.includeEql(item3)
          res.body.Items.should.includeEql(item5)
          res.body.Items.should.have.length(3)
          res.body.Count.should.equal(3)
          done()
        })
      })
    })

    it('should scan by LT on type B', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {B: new Buffer('abd').toString('base64')}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {B: new Buffer('abc\xff').toString('base64')}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: new Buffer('abc').toString('base64')}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {B: new Buffer('abd\x00').toString('base64')}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {B: new Buffer('ab').toString('base64')}, c: item.c},
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
          b: {ComparisonOperator: 'LT', AttributeValueList: [item.b]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.body.Items.should.includeEql(item2)
          res.body.Items.should.includeEql(item3)
          res.body.Items.should.includeEql(item5)
          res.body.Items.should.have.length(3)
          res.body.Count.should.equal(3)
          done()
        })
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'GE', AttributeValueList: [item3.b]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.body.Items.should.includeEql(item)
          res.body.Items.should.includeEql(item2)
          res.body.Items.should.includeEql(item3)
          res.body.Items.should.includeEql(item4)
          res.body.Items.should.have.length(4)
          res.body.Count.should.equal(4)
          done()
        })
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'GE', AttributeValueList: [item2.b]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.body.Items.should.includeEql(item)
          res.body.Items.should.includeEql(item2)
          res.body.Items.should.includeEql(item4)
          res.body.Items.should.have.length(3)
          res.body.Count.should.equal(3)
          done()
        })
      })
    })

    it('should scan by GE on type B', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {B: new Buffer('abd').toString('base64')}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {B: new Buffer('abc\xff').toString('base64')}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: new Buffer('abc').toString('base64')}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {B: new Buffer('abd\x00').toString('base64')}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {B: new Buffer('ab').toString('base64')}, c: item.c},
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
          b: {ComparisonOperator: 'GE', AttributeValueList: [item3.b]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.body.Items.should.includeEql(item)
          res.body.Items.should.includeEql(item2)
          res.body.Items.should.includeEql(item3)
          res.body.Items.should.includeEql(item4)
          res.body.Items.should.have.length(4)
          res.body.Count.should.equal(4)
          done()
        })
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'GT', AttributeValueList: [item3.b]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.body.Items.should.includeEql(item)
          res.body.Items.should.includeEql(item2)
          res.body.Items.should.includeEql(item4)
          res.body.Items.should.have.length(3)
          res.body.Count.should.equal(3)
          done()
        })
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'GT', AttributeValueList: [item2.b]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.body.Items.should.includeEql(item)
          res.body.Items.should.includeEql(item4)
          res.body.Items.should.have.length(2)
          res.body.Count.should.equal(2)
          done()
        })
      })
    })

    it('should scan by GT on type B', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {B: new Buffer('abd').toString('base64')}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {B: new Buffer('abc\xff').toString('base64')}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: new Buffer('abc').toString('base64')}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {B: new Buffer('abd\x00').toString('base64')}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {B: new Buffer('ab').toString('base64')}, c: item.c},
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
          b: {ComparisonOperator: 'GT', AttributeValueList: [item3.b]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.body.Items.should.includeEql(item)
          res.body.Items.should.includeEql(item2)
          res.body.Items.should.includeEql(item4)
          res.body.Items.should.have.length(3)
          res.body.Count.should.equal(3)
          done()
        })
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'NOT_NULL'},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.body.Items.should.includeEql(item)
          res.body.Items.should.includeEql(item2)
          res.body.Items.should.have.length(2)
          res.body.Count.should.equal(2)
          done()
        })
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'NULL'},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.body.Items.should.includeEql(item)
          res.body.Items.should.includeEql(item2)
          res.body.Items.should.have.length(2)
          res.body.Count.should.equal(2)
          done()
        })
      })
    })

    it('should scan by CONTAINS on type S', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'abdef'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {SS: ['abd', 'bde']}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: new Buffer('abdef').toString('base64')}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {BS: ['abcd', new Buffer('bde').toString('base64')]}, c: item.c},
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'CONTAINS', AttributeValueList: [item5.b]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.body.Items.should.includeEql(item)
          res.body.Items.should.includeEql(item2)
          res.body.Items.should.includeEql(item5)
          res.body.Items.should.have.length(3)
          res.body.Count.should.equal(3)
          done()
        })
      })
    })

    it('should scan by CONTAINS on type N', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {N: '1234'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {NS: ['123', '234']}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: new Buffer('1234').toString('base64')}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {BS: [new Buffer('234').toString('base64')]}, c: item.c},
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'CONTAINS', AttributeValueList: [{N: '234'}]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.body.Items.should.have.lengthOf(1)
          res.body.Items[0].a.should.eql(item2.a)
          res.body.Count.should.equal(1)
          done()
        })
      })
    })

    it('should scan by CONTAINS on type B', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'abdef'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {SS: ['abd', 'bde']}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: new Buffer('abdef').toString('base64')}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {BS: [new Buffer('bde').toString('base64'), 'abcd']}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {B: new Buffer('bde').toString('base64')}, c: item.c},
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'CONTAINS', AttributeValueList: [item5.b]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.body.Items.should.includeEql(item3)
          res.body.Items.should.includeEql(item4)
          res.body.Items.should.includeEql(item5)
          res.body.Items.should.have.length(3)
          res.body.Count.should.equal(3)
          done()
        })
      })
    })

    it('should scan by NOT_CONTAINS on type S', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'abdef'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {SS: ['abd', 'bde']}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: new Buffer('abdef').toString('base64')}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {BS: [new Buffer('bde').toString('base64'), 'abcd']}, c: item.c},
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'NOT_CONTAINS', AttributeValueList: [item5.b]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.body.Items.should.includeEql(item3)
          res.body.Items.should.includeEql(item4)
          res.body.Items.should.includeEql(item6)
          res.body.Items.should.have.length(3)
          res.body.Count.should.equal(3)
          done()
        })
      })
    })

    it('should scan by NOT_CONTAINS on type N', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {N: '1234'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {NS: ['123', '234']}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: new Buffer('1234').toString('base64')}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {BS: [new Buffer('234').toString('base64')]}, c: item.c},
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'NOT_CONTAINS', AttributeValueList: [{N: '234'}]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.body.Items.should.includeEql(item)
          res.body.Items.should.includeEql(item3)
          res.body.Items.should.includeEql(item4)
          res.body.Items.should.includeEql(item5)
          res.body.Items.should.have.length(4)
          res.body.Count.should.equal(4)
          done()
        })
      })
    })

    it('should scan by NOT_CONTAINS on type B', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'abdef'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {SS: ['abd', 'bde']}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: new Buffer('abdef').toString('base64')}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {BS: [new Buffer('bde').toString('base64'), 'abcd']}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {B: new Buffer('bde').toString('base64')}, c: item.c},
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'NOT_CONTAINS', AttributeValueList: [item5.b]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.body.Items.should.includeEql(item)
          res.body.Items.should.includeEql(item2)
          res.body.Items.should.includeEql(item6)
          res.body.Items.should.have.length(3)
          res.body.Count.should.equal(3)
          done()
        })
      })
    })

    it('should scan by BEGINS_WITH on type S', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'abdef'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {SS: ['abd', 'bde']}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: new Buffer('abdef').toString('base64')}, c: item.c},
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'BEGINS_WITH', AttributeValueList: [item5.b]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.body.Items.should.includeEql(item)
          res.body.Items.should.includeEql(item5)
          res.body.Items.should.have.length(2)
          res.body.Count.should.equal(2)
          done()
        })
      })
    })

    it('should scan by BEGINS_WITH on type B', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'abdef'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {SS: ['abd', 'bde']}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: new Buffer('abdef').toString('base64')}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {S: 'ab'}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {B: new Buffer('abd').toString('base64')}, c: item.c},
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
          b: {ComparisonOperator: 'BEGINS_WITH', AttributeValueList: [item5.b]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.body.Items.should.includeEql(item3)
          res.body.Items.should.includeEql(item5)
          res.body.Items.should.have.length(2)
          res.body.Count.should.equal(2)
          done()
        })
      })
    })

    it('should scan by IN on type S', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: 'abdef'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {SS: ['abd', 'bde']}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: new Buffer('abdef').toString('base64')}, c: item.c},
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'IN', AttributeValueList: [item5.b, item.b]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.body.Items.should.includeEql(item)
          res.body.Items.should.includeEql(item5)
          res.body.Items.should.have.length(2)
          res.body.Count.should.equal(2)
          done()
        })
      })
    })

    it('should scan by IN on type N', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: '1234'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {NS: ['1234']}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: new Buffer('1234').toString('base64')}, c: item.c},
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'IN', AttributeValueList: [item4.b, item5.b]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.body.Items.should.includeEql(item4)
          res.body.Items.should.includeEql(item5)
          res.body.Items.should.have.length(2)
          res.body.Count.should.equal(2)
          done()
        })
      })
    })

    it('should scan by IN on type B', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {S: '1234'}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {BS: [new Buffer('1234').toString('base64')]}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: new Buffer('1234').toString('base64')}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {N: '1234'}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {B: new Buffer('12345').toString('base64')}, c: item.c},
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
          b: {ComparisonOperator: 'IN', AttributeValueList: [item3.b, item5.b]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.body.Items.should.includeEql(item3)
          res.body.Items.should.includeEql(item5)
          res.body.Items.should.have.length(2)
          res.body.Count.should.equal(2)
          done()
        })
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'BETWEEN', AttributeValueList: [item2.b, item4.b]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.body.Items.should.includeEql(item2)
          res.body.Items.should.includeEql(item3)
          res.body.Items.should.includeEql(item4)
          res.body.Items.should.have.length(3)
          res.body.Count.should.equal(3)
          done()
        })
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          b: {ComparisonOperator: 'BETWEEN', AttributeValueList: [item2.b, item4.b]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.body.Items.should.includeEql(item2)
          res.body.Items.should.includeEql(item3)
          res.body.Items.should.includeEql(item4)
          res.body.Items.should.have.length(3)
          res.body.Count.should.equal(3)
          done()
        })
      })
    })

    it('should scan by BETWEEN on type B', function(done) {
      var item = {a: {S: helpers.randomString()}, b: {B: new Buffer('abc').toString('base64')}, c: {S: helpers.randomString()}},
          item2 = {a: {S: helpers.randomString()}, b: {B: new Buffer('abd').toString('base64')}, c: item.c},
          item3 = {a: {S: helpers.randomString()}, b: {B: new Buffer('abd\x00').toString('base64')}, c: item.c},
          item4 = {a: {S: helpers.randomString()}, b: {B: new Buffer('abe').toString('base64')}, c: item.c},
          item5 = {a: {S: helpers.randomString()}, b: {B: new Buffer('abe\x00').toString('base64')}, c: item.c},
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
          b: {ComparisonOperator: 'BETWEEN', AttributeValueList: [item2.b, item4.b]},
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }}), function(err, res) {
          if (err) return done(err)
          res.body.Items.should.includeEql(item2)
          res.body.Items.should.includeEql(item3)
          res.body.Items.should.includeEql(item4)
          res.body.Items.should.have.length(3)
          res.body.Count.should.equal(3)
          done()
        })
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
        request(opts({TableName: helpers.testHashTable, ScanFilter: {
          c: {ComparisonOperator: 'EQ', AttributeValueList: [item.c]},
        }, AttributesToGet: ['b', 'd']}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Items.should.includeEql({b: {S: 'b1'}, d: {S: 'd1'}})
          res.body.Items.should.includeEql({b: {S: 'b2'}})
          res.body.Items.should.includeEql({b: {S: 'b3'}, d: {S: 'd3'}})
          res.body.Items.should.have.length(3)
          res.body.Count.should.equal(3)
          done()
        })
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
        ScanFilter: {a: {ComparisonOperator: 'EQ', AttributeValueList: [{S: hashes[0]}]}}
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

        request(opts({TableName: helpers.testHashTable, Limit: 3}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.ScannedCount.should.equal(3)
          /* jshint -W030 */
          res.body.LastEvaluatedKey.a.S.should.not.be.empty
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
          /* jshint -W030 */
          res.body.LastEvaluatedKey.a.S.should.not.be.empty
          Object.keys(res.body.LastEvaluatedKey).should.have.length(1)
          done()
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

        request(opts({TableName: helpers.testHashTable, AttributesToGet: ['a'], Limit: 100000}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Count.should.equal(res.body.ScannedCount)
          should.not.exist(res.body.LastEvaluatedKey)
          for (var i = 0, lastIx = 0; i < res.body.Count; i++) {
            if (res.body.Items[i].a.S < 5) lastIx = i
          }
          var totalItems = res.body.Count
          request(opts({TableName: helpers.testHashTable, ScanFilter: scanFilter, Limit: lastIx}), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.Count.should.equal(4)
            /* jshint -W030 */
            res.body.LastEvaluatedKey.a.S.should.not.be.empty
            request(opts({TableName: helpers.testHashTable, ScanFilter: scanFilter, Limit: lastIx + 1}), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)
              res.body.Count.should.equal(5)
              /* jshint -W030 */
              res.body.LastEvaluatedKey.a.S.should.not.be.empty
              request(opts({TableName: helpers.testHashTable, ScanFilter: scanFilter, Limit: totalItems}), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)
                res.body.Count.should.equal(5)
                /* jshint -W030 */
                res.body.LastEvaluatedKey.a.S.should.not.be.empty
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

              res.body.Items.forEach(function(item) { firstHalf.should.includeEql(item) })

              request(opts({TableName: helpers.testHashTable, Segment: 1, TotalSegments: 4, ScanFilter: scanFilter}), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)

                res.body.Items.forEach(function(item) { firstHalf.should.includeEql(item) })

                request(opts({TableName: helpers.testHashTable, Segment: 2, TotalSegments: 4, ScanFilter: scanFilter}), function(err, res) {
                  if (err) return done(err)
                  res.statusCode.should.equal(200)

                  res.body.Items.forEach(function(item) { secondHalf.should.includeEql(item) })

                  request(opts({TableName: helpers.testHashTable, Segment: 3, TotalSegments: 4, ScanFilter: scanFilter}), function(err, res) {
                    if (err) return done(err)
                    res.statusCode.should.equal(200)

                    res.body.Items.forEach(function(item) { secondHalf.should.includeEql(item) })

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
          res.body.Items.should.includeEql(items[0])
          res.body.Items.should.includeEql(items[1])
          request(opts({TableName: helpers.testHashTable, Segment: 1, TotalSegments: 4096}), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.includeEql(items[2])
            request(opts({TableName: helpers.testHashTable, Segment: 4, TotalSegments: 4096}), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)
              res.body.Items.should.includeEql(items[3])
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
          res.body.Items.should.includeEql(items[0])
          request(opts({TableName: helpers.testHashNTable, Segment: 4095, TotalSegments: 4096}), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.Items.should.includeEql(items[1])
            done()
          })
        })
      })
    })

    // TODO: Need high capacity to run this
    it.skip('should return all if just under limit', function(done) {
      this.timeout(100000)

      var i, items = []
      for (i = 0; i < 25; i++)
        items.push({a: {S: ('0' + i).slice(-2)}})

      helpers.replaceTable(helpers.testHashTable, ['a'], items, function(err) {
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
              items[i].b = {S: b.slice(0, 43366)}
              items[i].c = {N: '12.3456'}
              items[i].d = {B: 'AQI='}
              items[i].e = {SS: ['a', 'bc']}
              items[i].f = {NS: ['1.23', '12.3']}
              items[i].g = {BS: ['AQI=', 'Ag==', 'AQ==']}
            } else if (i == 24) {
              items[i][bAttr] = {S: new Array(100).join('b')} // Last item doesn't matter
            } else {
              items[i][bAttr] = {S: b}
            }
          }

          helpers.replaceTable(helpers.testHashTable, ['a'], items, 10, function(err) {
            if (err) return done(err)

            request(opts({TableName: helpers.testHashTable, Select: 'COUNT', ReturnConsumedCapacity: 'TOTAL'}), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)
              res.body.ScannedCount.should.equal(25)
              res.body.Count.should.equal(25)
              res.body.ConsumedCapacity.CapacityUnits.should.equal(128)
              done()
            })
          })
        })
      })
    })

    // TODO: Need high capacity to run this
    it.skip('should return one less than all if just over limit', function(done) {
      this.timeout(100000)

      var i, items = []
      for (i = 0; i < 25; i++)
        items.push({a: {S: ('0' + i).slice(-2)}})

      helpers.replaceTable(helpers.testHashTable, ['a'], items, function(err) {
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
              items[i].b = {S: b.slice(0, 43367)}
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

          helpers.replaceTable(helpers.testHashTable, ['a'], items, 10, function(err) {
            if (err) return done(err)

            request(opts({TableName: helpers.testHashTable, Select: 'COUNT', ReturnConsumedCapacity: 'TOTAL'}), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)
              res.body.ScannedCount.should.equal(24)
              res.body.Count.should.equal(24)
              res.body.ConsumedCapacity.CapacityUnits.should.equal(127.5)
              done()
            })
          })
        })
      })
    })
  })

})


