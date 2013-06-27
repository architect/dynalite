var async = require('async'),
    helpers = require('./helpers'),
    should = require('should'),
    dynalite = require('..')

var target = 'Scan',
    request = helpers.request,
    opts = helpers.opts.bind(null, target),
    assertSerialization = helpers.assertSerialization.bind(null, target),
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
        'The paramater \'tableName\' is required but was not present in the request', done)
    })

    it('should return ValidationException for empty TableName', function(done) {
      assertValidation({TableName: ''},
        'TableName must be at least 3 characters long and at most 255 characters long', done)
    })

    it('should return ValidationException for short TableName', function(done) {
      assertValidation({TableName: 'a;'},
        'TableName must be at least 3 characters long and at most 255 characters long', done)
    })

    it('should return ValidationException for long TableName', function(done) {
      var name = '', i
      for (i = 0; i < 256; i++) name += 'a'
      assertValidation({TableName: name},
        'TableName must be at least 3 characters long and at most 255 characters long', done)
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
        'Member must satisfy enum value set: [TOTAL, NONE]; ' +
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
        'The attempted filter operation is not supported for the provided filter argument count', done)
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
        'The attempted filter operation is not supported for the provided filter argument count', done)
    })

    it('should return ValidationException for 1 arg to NULL', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'NULL', AttributeValueList: [{S: 'a'}]}}},
        'The attempted filter operation is not supported for the provided filter argument count', done)
    })

    it('should return ValidationException for 1 arg to NOT_NULL', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'NOT_NULL', AttributeValueList: [{S: 'a'}]}}},
        'The attempted filter operation is not supported for the provided filter argument count', done)
    })

    it('should return ValidationException for 0 args to NE', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'NE'}}},
        'The attempted filter operation is not supported for the provided filter argument count', done)
    })

    it('should return ValidationException for 2 args to NE', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'NE', AttributeValueList: [{S: 'a'}, {S: 'a'}]}}},
        'The attempted filter operation is not supported for the provided filter argument count', done)
    })

    it('should return ValidationException for 0 args to LE', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'LE'}}},
        'The attempted filter operation is not supported for the provided filter argument count', done)
    })

    it('should return ValidationException for 2 args to LE', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'LE', AttributeValueList: [{S: 'a'}, {S: 'a'}]}}},
        'The attempted filter operation is not supported for the provided filter argument count', done)
    })

    it('should return ValidationException for 0 args to LT', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'LT'}}},
        'The attempted filter operation is not supported for the provided filter argument count', done)
    })

    it('should return ValidationException for 2 args to LT', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'LT', AttributeValueList: [{S: 'a'}, {S: 'a'}]}}},
        'The attempted filter operation is not supported for the provided filter argument count', done)
    })

    it('should return ValidationException for 0 args to GE', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'GE'}}},
        'The attempted filter operation is not supported for the provided filter argument count', done)
    })

    it('should return ValidationException for 2 args to GE', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'GE', AttributeValueList: [{S: 'a'}, {S: 'a'}]}}},
        'The attempted filter operation is not supported for the provided filter argument count', done)
    })

    it('should return ValidationException for 0 args to GT', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'GT'}}},
        'The attempted filter operation is not supported for the provided filter argument count', done)
    })

    it('should return ValidationException for 2 args to GT', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'GT', AttributeValueList: [{S: 'a'}, {S: 'a'}]}}},
        'The attempted filter operation is not supported for the provided filter argument count', done)
    })

    it('should return ValidationException for 0 args to CONTAINS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'CONTAINS'}}},
        'The attempted filter operation is not supported for the provided filter argument count', done)
    })

    it('should return ValidationException for 2 args to CONTAINS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'CONTAINS', AttributeValueList: [{S: 'a'}, {S: 'a'}]}}},
        'The attempted filter operation is not supported for the provided filter argument count', done)
    })

    it('should return ValidationException for 0 args to NOT_CONTAINS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'NOT_CONTAINS'}}},
        'The attempted filter operation is not supported for the provided filter argument count', done)
    })

    it('should return ValidationException for 2 args to NOT_CONTAINS', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'NOT_CONTAINS', AttributeValueList: [{S: 'a'}, {S: 'a'}]}}},
        'The attempted filter operation is not supported for the provided filter argument count', done)
    })

    it('should return ValidationException for 0 args to BEGINS_WITH', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'BEGINS_WITH'}}},
        'The attempted filter operation is not supported for the provided filter argument count', done)
    })

    it('should return ValidationException for 2 args to BEGINS_WITH', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'BEGINS_WITH', AttributeValueList: [{S: 'a'}, {S: 'a'}]}}},
        'The attempted filter operation is not supported for the provided filter argument count', done)
    })

    it('should return ValidationException for 0 args to IN', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'IN'}}},
        'The attempted filter operation is not supported for the provided filter argument count', done)
    })

    // TODO: Will have to come back to this one - not sure what the upper bound is
    it.skip('should return ValidationException for 3 args to IN', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'IN', AttributeValueList: [{S: 'a'}, {S: 'a'}, {S: 'a'}]}}},
        'The attempted filter operation is not supported for the provided filter argument count', done)
    })

    it('should return ValidationException for 0 args to BETWEEN', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'BETWEEN'}}},
        'The attempted filter operation is not supported for the provided filter argument count', done)
    })

    it('should return ValidationException for 3 args to BETWEEN', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {a: {ComparisonOperator: 'BETWEEN', AttributeValueList: [{S: 'a'}, {S: 'a'}, {S: 'a'}]}}},
        'The attempted filter operation is not supported for the provided filter argument count', done)
    })

    // Weird - only checks this *after* it finds the table
    it.skip('should return ValidationException for unsupported comparison', function(done) {
      assertValidation({
        TableName: 'abc',
        ScanFilter: {
          a: {ComparisonOperator: 'CONTAINS', AttributeValueList: [{S: 'a'}]},
          b: {ComparisonOperator: 'NULL'},
        }},
        'Query key condition not supported', done)
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
        'One or more parameter values were invalid: An AttributeValue may not contain an empty binary type.', done)
    })

    // Somehow allows set types for keys
    it('should return ValidationException for empty set key', function(done) {
      assertValidation({
        TableName: 'abc',
        ExclusiveStartKey: {a: {SS: []}},
        ScanFilter: {a: {ComparisonOperator: 'NULL'}}},
        'The provided starting key is invalid: ' +
        'One or more parameter values were invalid: An AttributeValue may not contain an empty set.', done)
    })

    it('should return ValidationException for empty string in set', function(done) {
      assertValidation({
        TableName: 'abc',
        ExclusiveStartKey: {a: {SS: ['a', '']}},
        ScanFilter: {a: {ComparisonOperator: 'NULL'}}},
        'The provided starting key is invalid: ' +
        'One or more parameter values were invalid: An AttributeValue may not contain an empty string.', done)
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

  })

})


