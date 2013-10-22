var helpers = require('./helpers'),
    should = require('should')

var target = 'CreateTable',
    request = helpers.request,
    randomName = helpers.randomName,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target)

describe('createTable', function() {

  describe('serializations', function() {

    it('should return SerializationException when TableName is not a string', function(done) {
      assertType('TableName', 'String', done)
    })

    it('should return SerializationException when AttributeDefinitions is not a list', function(done) {
      assertType('AttributeDefinitions', 'List', done)
    })

    it('should return SerializationException when KeySchema is not a list', function(done) {
      assertType('KeySchema', 'List', done)
    })

    it('should return SerializationException when LocalSecondaryIndexes is not a list', function(done) {
      assertType('LocalSecondaryIndexes', 'List', done)
    })

    it('should return SerializationException when ProvisionedThroughput is not a struct', function(done) {
      assertType('ProvisionedThroughput', 'Structure', done)
    })

    it('should return SerializationException when ProvisionedThroughput.WriteCapacityUnits is not a long', function(done) {
      assertType('ProvisionedThroughput.WriteCapacityUnits', 'Long', done)
    })

    it('should return SerializationException when ProvisionedThroughput.ReadCapacityUnits is not a long', function(done) {
      assertType('ProvisionedThroughput.ReadCapacityUnits', 'Long', done)
    })

    it('should return SerializationException when KeySchema.0 is not a struct', function(done) {
      assertType('KeySchema.0', 'Structure', done)
    })

    it('should return SerializationException when KeySchema.0.KeyType is not a string', function(done) {
      assertType('KeySchema.0.KeyType', 'String', done)
    })

    it('should return SerializationException when KeySchema.0.AttributeName is not a string', function(done) {
      assertType('KeySchema.0.AttributeName', 'String', done)
    })

    it('should return SerializationException when AttributeDefinitions.0 is not a struct', function(done) {
      assertType('AttributeDefinitions.0', 'Structure', done)
    })

    it('should return SerializationException when AttributeDefinitions.0.AttributeName is not a string', function(done) {
      assertType('AttributeDefinitions.0.AttributeName', 'String', done)
    })

    it('should return SerializationException when AttributeDefinitions.0.AttributeType is not a string', function(done) {
      assertType('AttributeDefinitions.0.AttributeType', 'String', done)
    })

    it('should return SerializationException when LocalSecondaryIndexes.0 is not a struct', function(done) {
      assertType('LocalSecondaryIndexes.0', 'Structure', done)
    })

    it('should return SerializationException when LocalSecondaryIndexes.0.IndexName is not a string', function(done) {
      assertType('LocalSecondaryIndexes.0.IndexName', 'String', done)
    })

    it('should return SerializationException when LocalSecondaryIndexes.0.KeySchema is not a list', function(done) {
      assertType('LocalSecondaryIndexes.0.KeySchema', 'List', done)
    })

    it('should return SerializationException when LocalSecondaryIndexes.0.Projection is not a struct', function(done) {
      assertType('LocalSecondaryIndexes.0.Projection', 'Structure', done)
    })

    it('should return SerializationException when LocalSecondaryIndexes.0.KeySchema.0 is not a struct', function(done) {
      assertType('LocalSecondaryIndexes.0.KeySchema.0', 'Structure', done)
    })

    it('should return SerializationException when LocalSecondaryIndexes.0.KeySchema.0.AttributeName is not a string', function(done) {
      assertType('LocalSecondaryIndexes.0.KeySchema.0.AttributeName', 'String', done)
    })

    it('should return SerializationException when LocalSecondaryIndexes.0.KeySchema.0.KeyType is not a string', function(done) {
      assertType('LocalSecondaryIndexes.0.KeySchema.0.KeyType', 'String', done)
    })

    it('should return SerializationException when LocalSecondaryIndexes.0.Projection.NonKeyAttributes is not a list', function(done) {
      assertType('LocalSecondaryIndexes.0.Projection.NonKeyAttributes', 'List', done)
    })

    it('should return SerializationException when LocalSecondaryIndexes.0.Projection.ProjectionType is not a string', function(done) {
      assertType('LocalSecondaryIndexes.0.Projection.ProjectionType', 'String', done)
    })

    it('should return SerializationException when LocalSecondaryIndexes.0.Projection.NonKeyAttributes.0 is not a string', function(done) {
      assertType('LocalSecondaryIndexes.0.Projection.NonKeyAttributes.0', 'String', done)
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
      assertValidation({TableName: new Array(256 + 1).join('a')},
        'TableName must be at least 3 characters long and at most 255 characters long', done)
    })

    it('should return ValidationException for null attributes', function(done) {
      assertValidation({TableName: 'abc;'},
        '4 validation errors detected: ' +
        'Value null at \'attributeDefinitions\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value \'abc;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value null at \'provisionedThroughput\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'keySchema\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for empty AttributeDefinitions', function(done) {
      assertValidation({TableName: 'abc', AttributeDefinitions: []},
        '2 validation errors detected: ' +
        'Value null at \'provisionedThroughput\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'keySchema\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for empty ProvisionedThroughput', function(done) {
      assertValidation({TableName: 'abc', AttributeDefinitions: [], ProvisionedThroughput: {}},
        '3 validation errors detected: ' +
        'Value null at \'provisionedThroughput.writeCapacityUnits\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'provisionedThroughput.readCapacityUnits\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'keySchema\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for low ProvisionedThroughput.WriteCapacityUnits', function(done) {
      assertValidation({TableName: 'abc', AttributeDefinitions: [], KeySchema: [],
        ProvisionedThroughput: {ReadCapacityUnits: -1, WriteCapacityUnits: -1}},
        '3 validation errors detected: ' +
        'Value \'-1\' at \'provisionedThroughput.writeCapacityUnits\' failed to satisfy constraint: ' +
        'Member must have value greater than or equal to 1; ' +
        'Value \'-1\' at \'provisionedThroughput.readCapacityUnits\' failed to satisfy constraint: ' +
        'Member must have value greater than or equal to 1; ' +
        'Value \'[]\' at \'keySchema\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1', done)
    })

    // TODO: Need to create toString methods for things like KeySchemaElement, etc
    it.skip('should return ValidationException for key element names', function(done) {
      assertValidation({TableName: 'abc', AttributeDefinitions: [],
        KeySchema: [{KeyType: 'HASH'}, {AttributeName: 'a'}, {KeyType: 'Woop', AttributeName: 'a'}],
        ProvisionedThroughput: {ReadCapacityUnits: 1000000000001, WriteCapacityUnits: 1000000000001}},
        '4 validation errors detected: ' +
        'Value \'[com.amazonaws.dynamodb.v20120810.KeySchemaElement@21b90f, com.amazonaws.dynamodb.v20120810.KeySchemaElement@62, com.amazonaws.dynamodb.v20120810.KeySchemaElement@293b3b]\' at \'keySchema\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 2; ' +
        'Value null at \'keySchema.1.member.attributeName\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'keySchema.2.member.keyType\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value \'Woop\' at \'keySchema.3.member.keyType\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [HASH, RANGE]', done)
    })

    it('should return ValidationException for high ProvisionedThroughput.ReadCapacityUnits and neg', function(done) {
      assertValidation({TableName: 'abc', AttributeDefinitions: [], KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}],
        ProvisionedThroughput: {ReadCapacityUnits: 1000000000001, WriteCapacityUnits: -1}},
        '1 validation error detected: ' +
        'Value \'-1\' at \'provisionedThroughput.writeCapacityUnits\' failed to satisfy constraint: ' +
        'Member must have value greater than or equal to 1', done)
    })

    it('should return ValidationException for high ProvisionedThroughput.ReadCapacityUnits', function(done) {
      assertValidation({TableName: 'abc', AttributeDefinitions: [], KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}],
        ProvisionedThroughput: {ReadCapacityUnits: 1000000000001, WriteCapacityUnits: 1000000000001}},
        'Given value 1000000000001 for ReadCapacityUnits is out of bounds', done)
    })

    it('should return ValidationException for high ProvisionedThroughput.ReadCapacityUnits second', function(done) {
      assertValidation({TableName: 'abc', AttributeDefinitions: [], KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}],
        ProvisionedThroughput: {WriteCapacityUnits: 1000000000001, ReadCapacityUnits: 1000000000001}},
        'Given value 1000000000001 for ReadCapacityUnits is out of bounds', done)
    })

    it('should return ValidationException for high ProvisionedThroughput.WriteCapacityUnits', function(done) {
      assertValidation({TableName: 'abc', AttributeDefinitions: [], KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}],
        ProvisionedThroughput: {ReadCapacityUnits: 1000000000000, WriteCapacityUnits: 1000000000001}},
        'Given value 1000000000001 for WriteCapacityUnits is out of bounds', done)
    })

    it('should return ValidationException for missing key attribute definitions', function(done) {
      assertValidation({TableName: 'abc', AttributeDefinitions: [],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'HASH', AttributeName: 'a'}],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        'Invalid KeySchema: Some index key attribute have no definition', done)
    })

    it('should return ValidationException for attribute definitions member nulls', function(done) {
      assertValidation({TableName: 'abc', AttributeDefinitions: [{}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'HASH', AttributeName: 'a'}],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        '2 validation errors detected: ' +
        'Value null at \'attributeDefinitions.1.member.attributeName\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'attributeDefinitions.1.member.attributeType\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for SS in attr definition', function(done) {
      assertValidation({TableName: 'abc', AttributeDefinitions: [{AttributeName: 'b', AttributeType: 'SS'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'HASH', AttributeName: 'a'}],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        '1 validation error detected: ' +
        'Value \'SS\' at \'attributeDefinitions.1.member.attributeType\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [B, N, S]', done)
    })

    it('should return ValidationException for random attr definition', function(done) {
      assertValidation({TableName: 'abc', AttributeDefinitions: [{AttributeName: 'b', AttributeType: 'a'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'HASH', AttributeName: 'a'}],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        '1 validation error detected: ' +
        'Value \'a\' at \'attributeDefinitions.1.member.attributeType\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [B, N, S]', done)
    })

    it('should return ValidationException for missing key attr definition when double', function(done) {
      assertValidation({TableName: 'abc', AttributeDefinitions: [{AttributeName: 'b', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'HASH', AttributeName: 'a'}],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        'Invalid KeySchema: Some index key attribute have no definition', done)
    })

    it('should return ValidationException for missing key attr definition', function(done) {
      assertValidation({TableName: 'abc', AttributeDefinitions: [{AttributeName: 'b', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        'One or more parameter values were invalid: Some index key attributes are not defined in ' +
        'AttributeDefinitions. Keys: [a], AttributeDefinitions: [b]', done)
    })

    it('should return ValidationException for missing key attr definition when double and valid', function(done) {
      assertValidation({TableName: 'abc', AttributeDefinitions: [{AttributeName: 'b', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'RANGE', AttributeName: 'b'}],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        'Invalid KeySchema: Some index key attribute have no definition', done)
    })

    it('should return ValidationException for missing key attr definition when double and same', function(done) {
      assertValidation({TableName: 'abc', AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'HASH', AttributeName: 'a'}],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        'Invalid KeySchema: Some index key attribute have no definition', done)
    })

    it('should return ValidationException for 5', function(done) {
      assertValidation({TableName: 'abc',
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'HASH', AttributeName: 'a'}],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        'Both the Hash Key and the Range Key element in the KeySchema have the same name', done)
    })

    it('should return ValidationException for 6', function(done) {
      assertValidation({TableName: 'abc',
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'HASH', AttributeName: 'b'}],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        'Invalid KeySchema: The second KeySchemaElement is not a RANGE key type', done)
    })

    it('should return ValidationException for 7', function(done) {
      assertValidation({TableName: 'abc',
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'RANGE', AttributeName: 'a'}, {KeyType: 'HASH', AttributeName: 'b'}],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        'Invalid KeySchema: The first KeySchemaElement is not a HASH key type', done)
    })

    it('should return ValidationException for 8', function(done) {
      assertValidation({TableName: 'abc',
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'RANGE', AttributeName: 'a'}, {KeyType: 'RANGE', AttributeName: 'b'}],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        'Invalid KeySchema: The first KeySchemaElement is not a HASH key type', done)
    })

    it('should return ValidationException for 9', function(done) {
      assertValidation({TableName: 'abc',
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'c', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'RANGE', AttributeName: 'b'}],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        'One or more parameter values were invalid: Number of attributes in KeySchema does not ' +
        'exactly match number of attributes defined in AttributeDefinitions', done)
    })

    it('should return ValidationException for 10', function(done) {
      assertValidation({TableName: 'abc',
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        'One or more parameter values were invalid: Number of attributes in KeySchema does not ' +
        'exactly match number of attributes defined in AttributeDefinitions', done)
    })

    it('should return ValidationException for 11', function(done) {
      assertValidation({TableName: 'abc',
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}],
        LocalSecondaryIndexes: [],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        'One or more parameter values were invalid: List of LocalSecondaryIndexes is empty', done)
    })

    it('should return ValidationException for 12', function(done) {
      assertValidation({TableName: 'abc',
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'RANGE', AttributeName: 'b'}],
        LocalSecondaryIndexes: [{},{},{},{},{},{},{},{},{}],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        '10 validation errors detected: ' +
        'Value null at \'localSecondaryIndexes.1.member.projection\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'localSecondaryIndexes.1.member.indexName\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'localSecondaryIndexes.1.member.keySchema\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'localSecondaryIndexes.2.member.projection\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'localSecondaryIndexes.2.member.indexName\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'localSecondaryIndexes.2.member.keySchema\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'localSecondaryIndexes.3.member.projection\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'localSecondaryIndexes.3.member.indexName\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'localSecondaryIndexes.3.member.keySchema\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'localSecondaryIndexes.4.member.projection\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for 13', function(done) {
      var name = new Array(256 + 1).join('a')
      assertValidation({TableName: 'abc',
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}],
        LocalSecondaryIndexes: [{
          IndexName: 'h;', KeySchema: [], Projection: {}
        }, {
          IndexName: name, KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}], Projection: {}
        }],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        '4 validation errors detected: ' +
        'Value \'h;\' at \'localSecondaryIndexes.1.member.indexName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'h;\' at \'localSecondaryIndexes.1.member.indexName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 3; ' +
        'Value \'[]\' at \'localSecondaryIndexes.1.member.keySchema\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1; ' +
        'Value \'' + name + '\' at \'localSecondaryIndexes.2.member.indexName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 255', done)
    })

    it('should return ValidationException for 14', function(done) {
      assertValidation({TableName: 'abc',
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}],
        LocalSecondaryIndexes: [{IndexName: 'abc', KeySchema: [{AttributeName: 'c', KeyType: 'RANGE'}], Projection: {}}],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        'One or more parameter values were invalid: Table KeySchema does not have a range key, ' +
        'which is required when specifying a LocalSecondaryIndex', done)
    })

    it('should return ValidationException for 15', function(done) {
      assertValidation({TableName: 'abc',
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'RANGE', AttributeName: 'b'}],
        LocalSecondaryIndexes: [{
          IndexName: 'abc',
          KeySchema: [{AttributeName: 'c', KeyType: 'RANGE'}, {AttributeName: 'd', KeyType: 'RANGE'}],
          Projection: {}
        }, {
          IndexName: 'abc',
          KeySchema: [{AttributeName: 'e', KeyType: 'RANGE'}],
          Projection: {}
        }],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        'One or more parameter values were invalid: ' +
        'Some index key attributes are not defined in AttributeDefinitions. ' +
        'Keys: [d, c], AttributeDefinitions: [b, a]', done)
    })

    it('should return ValidationException for 16', function(done) {
      assertValidation({TableName: 'abc',
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'RANGE', AttributeName: 'b'}],
        LocalSecondaryIndexes: [{IndexName: 'abc', KeySchema: [{AttributeName: 'a', KeyType: 'RANGE'}], Projection: {}}],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        'Invalid KeySchema: The first KeySchemaElement is not a HASH key type', done)
    })

    it('should return ValidationException for 16.5', function(done) {
      assertValidation({TableName: 'abc',
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'RANGE', AttributeName: 'b'}],
        LocalSecondaryIndexes: [{IndexName: 'abc', KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}, {AttributeName: 'a', KeyType: 'HASH'}], Projection: {}}],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        'Both the Hash Key and the Range Key element in the KeySchema have the same name', done)
    })

    it('should return ValidationException for 16.6', function(done) {
      assertValidation({TableName: 'abc',
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'RANGE', AttributeName: 'b'}],
        LocalSecondaryIndexes: [{IndexName: 'abc', KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}, {AttributeName: 'b', KeyType: 'HASH'}], Projection: {}}],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        'Invalid KeySchema: The second KeySchemaElement is not a RANGE key type', done)
    })

    it('should return ValidationException for 17', function(done) {
      assertValidation({TableName: 'abc',
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'RANGE', AttributeName: 'b'}],
        LocalSecondaryIndexes: [{IndexName: 'abc', KeySchema: [{AttributeName: 'b', KeyType: 'HASH'}], Projection: {}}],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        'One or more parameter values were invalid: Index KeySchema does not have a range key for index: abc', done)
    })

    it('should return ValidationException for 18', function(done) {
      assertValidation({TableName: 'abc',
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'RANGE', AttributeName: 'b'}],
        LocalSecondaryIndexes: [{IndexName: 'abc', KeySchema: [{AttributeName: 'b', KeyType: 'HASH'}, {AttributeName: 'a', KeyType: 'RANGE'}], Projection: {}}],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        'One or more parameter values were invalid: ' +
        'Index KeySchema does not have the same leading hash key as table KeySchema for index: ' +
        'abc. index hash key: b, table hash key: a', done)
    })

    it('should return ValidationException for 19', function(done) {
      assertValidation({TableName: 'abc',
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'RANGE', AttributeName: 'b'}],
        LocalSecondaryIndexes: [{IndexName: 'abc', KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}, {AttributeName: 'a', KeyType: 'RANGE'}], Projection: {}}],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        'Both the Hash Key and the Range Key element in the KeySchema have the same name', done)
    })

    it('should return ValidationException for 20', function(done) {
      assertValidation({TableName: 'abc',
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'RANGE', AttributeName: 'b'}],
        LocalSecondaryIndexes: [{
          IndexName: 'abc',
          KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}, {AttributeName: 'c', KeyType: 'RANGE'}],
          Projection: {}
        }],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        'One or more parameter values were invalid: ' +
        'Some index key attributes are not defined in AttributeDefinitions. ' +
        'Keys: [c, a], AttributeDefinitions: [b, a]', done)
    })

    it('should return ValidationException for 21', function(done) {
      assertValidation({TableName: 'abc',
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'RANGE', AttributeName: 'b'}],
        LocalSecondaryIndexes: [{IndexName: 'abc', KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}, {AttributeName: 'b', KeyType: 'RANGE'}], Projection: {}}],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        'One or more parameter values were invalid: Unknown ProjectionType: null', done)
    })

    it('should return ValidationException for 22', function(done) {
      assertValidation({TableName: 'abc',
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'RANGE', AttributeName: 'b'}],
        LocalSecondaryIndexes: [{
          IndexName: 'abc',
          KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}, {AttributeName: 'b', KeyType: 'RANGE'}],
          Projection: {NonKeyAttributes: [], ProjectionType: 'abc'}
        }],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        '2 validation errors detected: ' +
        'Value \'abc\' at \'localSecondaryIndexes.1.member.projection.projectionType\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [ALL, INCLUDE, KEYS_ONLY]; ' +
        'Value \'[]\' at \'localSecondaryIndexes.1.member.projection.nonKeyAttributes\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1', done)
    })

    it('should return ValidationException for 23', function(done) {
      assertValidation({TableName: 'abc',
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'RANGE', AttributeName: 'b'}],
        LocalSecondaryIndexes: [{
          IndexName: 'abc',
          KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}, {AttributeName: 'b', KeyType: 'RANGE'}],
          Projection: {NonKeyAttributes: ['a']}
        }],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        'One or more parameter values were invalid: Unknown ProjectionType: null', done)
    })

    it('should return ValidationException for 24', function(done) {
      assertValidation({TableName: 'abc',
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'RANGE', AttributeName: 'b'}],
        LocalSecondaryIndexes: [{
          IndexName: 'abc',
          KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}, {AttributeName: 'b', KeyType: 'RANGE'}],
          Projection: {ProjectionType: 'ALL', NonKeyAttributes: ['a']}
        }],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        'One or more parameter values were invalid: ' +
        'ProjectionType is ALL, but NonKeyAttributes is specified', done)
    })

    it('should return ValidationException for 25', function(done) {
      assertValidation({TableName: 'abc',
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'RANGE', AttributeName: 'b'}],
        LocalSecondaryIndexes: [{
          IndexName: 'abc',
          KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}, {AttributeName: 'b', KeyType: 'RANGE'}],
          Projection: {ProjectionType: 'KEYS_ONLY', NonKeyAttributes: ['a']}
        }],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        'One or more parameter values were invalid: ' +
        'ProjectionType is KEYS_ONLY, but NonKeyAttributes is specified', done)
    })

    it('should return ValidationException for 26', function(done) {
      assertValidation({TableName: 'abc',
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'RANGE', AttributeName: 'b'}],
        LocalSecondaryIndexes: [{
          IndexName: 'abc',
          KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}, {AttributeName: 'b', KeyType: 'RANGE'}],
          Projection: {ProjectionType: 'ALL'}
        }, {
          IndexName: 'abc',
          KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}, {AttributeName: 'b', KeyType: 'RANGE'}],
          Projection: {ProjectionType: 'ALL'}
        }],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        'One or more parameter values were invalid: Duplicate index name: abc', done)
    })

    it('should return ValidationException for 27', function(done) {
      assertValidation({TableName: 'abc',
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'RANGE', AttributeName: 'b'}],
        LocalSecondaryIndexes: [{
          IndexName: 'abc',
          KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}, {AttributeName: 'b', KeyType: 'RANGE'}],
          Projection: {ProjectionType: 'ALL'}
        }, {
          IndexName: 'abd',
          KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}, {AttributeName: 'b', KeyType: 'RANGE'}],
          Projection: {ProjectionType: 'ALL'}
        }, {
          IndexName: 'abe',
          KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}, {AttributeName: 'b', KeyType: 'RANGE'}],
          Projection: {ProjectionType: 'ALL'}
        }, {
          IndexName: 'abf',
          KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}, {AttributeName: 'b', KeyType: 'RANGE'}],
          Projection: {ProjectionType: 'ALL'}
        }, {
          IndexName: 'abg',
          KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}, {AttributeName: 'b', KeyType: 'RANGE'}],
          Projection: {ProjectionType: 'ALL'}
        }, {
          IndexName: 'abh',
          KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}, {AttributeName: 'b', KeyType: 'RANGE'}],
          Projection: {ProjectionType: 'ALL'}
        }, {}],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        '3 validation errors detected: ' +
        'Value null at \'localSecondaryIndexes.7.member.projection\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'localSecondaryIndexes.7.member.indexName\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'localSecondaryIndexes.7.member.keySchema\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for 28', function(done) {
      assertValidation({TableName: 'abc',
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'RANGE', AttributeName: 'b'}],
        LocalSecondaryIndexes: [{
          IndexName: 'abc',
          KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}, {AttributeName: 'b', KeyType: 'RANGE'}],
          Projection: {ProjectionType: 'ALL'}
        }, {
          IndexName: 'abd',
          KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}, {AttributeName: 'b', KeyType: 'RANGE'}],
          Projection: {ProjectionType: 'ALL'}
        }, {
          IndexName: 'abe',
          KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}, {AttributeName: 'b', KeyType: 'RANGE'}],
          Projection: {ProjectionType: 'ALL'}
        }, {
          IndexName: 'abf',
          KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}, {AttributeName: 'b', KeyType: 'RANGE'}],
          Projection: {ProjectionType: 'ALL'}
        }, {
          IndexName: 'abg',
          KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}, {AttributeName: 'b', KeyType: 'RANGE'}],
          Projection: {ProjectionType: 'ALL'}
        }, {
          IndexName: 'abh',
          KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}, {AttributeName: 'b', KeyType: 'RANGE'}],
          Projection: {ProjectionType: 'ALL'}
        }],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}},
        'One or more parameter values were invalid: Number of indexes exceeds per-table limit of 5', done)
    })

    it('should succeed for basic', function(done) {
      var table = {
        TableName: randomName(),
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1},
      }, createdAt = Date.now() / 1000
      request(opts(table), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        should.exist(res.body.TableDescription)
        var desc = res.body.TableDescription
        desc.CreationDateTime.should.be.above(createdAt - 5)
        ;delete desc.CreationDateTime
        table.ItemCount = 0
        table.ProvisionedThroughput.NumberOfDecreasesToday = 0
        table.TableSizeBytes = 0
        table.TableStatus = 'CREATING'
        desc.should.eql(table)
        done()
      })
    })

    it('should change state to ACTIVE after a period', function(done) {
      this.timeout(100000)
      var table = {
        TableName: randomName(),
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1},
      }
      request(opts(table), function(err, res) {
        if (err) return done(err)
        res.body.TableDescription.TableStatus.should.equal('CREATING')

        helpers.waitUntilActive(table.TableName, function(err, res) {
          if (err) return done(err)
          res.body.Table.TableStatus.should.equal('ACTIVE')
          done()
        })
      })
    })

    it('should succeed for indexes', function(done) {
      var table = {
        TableName: randomName(),
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'RANGE', AttributeName: 'b'}],
        LocalSecondaryIndexes: [{
          IndexName: 'abc',
          KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}, {AttributeName: 'b', KeyType: 'RANGE'}],
          Projection: {ProjectionType: 'INCLUDE', NonKeyAttributes: ['a']}
        }],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}
      }, createdAt = Date.now() / 1000
      request(opts(table), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        should.exist(res.body.TableDescription)
        var desc = res.body.TableDescription
        desc.CreationDateTime.should.be.above(createdAt - 5)
        ;delete desc.CreationDateTime
        table.ItemCount = 0
        table.ProvisionedThroughput.NumberOfDecreasesToday = 0
        table.TableSizeBytes = 0
        table.TableStatus = 'CREATING'
        table.LocalSecondaryIndexes.forEach(function(index) {
          index.IndexSizeBytes = 0
          index.ItemCount = 0
        })
        desc.should.eql(table)
        done()
      })
    })

    // TODO: Implement this error:
    //{ __type: 'com.amazonaws.dynamodb.v20120810#LimitExceededException',
    //message: 'Subscriber limit exceeded: Only 1 table with local secondary index can be created simultaneously' }
    //
    it.skip('should succeed for multiple indexes', function(done) {
      var table = {
        TableName: randomName(),
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'S'}],
        KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'RANGE', AttributeName: 'b'}],
        LocalSecondaryIndexes: [{
          IndexName: 'abc',
          KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}, {AttributeName: 'b', KeyType: 'RANGE'}],
          Projection: {ProjectionType: 'ALL'}
        }, {
          IndexName: 'abd',
          KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}, {AttributeName: 'b', KeyType: 'RANGE'}],
          Projection: {ProjectionType: 'ALL'}
        }, {
          IndexName: 'abe',
          KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}, {AttributeName: 'b', KeyType: 'RANGE'}],
          Projection: {ProjectionType: 'ALL'}
        }, {
          IndexName: 'abf',
          KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}, {AttributeName: 'b', KeyType: 'RANGE'}],
          Projection: {ProjectionType: 'ALL'}
        }, {
          IndexName: 'abg',
          KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}, {AttributeName: 'b', KeyType: 'RANGE'}],
          Projection: {ProjectionType: 'ALL'}
        }],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1}
      }, createdAt = Date.now() / 1000
      request(opts(table), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        should.exist(res.body.TableDescription)
        var desc = res.body.TableDescription
        desc.CreationDateTime.should.be.above(createdAt - 5)
        ;delete desc.CreationDateTime
        table.ItemCount = 0
        table.ProvisionedThroughput.NumberOfDecreasesToday = 0
        table.TableSizeBytes = 0
        table.TableStatus = 'CREATING'
        // DynamoDB seem to put them in a weird order, so check separately
        table.LocalSecondaryIndexes.forEach(function(index) {
          index.IndexSizeBytes = 0
          index.ItemCount = 0
          desc.LocalSecondaryIndexes.should.includeEql(index)
        })
        desc.LocalSecondaryIndexes.length.should.equal(table.LocalSecondaryIndexes.length)
        ;delete desc.LocalSecondaryIndexes
        ;delete table.LocalSecondaryIndexes
        desc.should.eql(table)
        done()
      })
    })

  })

})


