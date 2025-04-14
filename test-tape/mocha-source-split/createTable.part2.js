var helpers = require('./helpers'),
  should = require('should')

var target = 'CreateTable',
  request = helpers.request,
  randomName = helpers.randomName,
  opts = helpers.opts.bind(null, target),
  assertType = helpers.assertType.bind(null, target),
  assertValidation = helpers.assertValidation.bind(null, target)

describe('createTable', function () {
  describe('validations', function () {

    it('should return ValidationException for no TableName', function (done) {
      assertValidation({},
        'The parameter \'TableName\' is required but was not present in the request', done)
    })

    it('should return ValidationException for empty TableName', function (done) {
      assertValidation({ TableName: '' },
        'TableName must be at least 3 characters long and at most 255 characters long', done)
    })

    it('should return ValidationException for short TableName', function (done) {
      assertValidation({ TableName: 'a;' },
        'TableName must be at least 3 characters long and at most 255 characters long', done)
    })

    it('should return ValidationException for long TableName', function (done) {
      assertValidation({ TableName: new Array(256 + 1).join('a') },
        'TableName must be at least 3 characters long and at most 255 characters long', done)
    })

    it('should return ValidationException for null attributes', function (done) {
      assertValidation({ TableName: 'abc;' }, [
        'Value null at \'attributeDefinitions\' failed to satisfy constraint: ' +
        'Member must not be null',
        'Value \'abc;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        'Value null at \'keySchema\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for empty AttributeDefinitions', function (done) {
      assertValidation({ TableName: 'abc', AttributeDefinitions: [] }, [
        'Value null at \'keySchema\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for empty ProvisionedThroughput', function (done) {
      assertValidation({ TableName: 'abc', AttributeDefinitions: [], ProvisionedThroughput: {}, BillingMode: 'PAY_PER_REQUEST' }, [
        'Value null at \'provisionedThroughput.writeCapacityUnits\' failed to satisfy constraint: ' +
        'Member must not be null',
        'Value null at \'provisionedThroughput.readCapacityUnits\' failed to satisfy constraint: ' +
        'Member must not be null',
        'Value null at \'keySchema\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for low ProvisionedThroughput.WriteCapacityUnits', function (done) {
      assertValidation({ TableName: 'abc', AttributeDefinitions: [], KeySchema: [],
        ProvisionedThroughput: { ReadCapacityUnits: -1, WriteCapacityUnits: -1 }, BillingMode: 'A' }, [
        'Value \'A\' at \'billingMode\' failed to satisfy constraint: ' +
          'Member must satisfy enum value set: [PROVISIONED, PAY_PER_REQUEST]',
        'Value \'-1\' at \'provisionedThroughput.writeCapacityUnits\' failed to satisfy constraint: ' +
          'Member must have value greater than or equal to 1',
        'Value \'-1\' at \'provisionedThroughput.readCapacityUnits\' failed to satisfy constraint: ' +
          'Member must have value greater than or equal to 1',
        'Value \'[]\' at \'keySchema\' failed to satisfy constraint: ' +
          'Member must have length greater than or equal to 1',
      ], done)
    })

    it('should return ValidationException for key element names', function (done) {
      assertValidation({ TableName: 'abc', AttributeDefinitions: [],
        KeySchema: [ { KeyType: 'HASH' }, { AttributeName: 'a' }, { KeyType: 'Woop', AttributeName: 'a' } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1000000000001, WriteCapacityUnits: 1000000000001 } }, [
        new RegExp('Value \'\\[.+\\]\' at \'keySchema\' failed to satisfy constraint: ' +
            'Member must have length less than or equal to 2'),
        'Value null at \'keySchema.1.member.attributeName\' failed to satisfy constraint: ' +
          'Member must not be null',
        'Value null at \'keySchema.2.member.keyType\' failed to satisfy constraint: ' +
          'Member must not be null',
        'Value \'Woop\' at \'keySchema.3.member.keyType\' failed to satisfy constraint: ' +
          'Member must satisfy enum value set: [HASH, RANGE]',
      ], done)
    })

    it('should return ValidationException for high ProvisionedThroughput.ReadCapacityUnits and neg', function (done) {
      assertValidation({ TableName: 'abc', AttributeDefinitions: [], KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1000000000001, WriteCapacityUnits: -1 } }, [
        'Value \'-1\' at \'provisionedThroughput.writeCapacityUnits\' failed to satisfy constraint: ' +
          'Member must have value greater than or equal to 1',
      ], done)
    })

    it('should return ValidationException for missing ProvisionedThroughput', function (done) {
      assertValidation({ TableName: 'abc', AttributeDefinitions: [], KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ] },
        'One or more parameter values were invalid: ReadCapacityUnits and WriteCapacityUnits must both be specified when BillingMode is PROVISIONED', done)
    })

    it('should return ValidationException if ProvisionedThroughput set when BillingMode is PAY_PER_REQUEST', function (done) {
      assertValidation({ TableName: 'abc', AttributeDefinitions: [],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'HASH', AttributeName: 'a' } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 }, BillingMode: 'PAY_PER_REQUEST' },
      'One or more parameter values were invalid: ' +
        'Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST', done)
    })

    it('should return ValidationException for high ProvisionedThroughput.ReadCapacityUnits', function (done) {
      assertValidation({ TableName: 'abc', AttributeDefinitions: [], KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1000000000001, WriteCapacityUnits: 1000000000001 } },
      'Given value 1000000000001 for ReadCapacityUnits is out of bounds', done)
    })

    it('should return ValidationException for high ProvisionedThroughput.ReadCapacityUnits second', function (done) {
      assertValidation({ TableName: 'abc', AttributeDefinitions: [], KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
        ProvisionedThroughput: { WriteCapacityUnits: 1000000000001, ReadCapacityUnits: 1000000000001 } },
      'Given value 1000000000001 for ReadCapacityUnits is out of bounds', done)
    })

    it('should return ValidationException for high ProvisionedThroughput.WriteCapacityUnits', function (done) {
      assertValidation({ TableName: 'abc', AttributeDefinitions: [], KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1000000000000, WriteCapacityUnits: 1000000000001 } },
      'Given value 1000000000001 for WriteCapacityUnits is out of bounds', done)
    })

    it('should return ValidationException for missing key attribute definitions', function (done) {
      assertValidation({ TableName: 'abc', AttributeDefinitions: [],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'HASH', AttributeName: 'a' } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'Invalid KeySchema: Some index key attribute have no definition', done)
    })

    it('should return ValidationException for missing key attribute definitions if BillingMode is PAY_PER_REQUEST', function (done) {
      assertValidation({ TableName: 'abc', AttributeDefinitions: [],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ], BillingMode: 'PAY_PER_REQUEST' },
      'Invalid KeySchema: Some index key attribute have no definition', done)
    })

    it('should return ValidationException for attribute definitions member nulls', function (done) {
      assertValidation({ TableName: 'abc', AttributeDefinitions: [ {} ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'HASH', AttributeName: 'a' } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } }, [
        'Value null at \'attributeDefinitions.1.member.attributeName\' failed to satisfy constraint: ' +
          'Member must not be null',
        'Value null at \'attributeDefinitions.1.member.attributeType\' failed to satisfy constraint: ' +
          'Member must not be null',
      ], done)
    })

    it('should return ValidationException for SS in attr definition', function (done) {
      assertValidation({ TableName: 'abc', AttributeDefinitions: [ { AttributeName: 'b', AttributeType: 'SS' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'HASH', AttributeName: 'a' } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      '1 validation error detected: ' +
        'Value \'SS\' at \'attributeDefinitions.1.member.attributeType\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [B, N, S]', done)
    })

    it('should return ValidationException for random attr definition', function (done) {
      assertValidation({ TableName: 'abc', AttributeDefinitions: [ { AttributeName: 'b', AttributeType: 'a' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'HASH', AttributeName: 'a' } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      '1 validation error detected: ' +
        'Value \'a\' at \'attributeDefinitions.1.member.attributeType\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [B, N, S]', done)
    })

    it('should return ValidationException for missing key attr definition when double', function (done) {
      assertValidation({ TableName: 'abc', AttributeDefinitions: [ { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'HASH', AttributeName: 'a' } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'Invalid KeySchema: Some index key attribute have no definition', done)
    })

    it('should return ValidationException for missing key attr definition', function (done) {
      assertValidation({ TableName: 'abc', AttributeDefinitions: [ { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'One or more parameter values were invalid: Some index key attributes are not defined in ' +
        'AttributeDefinitions. Keys: [a], AttributeDefinitions: [b]', done)
    })

    it('should return ValidationException for missing key attr definition when double and valid', function (done) {
      assertValidation({ TableName: 'abc', AttributeDefinitions: [ { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'Invalid KeySchema: Some index key attribute have no definition', done)
    })

    it('should return ValidationException for missing key attr definition when double and same', function (done) {
      assertValidation({ TableName: 'abc', AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'HASH', AttributeName: 'a' } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'Invalid KeySchema: Some index key attribute have no definition', done)
    })

    it('should return ValidationException for hash key and range key having same name', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'HASH', AttributeName: 'a' } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'Both the Hash Key and the Range Key element in the KeySchema have the same name', done)
    })

    it('should return ValidationException for second key not being range', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'HASH', AttributeName: 'b' } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'Invalid KeySchema: The second KeySchemaElement is not a RANGE key type', done)
    })

    it('should return ValidationException for second key being hash', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'RANGE', AttributeName: 'a' }, { KeyType: 'HASH', AttributeName: 'b' } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'Invalid KeySchema: The first KeySchemaElement is not a HASH key type', done)
    })

    it('should return ValidationException for both being range key', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'RANGE', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'Invalid KeySchema: The first KeySchemaElement is not a HASH key type', done)
    })

    it('should return ValidationException for extra attribute in definitions when range', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'c', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'One or more parameter values were invalid: Number of attributes in KeySchema does not ' +
        'exactly match number of attributes defined in AttributeDefinitions', done)
    })

    it('should return ValidationException for extra attribute in definitions when hash', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'One or more parameter values were invalid: Number of attributes in KeySchema does not ' +
        'exactly match number of attributes defined in AttributeDefinitions', done)
    })

    it('should return ValidationException for empty LocalSecondaryIndexes list', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
        LocalSecondaryIndexes: [],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'One or more parameter values were invalid: List of LocalSecondaryIndexes is empty', done)
    })

    it('should return ValidationException for more than five empty LocalSecondaryIndexes', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        LocalSecondaryIndexes: [ {}, {}, {}, {}, {}, {}, {}, {}, {} ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } }, [
        'Value null at \'localSecondaryIndexes.1.member.indexName\' failed to satisfy constraint: ' +
          'Member must not be null',
        'Value null at \'localSecondaryIndexes.1.member.keySchema\' failed to satisfy constraint: ' +
          'Member must not be null',
        'Value null at \'localSecondaryIndexes.1.member.projection\' failed to satisfy constraint: ' +
          'Member must not be null',
        'Value null at \'localSecondaryIndexes.2.member.indexName\' failed to satisfy constraint: ' +
          'Member must not be null',
        'Value null at \'localSecondaryIndexes.2.member.keySchema\' failed to satisfy constraint: ' +
          'Member must not be null',
        'Value null at \'localSecondaryIndexes.2.member.projection\' failed to satisfy constraint: ' +
          'Member must not be null',
        'Value null at \'localSecondaryIndexes.3.member.indexName\' failed to satisfy constraint: ' +
          'Member must not be null',
        'Value null at \'localSecondaryIndexes.3.member.keySchema\' failed to satisfy constraint: ' +
          'Member must not be null',
        'Value null at \'localSecondaryIndexes.3.member.projection\' failed to satisfy constraint: ' +
          'Member must not be null',
        'Value null at \'localSecondaryIndexes.4.member.indexName\' failed to satisfy constraint: ' +
          'Member must not be null',
      ], done)
    })

    it('should return ValidationException for bad LocalSecondaryIndex names', function (done) {
      var name = new Array(256 + 1).join('a')
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
        LocalSecondaryIndexes: [ {
          IndexName: 'h;', KeySchema: [], Projection: {},
        }, {
          IndexName: name, KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' } ], Projection: {},
        } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } }, [
        'Value \'h;\' at \'localSecondaryIndexes.1.member.indexName\' failed to satisfy constraint: ' +
          'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        'Value \'h;\' at \'localSecondaryIndexes.1.member.indexName\' failed to satisfy constraint: ' +
          'Member must have length greater than or equal to 3',
        'Value \'[]\' at \'localSecondaryIndexes.1.member.keySchema\' failed to satisfy constraint: ' +
          'Member must have length greater than or equal to 1',
        'Value \'' + name + '\' at \'localSecondaryIndexes.2.member.indexName\' failed to satisfy constraint: ' +
          'Member must have length less than or equal to 255',
      ], done)
    })

    it('should return ValidationException for no range key with LocalSecondaryIndex', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
        LocalSecondaryIndexes: [ { IndexName: 'abc', KeySchema: [ { AttributeName: 'c', KeyType: 'RANGE' } ], Projection: {} } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'One or more parameter values were invalid: Table KeySchema does not have a range key, ' +
        'which is required when specifying a LocalSecondaryIndex', done)
    })

    it('should return ValidationException for missing attribute definitions in LocalSecondaryIndex', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        LocalSecondaryIndexes: [ {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'c', KeyType: 'RANGE' }, { AttributeName: 'd', KeyType: 'RANGE' } ],
          Projection: {},
        }, {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'e', KeyType: 'RANGE' } ],
          Projection: {},
        } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      new RegExp('One or more parameter values were invalid: ' +
          'Some index key attributes are not defined in AttributeDefinitions. ' +
          'Keys: \\[(c, d|d, c)\\], AttributeDefinitions: \\[(a, b|b, a)\\]'), done)
    })

    it('should return ValidationException for first key in LocalSecondaryIndex not being hash', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        LocalSecondaryIndexes: [ { IndexName: 'abc', KeySchema: [ { AttributeName: 'a', KeyType: 'RANGE' } ], Projection: {} } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'Invalid KeySchema: The first KeySchemaElement is not a HASH key type', done)
    })

    it('should return ValidationException for same names of keys in LocalSecondaryIndex', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        LocalSecondaryIndexes: [ { IndexName: 'abc', KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'a', KeyType: 'HASH' } ], Projection: {} } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'Both the Hash Key and the Range Key element in the KeySchema have the same name', done)
    })

    it('should return ValidationException for second key of LocalSecondaryIndex not being range', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        LocalSecondaryIndexes: [ { IndexName: 'abc', KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'HASH' } ], Projection: {} } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'Invalid KeySchema: The second KeySchemaElement is not a RANGE key type', done)
    })

    it('should return ValidationException for no range key in LocalSecondaryIndex', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        LocalSecondaryIndexes: [ { IndexName: 'abc', KeySchema: [ { AttributeName: 'b', KeyType: 'HASH' } ], Projection: {} } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'One or more parameter values were invalid: Index KeySchema does not have a range key for index: abc', done)
    })

    it('should return ValidationException for different hash key between LocalSecondaryIndex and table', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        LocalSecondaryIndexes: [ { IndexName: 'abc', KeySchema: [ { AttributeName: 'b', KeyType: 'HASH' }, { AttributeName: 'a', KeyType: 'RANGE' } ], Projection: {} } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'One or more parameter values were invalid: ' +
        'Index KeySchema does not have the same leading hash key as table KeySchema for index: ' +
        'abc. index hash key: b, table hash key: a', done)
    })

    it('should return ValidationException for same named keys in LocalSecondaryIndex when one hash and one range', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        LocalSecondaryIndexes: [ { IndexName: 'abc', KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'a', KeyType: 'RANGE' } ], Projection: {} } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'Both the Hash Key and the Range Key element in the KeySchema have the same name', done)
    })

    it('should return ValidationException for missing attribute definitions when hash is same in LocalSecondaryIndex', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        LocalSecondaryIndexes: [ {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'c', KeyType: 'RANGE' } ],
          Projection: {},
        } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      new RegExp('One or more parameter values were invalid: ' +
          'Some index key attributes are not defined in AttributeDefinitions. ' +
          'Keys: \\[(a, c|c, a)\\], AttributeDefinitions: \\[(a, b|b, a)\\]'), done)
    })

    it('should return ValidationException for empty Projection in LocalSecondaryIndex', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        LocalSecondaryIndexes: [ { IndexName: 'abc', KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ], Projection: {} } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'One or more parameter values were invalid: Unknown ProjectionType: null', done)
    })

    it('should return ValidationException for invalid properties in LocalSecondaryIndex', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        LocalSecondaryIndexes: [ {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          Projection: { NonKeyAttributes: [], ProjectionType: 'abc' },
        } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } }, [
        'Value \'abc\' at \'localSecondaryIndexes.1.member.projection.projectionType\' failed to satisfy constraint: ' +
          'Member must satisfy enum value set: [ALL, INCLUDE, KEYS_ONLY]',
        'Value \'[]\' at \'localSecondaryIndexes.1.member.projection.nonKeyAttributes\' failed to satisfy constraint: ' +
          'Member must have length greater than or equal to 1',
      ], done)
    })

    it('should return ValidationException for missing ProjectionType in LocalSecondaryIndex', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        LocalSecondaryIndexes: [ {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          Projection: { NonKeyAttributes: [ 'a' ] },
        } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'One or more parameter values were invalid: Unknown ProjectionType: null', done)
    })

    it('should return ValidationException for NonKeyAttributes with ProjectionType ALL in LocalSecondaryIndex', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        LocalSecondaryIndexes: [ {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          Projection: { ProjectionType: 'ALL', NonKeyAttributes: [ 'a' ] },
        } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'One or more parameter values were invalid: ' +
        'ProjectionType is ALL, but NonKeyAttributes is specified', done)
    })

    it('should return ValidationException for NonKeyAttributes with ProjectionType KEYS_ONLY in LocalSecondaryIndex', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        LocalSecondaryIndexes: [ {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          Projection: { ProjectionType: 'KEYS_ONLY', NonKeyAttributes: [ 'a' ] },
        } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'One or more parameter values were invalid: ' +
        'ProjectionType is KEYS_ONLY, but NonKeyAttributes is specified', done)
    })

    it('should return ValidationException for duplicate index names in LocalSecondaryIndexes', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        LocalSecondaryIndexes: [ {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          Projection: { ProjectionType: 'ALL' },
        }, {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          Projection: { ProjectionType: 'ALL' },
        } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'One or more parameter values were invalid: Duplicate index name: abc', done)
    })

    it('should return ValidationException for extraneous values in LocalSecondaryIndexes', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        LocalSecondaryIndexes: [ {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          Projection: { ProjectionType: 'ALL' },
        }, {
          IndexName: 'abd',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          Projection: { ProjectionType: 'ALL' },
        }, {
          IndexName: 'abe',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          Projection: { ProjectionType: 'ALL' },
        }, {
          IndexName: 'abf',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          Projection: { ProjectionType: 'ALL' },
        }, {
          IndexName: 'abg',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          Projection: { ProjectionType: 'ALL' },
        }, {
          IndexName: 'abh',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          Projection: { ProjectionType: 'ALL' },
        }, {} ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } }, [
        'Value null at \'localSecondaryIndexes.7.member.projection\' failed to satisfy constraint: ' +
          'Member must not be null',
        'Value null at \'localSecondaryIndexes.7.member.indexName\' failed to satisfy constraint: ' +
          'Member must not be null',
        'Value null at \'localSecondaryIndexes.7.member.keySchema\' failed to satisfy constraint: ' +
          'Member must not be null',
      ], done)
    })

    it('should return ValidationException for more than five valid LocalSecondaryIndexes', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        LocalSecondaryIndexes: [ {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          Projection: { ProjectionType: 'ALL' },
        }, {
          IndexName: 'abd',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          Projection: { ProjectionType: 'ALL' },
        }, {
          IndexName: 'abe',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          Projection: { ProjectionType: 'ALL' },
        }, {
          IndexName: 'abf',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          Projection: { ProjectionType: 'ALL' },
        }, {
          IndexName: 'abg',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          Projection: { ProjectionType: 'ALL' },
        }, {
          IndexName: 'abh',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          Projection: { ProjectionType: 'ALL' },
        } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'One or more parameter values were invalid: Number of LocalSecondaryIndexes exceeds per-table limit of 5', done)
    })


    it('should return ValidationException for empty GlobalSecondaryIndexes list', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
        GlobalSecondaryIndexes: [],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'One or more parameter values were invalid: List of GlobalSecondaryIndexes is empty', done)
    })

    it('should return ValidationException for more than five empty GlobalSecondaryIndexes', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        GlobalSecondaryIndexes: [ {}, {}, {}, {}, {}, {}, {}, {}, {} ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } }, [
        'Value null at \'globalSecondaryIndexes.1.member.keySchema\' failed to satisfy constraint: ' +
          'Member must not be null',
        'Value null at \'globalSecondaryIndexes.1.member.projection\' failed to satisfy constraint: ' +
          'Member must not be null',
        'Value null at \'globalSecondaryIndexes.1.member.indexName\' failed to satisfy constraint: ' +
          'Member must not be null',
        'Value null at \'globalSecondaryIndexes.2.member.keySchema\' failed to satisfy constraint: ' +
          'Member must not be null',
        'Value null at \'globalSecondaryIndexes.2.member.projection\' failed to satisfy constraint: ' +
          'Member must not be null',
        'Value null at \'globalSecondaryIndexes.2.member.indexName\' failed to satisfy constraint: ' +
          'Member must not be null',
        'Value null at \'globalSecondaryIndexes.3.member.keySchema\' failed to satisfy constraint: ' +
          'Member must not be null',
        'Value null at \'globalSecondaryIndexes.3.member.projection\' failed to satisfy constraint: ' +
          'Member must not be null',
        'Value null at \'globalSecondaryIndexes.3.member.indexName\' failed to satisfy constraint: ' +
          'Member must not be null',
        'Value null at \'globalSecondaryIndexes.4.member.indexName\' failed to satisfy constraint: ' +
          'Member must not be null',
      ], done)
    })

    it('should return ValidationException for bad GlobalSecondaryIndex names', function (done) {
      var name = new Array(256 + 1).join('a')
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
        GlobalSecondaryIndexes: [ {
          IndexName: 'h;', KeySchema: [], Projection: {}, ProvisionedThroughput: { ReadCapacityUnits: 0, WriteCapacityUnits: 0 },
        }, {
          IndexName: name, KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' } ], Projection: {}, ProvisionedThroughput: {},
        } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } }, [
        'Value \'h;\' at \'globalSecondaryIndexes.1.member.indexName\' failed to satisfy constraint: ' +
          'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        'Value \'h;\' at \'globalSecondaryIndexes.1.member.indexName\' failed to satisfy constraint: ' +
          'Member must have length greater than or equal to 3',
        'Value \'0\' at \'globalSecondaryIndexes.1.member.provisionedThroughput.writeCapacityUnits\' failed to satisfy constraint: ' +
          'Member must have value greater than or equal to 1',
        'Value \'0\' at \'globalSecondaryIndexes.1.member.provisionedThroughput.readCapacityUnits\' failed to satisfy constraint: ' +
          'Member must have value greater than or equal to 1',
        'Value \'[]\' at \'globalSecondaryIndexes.1.member.keySchema\' failed to satisfy constraint: ' +
          'Member must have length greater than or equal to 1',
        'Value \'' + name + '\' at \'globalSecondaryIndexes.2.member.indexName\' failed to satisfy constraint: ' +
          'Member must have length less than or equal to 255',
        'Value null at \'globalSecondaryIndexes.2.member.provisionedThroughput.writeCapacityUnits\' failed to satisfy constraint: ' +
          'Member must not be null',
        'Value null at \'globalSecondaryIndexes.2.member.provisionedThroughput.readCapacityUnits\' failed to satisfy constraint: ' +
          'Member must not be null',
      ], done)
    })

    it('should return ValidationException for missing attribute definition with only range key with GlobalSecondaryIndex', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
        GlobalSecondaryIndexes: [ { IndexName: 'abc', KeySchema: [ { AttributeName: 'c', KeyType: 'RANGE' } ], Projection: {}, ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      new RegExp('One or more parameter values were invalid: ' +
          'Some index key attributes are not defined in AttributeDefinitions. ' +
          'Keys: \\[c\\], AttributeDefinitions: \\[(a, b|b, a)\\]'), done)
    })

    it('should return ValidationException for missing attribute definitions in GlobalSecondaryIndex', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        GlobalSecondaryIndexes: [ {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'c', KeyType: 'RANGE' }, { AttributeName: 'd', KeyType: 'RANGE' } ],
          ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
          Projection: {},
        }, {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'e', KeyType: 'RANGE' } ],
          ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
          Projection: {},
        } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      new RegExp('One or more parameter values were invalid: ' +
          'Some index key attributes are not defined in AttributeDefinitions. ' +
          'Keys: \\[(c, d|d, c)\\], AttributeDefinitions: \\[(a, b|b, a)\\]'), done)
    })

    it('should return ValidationException for first key in GlobalSecondaryIndex not being hash', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        GlobalSecondaryIndexes: [ {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'a', KeyType: 'RANGE' } ],
          ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
          Projection: {},
        } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'Invalid KeySchema: The first KeySchemaElement is not a HASH key type', done)
    })

    it('should return ValidationException for same names of keys in GlobalSecondaryIndex', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        GlobalSecondaryIndexes: [ {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'a', KeyType: 'HASH' } ],
          ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
          Projection: {},
        } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'Both the Hash Key and the Range Key element in the KeySchema have the same name', done)
    })

    it('should return ValidationException for second key of GlobalSecondaryIndex not being range', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        GlobalSecondaryIndexes: [ {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'HASH' } ],
          ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
          Projection: {},
        } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'Invalid KeySchema: The second KeySchemaElement is not a RANGE key type', done)
    })

    it('should return ValidationException about Projection if no range key in GlobalSecondaryIndex', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        GlobalSecondaryIndexes: [ {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'b', KeyType: 'HASH' } ],
          ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
          Projection: {},
        } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'One or more parameter values were invalid: Unknown ProjectionType: null', done)
    })

    it('should return ValidationException about Projection for different hash key between GlobalSecondaryIndex and table', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        GlobalSecondaryIndexes: [ {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'b', KeyType: 'HASH' }, { AttributeName: 'a', KeyType: 'RANGE' } ],
          ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
          Projection: {},
        } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'One or more parameter values were invalid: Unknown ProjectionType: null', done)
    })

    it('should return ValidationException for same named keys in GlobalSecondaryIndex when one hash and one range', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        GlobalSecondaryIndexes: [ {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'a', KeyType: 'RANGE' } ],
          ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
          Projection: {},
        } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'Both the Hash Key and the Range Key element in the KeySchema have the same name', done)
    })

    it('should return ValidationException for missing attribute definitions when hash is same in GlobalSecondaryIndex', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        GlobalSecondaryIndexes: [ {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'c', KeyType: 'RANGE' } ],
          ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
          Projection: {},
        } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      new RegExp('One or more parameter values were invalid: ' +
          'Some index key attributes are not defined in AttributeDefinitions. ' +
          'Keys: \\[(a, c|c, a)\\], AttributeDefinitions: \\[(a, b|b, a)\\]'), done)
    })

    it('should return ValidationException for empty Projection in GlobalSecondaryIndex', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        GlobalSecondaryIndexes: [ {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
          Projection: {},
        } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'One or more parameter values were invalid: Unknown ProjectionType: null', done)
    })

    it('should return ValidationException for invalid properties in GlobalSecondaryIndex', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        GlobalSecondaryIndexes: [ {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
          Projection: { NonKeyAttributes: [], ProjectionType: 'abc' },
        } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } }, [
        'Value \'abc\' at \'globalSecondaryIndexes.1.member.projection.projectionType\' failed to satisfy constraint: ' +
          'Member must satisfy enum value set: [ALL, INCLUDE, KEYS_ONLY]',
        'Value \'[]\' at \'globalSecondaryIndexes.1.member.projection.nonKeyAttributes\' failed to satisfy constraint: ' +
          'Member must have length greater than or equal to 1',
      ], done)
    })

    it('should return ValidationException for missing ProjectionType in GlobalSecondaryIndex', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        GlobalSecondaryIndexes: [ {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
          Projection: { NonKeyAttributes: [ 'a' ] },
        } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'One or more parameter values were invalid: Unknown ProjectionType: null', done)
    })

    it('should return ValidationException for NonKeyAttributes with ProjectionType ALL in GlobalSecondaryIndex', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        GlobalSecondaryIndexes: [ {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
          Projection: { ProjectionType: 'ALL', NonKeyAttributes: [ 'a' ] },
        } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'One or more parameter values were invalid: ' +
        'ProjectionType is ALL, but NonKeyAttributes is specified', done)
    })

    it('should return ValidationException for NonKeyAttributes with ProjectionType KEYS_ONLY in GlobalSecondaryIndex', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        GlobalSecondaryIndexes: [ {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
          Projection: { ProjectionType: 'KEYS_ONLY', NonKeyAttributes: [ 'a' ] },
        } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'One or more parameter values were invalid: ' +
        'ProjectionType is KEYS_ONLY, but NonKeyAttributes is specified', done)
    })

    it('should return ValidationException for duplicate index names in GlobalSecondaryIndexes', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        GlobalSecondaryIndexes: [ {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
          Projection: { ProjectionType: 'ALL' },
        }, {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
          Projection: { ProjectionType: 'ALL' },
        } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'One or more parameter values were invalid: Duplicate index name: abc', done)
    })

    it('should return ValidationException for extraneous values in GlobalSecondaryIndexes', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        GlobalSecondaryIndexes: [ {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
          Projection: { ProjectionType: 'ALL' },
        }, {
          IndexName: 'abd',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
          Projection: { ProjectionType: 'ALL' },
        }, {
          IndexName: 'abe',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
          Projection: { ProjectionType: 'ALL' },
        }, {
          IndexName: 'abf',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
          Projection: { ProjectionType: 'ALL' },
        }, {
          IndexName: 'abg',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
          Projection: { ProjectionType: 'ALL' },
        }, {
          IndexName: 'abh',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
          Projection: { ProjectionType: 'ALL' },
        }, {} ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } }, [
        'Value null at \'globalSecondaryIndexes.7.member.projection\' failed to satisfy constraint: ' +
          'Member must not be null',
        'Value null at \'globalSecondaryIndexes.7.member.indexName\' failed to satisfy constraint: ' +
          'Member must not be null',
        'Value null at \'globalSecondaryIndexes.7.member.keySchema\' failed to satisfy constraint: ' +
          'Member must not be null',
      ], done)
    })

    it('should return ValidationException for more than twenty valid GlobalSecondaryIndexes', function (done) {
      var gsis = []
      for (var i = 0; i < 21; i++) {
        gsis.push({
          IndexName: 'abc' + i,
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
          Projection: { ProjectionType: 'ALL' },
        })
      }
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        GlobalSecondaryIndexes: gsis,
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'One or more parameter values were invalid: GlobalSecondaryIndex count exceeds the per-table limit of 20', done)
    })

    it('should return ValidationException for duplicate index names between LocalSecondaryIndexes and GlobalSecondaryIndexes', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        LocalSecondaryIndexes: [ {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          Projection: { ProjectionType: 'ALL' },
        } ],
        GlobalSecondaryIndexes: [ {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
          Projection: { ProjectionType: 'ALL' },
        } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'One or more parameter values were invalid: Duplicate index name: abc', done)
    })

    it('should return LimitExceededException for more than one table with LocalSecondaryIndexes at a time', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        LocalSecondaryIndexes: [ {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          Projection: { ProjectionType: 'ALL' },
        } ],
        GlobalSecondaryIndexes: [ {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
          Projection: { ProjectionType: 'ALL' },
        } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 } },
      'One or more parameter values were invalid: Duplicate index name: abc', done)
    })

    it('should not allow ProvisionedThroughput with PAY_PER_REQUEST and GlobalSecondaryIndexes', function (done) {
      assertValidation({ TableName: 'abc',
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
        BillingMode: 'PAY_PER_REQUEST',
        GlobalSecondaryIndexes: [ {
          IndexName: 'abc',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          Projection: { ProjectionType: 'ALL' },
        }, {
          IndexName: 'abd',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
          ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
          Projection: { ProjectionType: 'ALL' },
        } ],
      }, 'One or more parameter values were invalid: ' +
        'ProvisionedThroughput should not be specified for index: abd when BillingMode is PAY_PER_REQUEST', done)
    })

  })
})