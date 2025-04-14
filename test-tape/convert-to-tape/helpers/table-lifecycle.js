// helpers/table-lifecycle.js
const async = require('async')
const config = require('./config')
const { request, opts } = require('./request')
const { randomName } = require('./random')

// Define table names based on environment
const testHashTable = config.useRemoteDynamo ? '__dynalite_test_1' : randomName()
const testHashNTable = config.useRemoteDynamo ? '__dynalite_test_2' : randomName()
const testRangeTable = config.useRemoteDynamo ? '__dynalite_test_3' : randomName()
const testRangeNTable = config.useRemoteDynamo ? '__dynalite_test_4' : randomName()
const testRangeBTable = config.useRemoteDynamo ? '__dynalite_test_5' : randomName()

function createTestTables (done) {
  if (config.useRemoteDynamo && !config.CREATE_REMOTE_TABLES) return done()

  const tables = [
    {
      TableName: testHashTable,
      AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' } ],
      KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
      ProvisionedThroughput: { ReadCapacityUnits: config.readCapacity, WriteCapacityUnits: config.writeCapacity },
    }, {
      TableName: testHashNTable,
      AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'N' } ],
      KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
      BillingMode: 'PAY_PER_REQUEST',
    }, {
      TableName: testRangeTable,
      AttributeDefinitions: [
        { AttributeName: 'a', AttributeType: 'S' },
        { AttributeName: 'b', AttributeType: 'S' },
        { AttributeName: 'c', AttributeType: 'S' },
        { AttributeName: 'd', AttributeType: 'S' },
      ],
      KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
      ProvisionedThroughput: { ReadCapacityUnits: config.readCapacity, WriteCapacityUnits: config.writeCapacity },
      LocalSecondaryIndexes: [
        {
          IndexName: 'index1',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'c', KeyType: 'RANGE' } ],
          Projection: { ProjectionType: 'ALL' },
        }, {
          IndexName: 'index2',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'd', KeyType: 'RANGE' } ],
          Projection: { ProjectionType: 'INCLUDE', NonKeyAttributes: [ 'c' ] },
        }
      ],
      GlobalSecondaryIndexes: [
        {
          IndexName: 'index3',
          KeySchema: [ { AttributeName: 'c', KeyType: 'HASH' } ],
          ProvisionedThroughput: { ReadCapacityUnits: config.readCapacity, WriteCapacityUnits: config.writeCapacity },
          Projection: { ProjectionType: 'ALL' },
        }, {
          IndexName: 'index4',
          KeySchema: [ { AttributeName: 'c', KeyType: 'HASH' }, { AttributeName: 'd', KeyType: 'RANGE' } ],
          ProvisionedThroughput: { ReadCapacityUnits: config.readCapacity, WriteCapacityUnits: config.writeCapacity },
          Projection: { ProjectionType: 'INCLUDE', NonKeyAttributes: [ 'e' ] },
        }
      ],
    }, {
      TableName: testRangeNTable,
      AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'N' } ],
      KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
      ProvisionedThroughput: { ReadCapacityUnits: config.readCapacity, WriteCapacityUnits: config.writeCapacity },
    }, {
      TableName: testRangeBTable,
      AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'B' } ],
      KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
      ProvisionedThroughput: { ReadCapacityUnits: config.readCapacity, WriteCapacityUnits: config.writeCapacity },
    }
  ]
  async.forEach(tables, createAndWait, done)
}

function getAccountId (done) {
  request(opts('DescribeTable', { TableName: testHashTable }), (err, res) => {
    if (err) return done(err)
    try {
      const accountId = res.body.Table.TableArn.split(':')[4]
      config.setAwsAccountId(accountId) // Update config
      done()
    }
    catch (e) {
      done(new Error(`Failed to parse TableArn from DescribeTable response: ${res.rawBody}`))
    }
  })
}

function deleteTestTables (done) {
  if (config.useRemoteDynamo && !config.DELETE_REMOTE_TABLES) return done()
  request(opts('ListTables', {}), (err, res) => {
    if (err) return done(err)
    const names = res.body.TableNames.filter((name) => name.indexOf(config.prefix) === 0)
    async.forEach(names, deleteAndWait, done)
  })
}

function createAndWait (table, done) {
  request(opts('CreateTable', table), (err, res) => {
    if (err) return done(err)
    if (res.statusCode != 200) return done(new Error(`${res.statusCode}: ${JSON.stringify(res.body)}`))
    setTimeout(waitUntilActive, 1000, table.TableName, done)
  })
}

function deleteAndWait (name, done) {
  request(opts('DeleteTable', { TableName: name }), (err, res) => {
    if (err) return done(err)
    if (res.body && res.body.__type === 'com.amazonaws.dynamodb.v20120810#ResourceInUseException') {
      return setTimeout(deleteAndWait, 1000, name, done)
    }
    else if (res.statusCode != 200) {
      return done(new Error(`${res.statusCode}: ${JSON.stringify(res.body)}`))
    }
    setTimeout(waitUntilDeleted, 1000, name, done)
  })
}

function waitUntilActive (name, done) {
  request(opts('DescribeTable', { TableName: name }), (err, res) => {
    if (err) return done(err)
    if (res.statusCode != 200) return done(new Error(`${res.statusCode}: ${JSON.stringify(res.body)}`))
    if (res.body.Table.TableStatus === 'ACTIVE' &&
            (!res.body.Table.GlobalSecondaryIndexes ||
             res.body.Table.GlobalSecondaryIndexes.every((index) => index.IndexStatus === 'ACTIVE'))) {
      return done(null, res)
    }
    setTimeout(waitUntilActive, 1000, name, done)
  })
}

function waitUntilDeleted (name, done) {
  request(opts('DescribeTable', { TableName: name }), (err, res) => {
    if (err) return done(err)
    if (res.body && res.body.__type === 'com.amazonaws.dynamodb.v20120810#ResourceNotFoundException') {
      return done(null, res)
    }
    else if (res.statusCode != 200) {
      return done(new Error(`${res.statusCode}: ${JSON.stringify(res.body)}`))
    }
    setTimeout(waitUntilDeleted, 1000, name, done)
  })
}

function waitUntilIndexesActive (name, done) {
  request(opts('DescribeTable', { TableName: name }), (err, res) => {
    if (err) return done(err)
    if (res.statusCode != 200) {
      return done(new Error(`${res.statusCode}: ${JSON.stringify(res.body)}`))
    }
    else if (res.body.Table.GlobalSecondaryIndexes && res.body.Table.GlobalSecondaryIndexes.every((index) => index.IndexStatus === 'ACTIVE')) {
      return done(null, res)
    }
    else if (!res.body.Table.GlobalSecondaryIndexes) {
      // Handle case where there are no GSIs - table is active, indexes are technically active
      return done(null, res)
    }
    setTimeout(waitUntilIndexesActive, 1000, name, done)
  })
}

function deleteWhenActive (name, done) {
  if (!done) done = function () {}
  waitUntilActive(name, (err) => {
    if (err) return done(err)
    request(opts('DeleteTable', { TableName: name }), done)
  })
}

module.exports = {
  // Table names
  testHashTable,
  testHashNTable,
  testRangeTable,
  testRangeNTable,
  testRangeBTable,
  // Lifecycle functions
  createTestTables,
  getAccountId,
  deleteTestTables,
  createAndWait,
  deleteAndWait,
  waitUntilActive,
  waitUntilDeleted,
  waitUntilIndexesActive,
  deleteWhenActive,
}
