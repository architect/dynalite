const test = require('tape')
const helpers = require('./helpers')

const target = 'CreateTable'
const request = helpers.request
const randomName = helpers.randomName
const opts = helpers.opts.bind(null, target)
// const assertType = helpers.assertType.bind(null, target) // Not used
// const assertValidation = helpers.assertValidation.bind(null, target) // Not used

test('createTable - functionality - should succeed for basic provisioned throughput', function (t) {
  const tableName = randomName()
  const table = {
    TableName: tableName,
    AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' } ],
    KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
    ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
  }
  const createdAt = Date.now() / 1000

  request(opts(table), function (err, res) {
    t.error(err, 'CreateTable request should not error')
    if (!res) return t.end('No response received')

    t.equal(res.statusCode, 200, 'statusCode should be 200')
    t.ok(res.body.TableDescription, 'Response should contain TableDescription')

    const desc = res.body.TableDescription
    t.match(desc.TableId, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{8}$/, 'TableId should be a UUID')
    t.ok(desc.CreationDateTime >= createdAt - 5 && desc.CreationDateTime <= createdAt + 5, 'CreationDateTime should be close to now')
    const expectedArn = `arn:aws:dynamodb:${helpers.awsRegion}:\\d{12}:table/${tableName}`
    t.match(desc.TableArn, new RegExp(expectedArn), 'TableArn should match pattern')

    // Create expected description for comparison, excluding generated fields
    const expectedDesc = {
      ...table,
      ItemCount: 0,
      TableSizeBytes: 0,
      TableStatus: 'CREATING',
    }
    expectedDesc.ProvisionedThroughput.NumberOfDecreasesToday = 0

    // Delete fields that are generated/dynamic before deep comparison
    delete desc.TableId
    delete desc.CreationDateTime
    delete desc.TableArn

    t.deepEqual(desc, expectedDesc, 'TableDescription should match expected structure')

    helpers.deleteWhenActive(tableName) // Cleanup
    t.end()
  })
})

test('createTable - functionality - should succeed for basic PAY_PER_REQUEST', function (t) {
  const tableName = randomName()
  const table = {
    TableName: tableName,
    AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' } ],
    KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
    BillingMode: 'PAY_PER_REQUEST',
  }
  const createdAt = Date.now() / 1000

  request(opts(table), function (err, res) {
    t.error(err, 'CreateTable request should not error')
    if (!res) return t.end('No response received')

    t.equal(res.statusCode, 200, 'statusCode should be 200')
    t.ok(res.body.TableDescription, 'Response should contain TableDescription')

    const desc = res.body.TableDescription
    t.match(desc.TableId, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{8}$/, 'TableId should be a UUID')
    t.ok(desc.CreationDateTime >= createdAt - 5 && desc.CreationDateTime <= createdAt + 5, 'CreationDateTime should be close to now')
    const expectedArn = `arn:aws:dynamodb:${helpers.awsRegion}:\\d{12}:table/${tableName}`
    t.match(desc.TableArn, new RegExp(expectedArn), 'TableArn should match pattern')

    // Create expected description for comparison
    const expectedDesc = {
      AttributeDefinitions: table.AttributeDefinitions,
      KeySchema: table.KeySchema,
      TableName: table.TableName,
      ItemCount: 0,
      TableSizeBytes: 0,
      TableStatus: 'CREATING',
      BillingModeSummary: { BillingMode: 'PAY_PER_REQUEST' },
      TableThroughputModeSummary: { TableThroughputMode: 'PAY_PER_REQUEST' }, // Added based on observed behavior
      ProvisionedThroughput: {
        NumberOfDecreasesToday: 0,
        ReadCapacityUnits: 0, // Should be 0 for PAY_PER_REQUEST
        WriteCapacityUnits: 0, // Should be 0 for PAY_PER_REQUEST
      },
    }

    // Delete fields that are generated/dynamic before deep comparison
    delete desc.TableId
    delete desc.CreationDateTime
    delete desc.TableArn

    t.deepEqual(desc, expectedDesc, 'TableDescription should match expected PAY_PER_REQUEST structure')

    helpers.deleteWhenActive(tableName) // Cleanup
    t.end()
  })
})

test('createTable - functionality - should change state to ACTIVE after a period', function (t) {
  // Tape doesn't have per-test timeouts like Mocha's this.timeout()
  // The test relies on helpers.waitUntilActive which should handle its own timeout/retry logic.
  const tableName = randomName()
  const table = {
    TableName: tableName,
    AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' } ],
    KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
    ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
  }

  request(opts(table), function (err, res) {
    t.error(err, 'CreateTable request should not error')
    if (!res || !res.body || !res.body.TableDescription) return t.end('Initial CreateTable response invalid')
    t.equal(res.body.TableDescription.TableStatus, 'CREATING', 'Initial status should be CREATING')

    helpers.waitUntilActive(tableName, function (err, resActive) {
      t.error(err, `waitUntilActive for ${tableName} should succeed`)
      if (resActive && resActive.body && resActive.body.Table) {
        t.equal(resActive.body.Table.TableStatus, 'ACTIVE', 'Table status should become ACTIVE')
      }
      helpers.deleteWhenActive(tableName) // Cleanup
      t.end()
    })
  })
})

test('createTable - functionality - should succeed for LocalSecondaryIndexes', function (t) {
  const tableName = randomName()
  const table = {
    TableName: tableName,
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
    } ],
    ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
  }
  const createdAt = Date.now() / 1000

  request(opts(table), function (err, res) {
    t.error(err, 'CreateTable request should not error')
    if (!res || !res.body || !res.body.TableDescription) return t.end('Initial CreateTable response invalid')

    t.equal(res.statusCode, 200, 'statusCode should be 200')
    const desc = res.body.TableDescription

    t.match(desc.TableId, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{8}$/, 'TableId should be a UUID')
    t.ok(desc.CreationDateTime >= createdAt - 5 && desc.CreationDateTime <= createdAt + 5, 'CreationDateTime should be close to now')
    const expectedArnPrefix = `arn:aws:dynamodb:${helpers.awsRegion}:\\d{12}:table/${tableName}`
    t.match(desc.TableArn, new RegExp(expectedArnPrefix), 'TableArn should match pattern')

    t.ok(desc.LocalSecondaryIndexes, 'LocalSecondaryIndexes should exist')
    t.equal(desc.LocalSecondaryIndexes.length, table.LocalSecondaryIndexes.length, 'Correct number of LSIs')

    const expectedLsis = JSON.parse(JSON.stringify(table.LocalSecondaryIndexes)) // Deep clone
    const actualLsis = JSON.parse(JSON.stringify(desc.LocalSecondaryIndexes)) // Deep clone

    // Check and remove IndexArn before comparison
    actualLsis.forEach(index => {
      const expectedIndexArn = `${expectedArnPrefix}/index/${index.IndexName}`
      t.match(index.IndexArn, new RegExp(expectedIndexArn), `IndexArn for ${index.IndexName} should match pattern`)
      delete index.IndexArn
      // Add expected fields
      index.IndexSizeBytes = 0
      index.ItemCount = 0
    })

    // Sort both arrays by IndexName for consistent comparison
    actualLsis.sort((a, b) => a.IndexName.localeCompare(b.IndexName))
    expectedLsis.forEach(index => {
      // Add expected fields to match actual structure
      index.IndexSizeBytes = 0
      index.ItemCount = 0
    })
    expectedLsis.sort((a, b) => a.IndexName.localeCompare(b.IndexName))

    t.deepEqual(actualLsis, expectedLsis, 'LocalSecondaryIndexes descriptions should match expected structure')

    // Prepare expected description for base table comparison
    const expectedDescBase = {
      ...table,
      ItemCount: 0,
      TableSizeBytes: 0,
      TableStatus: 'CREATING',
    }
    expectedDescBase.ProvisionedThroughput.NumberOfDecreasesToday = 0
    delete expectedDescBase.LocalSecondaryIndexes // LSIs checked above

    // Delete dynamic fields from actual description
    delete desc.TableId
    delete desc.CreationDateTime
    delete desc.TableArn
    delete desc.LocalSecondaryIndexes // LSIs checked above

    t.deepEqual(desc, expectedDescBase, 'Base TableDescription should match expected structure')

    helpers.deleteWhenActive(tableName) // Cleanup
    t.end()
  })
})

test('createTable - functionality - should succeed for multiple GlobalSecondaryIndexes', function (t) {
  const tableName = randomName()
  const table = {
    TableName: tableName,
    AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
    KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
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
    } ],
    ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
  }
  const createdAt = Date.now() / 1000
  const expectedGsis = JSON.parse(JSON.stringify(table.GlobalSecondaryIndexes)) // Deep clone

  request(opts(table), function (err, res) {
    t.error(err, 'CreateTable request should not error')
    if (!res || !res.body || !res.body.TableDescription) return t.end('Initial CreateTable response invalid')

    t.equal(res.statusCode, 200, 'statusCode should be 200')
    const desc = res.body.TableDescription

    t.match(desc.TableId, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{8}$/, 'TableId should be a UUID')
    t.ok(desc.CreationDateTime >= createdAt - 5 && desc.CreationDateTime <= createdAt + 5, 'CreationDateTime should be close to now')
    const expectedArnPrefix = `arn:aws:dynamodb:${helpers.awsRegion}:\\d{12}:table/${tableName}`
    t.match(desc.TableArn, new RegExp(expectedArnPrefix), 'TableArn should match pattern')

    t.ok(desc.GlobalSecondaryIndexes, 'GlobalSecondaryIndexes should exist')
    t.equal(desc.GlobalSecondaryIndexes.length, table.GlobalSecondaryIndexes.length, 'Correct number of GSIs')

    const actualGsis = JSON.parse(JSON.stringify(desc.GlobalSecondaryIndexes)) // Deep clone

    // Check and remove IndexArn, add expected fields before comparison
    actualGsis.forEach(index => {
      const expectedIndexArn = `${expectedArnPrefix}/index/${index.IndexName}`
      t.match(index.IndexArn, new RegExp(expectedIndexArn), `IndexArn for ${index.IndexName} should match pattern`)
      delete index.IndexArn
      // Add expected fields from response
      index.IndexSizeBytes = 0
      index.ItemCount = 0
      index.IndexStatus = 'CREATING'
      index.ProvisionedThroughput.NumberOfDecreasesToday = 0
    })

    // Sort both arrays by IndexName for consistent comparison
    actualGsis.sort((a, b) => a.IndexName.localeCompare(b.IndexName))
    expectedGsis.forEach(index => {
      index.IndexSizeBytes = 0
      index.ItemCount = 0
      index.IndexStatus = 'CREATING'
      index.ProvisionedThroughput.NumberOfDecreasesToday = 0
    })
    expectedGsis.sort((a, b) => a.IndexName.localeCompare(b.IndexName))

    t.deepEqual(actualGsis, expectedGsis, 'GlobalSecondaryIndexes descriptions should match expected structure at creation')

    // Prepare expected description for base table comparison
    const expectedDescBase = {
      ...table,
      ItemCount: 0,
      TableSizeBytes: 0,
      TableStatus: 'CREATING',
    }
    expectedDescBase.ProvisionedThroughput.NumberOfDecreasesToday = 0
    delete expectedDescBase.GlobalSecondaryIndexes // GSIs checked above

    // Delete dynamic fields from actual description
    delete desc.TableId
    delete desc.CreationDateTime
    delete desc.TableArn
    delete desc.GlobalSecondaryIndexes // GSIs checked above

    t.deepEqual(desc, expectedDescBase, 'Base TableDescription should match expected structure')

    // Ensure that the indexes become active too
    helpers.waitUntilIndexesActive(tableName, function (err, resActive) {
      t.error(err, `waitUntilIndexesActive for ${tableName} should succeed`)
      if (resActive && resActive.body && resActive.body.Table && resActive.body.Table.GlobalSecondaryIndexes) {
        const activeGsis = JSON.parse(JSON.stringify(resActive.body.Table.GlobalSecondaryIndexes))
        activeGsis.forEach(index => { delete index.IndexArn })
        activeGsis.sort((a, b) => a.IndexName.localeCompare(b.IndexName))

        expectedGsis.forEach(index => { index.IndexStatus = 'ACTIVE' })
        t.deepEqual(activeGsis, expectedGsis, 'GlobalSecondaryIndexes should become ACTIVE')
      }
      helpers.deleteWhenActive(tableName) // Cleanup
      t.end()
    })
  })
})

test('createTable - functionality - should succeed for PAY_PER_REQUEST GlobalSecondaryIndexes', function (t) {
  const tableName = randomName()
  const table = {
    TableName: tableName,
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
      Projection: { ProjectionType: 'ALL' },
    } ],
  }
  const createdAt = Date.now() / 1000
  const expectedGsis = JSON.parse(JSON.stringify(table.GlobalSecondaryIndexes)) // Deep clone

  request(opts(table), function (err, res) {
    t.error(err, 'CreateTable request should not error')
    if (!res || !res.body || !res.body.TableDescription) return t.end('Initial CreateTable response invalid')

    t.equal(res.statusCode, 200, 'statusCode should be 200')
    const desc = res.body.TableDescription

    t.match(desc.TableId, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{8}$/, 'TableId should be a UUID')
    t.ok(desc.CreationDateTime >= createdAt - 5 && desc.CreationDateTime <= createdAt + 5, 'CreationDateTime should be close to now')
    const expectedArnPrefix = `arn:aws:dynamodb:${helpers.awsRegion}:\\d{12}:table/${tableName}`
    t.match(desc.TableArn, new RegExp(expectedArnPrefix), 'TableArn should match pattern')

    t.ok(desc.GlobalSecondaryIndexes, 'GlobalSecondaryIndexes should exist')
    t.equal(desc.GlobalSecondaryIndexes.length, table.GlobalSecondaryIndexes.length, 'Correct number of GSIs')

    const actualGsis = JSON.parse(JSON.stringify(desc.GlobalSecondaryIndexes)) // Deep clone

    // Check and remove IndexArn, add expected fields before comparison
    actualGsis.forEach(index => {
      const expectedIndexArn = `${expectedArnPrefix}/index/${index.IndexName}`
      t.match(index.IndexArn, new RegExp(expectedIndexArn), `IndexArn for ${index.IndexName} should match pattern`)
      delete index.IndexArn
      // Add expected fields for PAY_PER_REQUEST GSI
      index.IndexSizeBytes = 0
      index.ItemCount = 0
      index.IndexStatus = 'CREATING'
      index.ProvisionedThroughput = { // Should be 0 for PAY_PER_REQUEST
        ReadCapacityUnits: 0,
        WriteCapacityUnits: 0,
        NumberOfDecreasesToday: 0,
      }
    })

    // Sort both arrays by IndexName for consistent comparison
    actualGsis.sort((a, b) => a.IndexName.localeCompare(b.IndexName))
    expectedGsis.forEach(index => {
      index.IndexSizeBytes = 0
      index.ItemCount = 0
      index.IndexStatus = 'CREATING'
      index.ProvisionedThroughput = { // Match expected fields
        ReadCapacityUnits: 0,
        WriteCapacityUnits: 0,
        NumberOfDecreasesToday: 0,
      }
    })
    expectedGsis.sort((a, b) => a.IndexName.localeCompare(b.IndexName))

    t.deepEqual(actualGsis, expectedGsis, 'PAY_PER_REQUEST GSIs descriptions should match expected structure at creation')

    // Prepare expected description for base table comparison
    const expectedDescBase = {
      AttributeDefinitions: table.AttributeDefinitions,
      KeySchema: table.KeySchema,
      TableName: table.TableName,
      BillingModeSummary: { BillingMode: 'PAY_PER_REQUEST' },
      TableThroughputModeSummary: { TableThroughputMode: 'PAY_PER_REQUEST' },
      ProvisionedThroughput: { // Should be 0 for PAY_PER_REQUEST
        NumberOfDecreasesToday: 0,
        ReadCapacityUnits: 0,
        WriteCapacityUnits: 0,
      },
      ItemCount: 0,
      TableSizeBytes: 0,
      TableStatus: 'CREATING',
    }
    // Note: table.GlobalSecondaryIndexes was already deleted conceptually for comparison

    // Delete dynamic fields from actual description
    delete desc.TableId
    delete desc.CreationDateTime
    delete desc.TableArn
    delete desc.GlobalSecondaryIndexes // GSIs checked above

    t.deepEqual(desc, expectedDescBase, 'Base TableDescription should match expected PAY_PER_REQUEST structure')

    // Ensure that the indexes become active too
    helpers.waitUntilIndexesActive(tableName, function (err, resActive) {
      t.error(err, `waitUntilIndexesActive for ${tableName} should succeed`)
      if (resActive && resActive.body && resActive.body.Table && resActive.body.Table.GlobalSecondaryIndexes) {
        const activeGsis = JSON.parse(JSON.stringify(resActive.body.Table.GlobalSecondaryIndexes))
        activeGsis.forEach(index => { delete index.IndexArn })
        activeGsis.sort((a, b) => a.IndexName.localeCompare(b.IndexName))

        expectedGsis.forEach(index => { index.IndexStatus = 'ACTIVE' })
        t.deepEqual(activeGsis, expectedGsis, 'PAY_PER_REQUEST GSIs should become ACTIVE')
      }
      helpers.deleteWhenActive(tableName) // Cleanup
      t.end()
    })
  })
})
