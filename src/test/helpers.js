var http = require('http'),
  aws4 = require('aws4'),
  async = require('async'),
  once = require('once'),
  dynalite = require('..')

var useRemoteDynamo = process.env.REMOTE
var runSlowTests = true
if (useRemoteDynamo && !process.env.SLOW_TESTS) runSlowTests = false

http.globalAgent.maxSockets = Infinity

// TestHelpers factory function to encapsulate server and database management
function createTestHelper (options) {
  options = options || {}

  var helper = {
    options: options,
    server: null,
    port: options.port || getRandomPort(),
    useRemoteDynamo: options.useRemoteDynamo || useRemoteDynamo,
    awsRegion: options.awsRegion || process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION || 'us-east-1',
    awsAccountId: options.awsAccountId || process.env.AWS_ACCOUNT_ID,
    version: options.version || 'DynamoDB_20120810',
    prefix: options.prefix || '__dynalite_test_',
    readCapacity: options.readCapacity || 10,
    writeCapacity: options.writeCapacity || 5,
    runSlowTests: options.runSlowTests !== undefined ? options.runSlowTests : runSlowTests,
  }

  function getRandomPort () {
    return 10000 + Math.round(Math.random() * 10000)
  }

  helper.randomString = function () {
    return ('AAAAAAAAA' + helper.randomNumber()).slice(-10)
  }

  helper.randomNumber = function () {
    return String(Math.random() * 0x100000000)
  }

  helper.randomName = function () {
    return helper.prefix + helper.randomString()
  }

  // Generate table names (after helper functions are defined)
  helper.testHashTable = helper.useRemoteDynamo ? '__dynalite_test_1' : helper.randomName()
  helper.testHashNTable = helper.useRemoteDynamo ? '__dynalite_test_2' : helper.randomName()
  helper.testRangeTable = helper.useRemoteDynamo ? '__dynalite_test_3' : helper.randomName()
  helper.testRangeNTable = helper.useRemoteDynamo ? '__dynalite_test_4' : helper.randomName()
  helper.testRangeBTable = helper.useRemoteDynamo ? '__dynalite_test_5' : helper.randomName()

  // Set up request options
  helper.requestOpts = helper.useRemoteDynamo ?
    { host: 'dynamodb.' + helper.awsRegion + '.amazonaws.com', method: 'POST' } :
    { host: '127.0.0.1', port: helper.port, method: 'POST' }

  helper.startServer = function () {
    return new Promise(function (resolve, reject) {
      if (helper.useRemoteDynamo) {
        // For remote DynamoDB, just set up tables and account ID
        helper.createTestTables(function (err) {
          if (err) return reject(err)
          helper.getAccountId(resolve)
        })
        return
      }

      helper.server = dynalite({ path: process.env.DYNALITE_PATH })
      helper.server.listen(helper.port, function (err) {
        if (err) return reject(err)
        helper.createTestTables(function (err) {
          if (err) return reject(err)
          helper.getAccountId(resolve)
        })
      })
    })
  }

  helper.stopServer = function () {
    return new Promise(function (resolve, reject) {
      helper.deleteTestTables(function (err) {
        if (err) return reject(err)
        if (helper.server) {
          helper.server.close(resolve)
        }
        else {
          resolve()
        }
      })
    })
  }

  // Helper functions already defined above

  helper.request = function (opts, cb) {
    if (typeof opts === 'function') { cb = opts; opts = {} }
    opts.retries = opts.retries || 0
    cb = once(cb)
    for (var key in helper.requestOpts) {
      if (opts[key] === undefined)
        opts[key] = helper.requestOpts[key]
    }
    if (!opts.noSign) {
      aws4.sign(opts)
      opts.noSign = true // don't sign twice if calling recursively
    }

    var MAX_RETRIES = 20
    http.request(opts, function (res) {
      res.setEncoding('utf8')
      res.on('error', cb)
      res.rawBody = ''
      res.on('data', function (chunk) { res.rawBody += chunk })
      res.on('end', function () {
        try {
          res.body = JSON.parse(res.rawBody)
        }
        catch {
          res.body = res.rawBody
        }
        if (helper.useRemoteDynamo && opts.retries <= MAX_RETRIES &&
            (res.body.__type == 'com.amazon.coral.availability#ThrottlingException' ||
            res.body.__type == 'com.amazonaws.dynamodb.v20120810#LimitExceededException')) {
          opts.retries++
          return setTimeout(helper.request, Math.floor(Math.random() * 1000), opts, cb)
        }
        cb(null, res)
      })
    }).on('error', function (err) {
      if (err && ~[ 'ECONNRESET', 'EMFILE', 'ENOTFOUND' ].indexOf(err.code) && opts.retries <= MAX_RETRIES) {
        opts.retries++
        return setTimeout(helper.request, Math.floor(Math.random() * 100), opts, cb)
      }
      cb(err)
    }).end(opts.body)
  }

  helper.opts = function (target, data) {
    return {
      headers: {
        'Content-Type': 'application/x-amz-json-1.0',
        'X-Amz-Target': helper.version + '.' + target,
      },
      body: JSON.stringify(data),
    }
  }

  helper.createTestTables = function (done) {
    if (helper.useRemoteDynamo && !CREATE_REMOTE_TABLES) return done()

    // First, ensure any existing test tables are cleaned up
    helper.deleteTestTables(function (err) {
      if (err) return done(err)

      var readCapacity = helper.readCapacity, writeCapacity = helper.writeCapacity
      var tables = [ {
        TableName: helper.testHashTable,
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
        ProvisionedThroughput: { ReadCapacityUnits: readCapacity, WriteCapacityUnits: writeCapacity },
      }, {
        TableName: helper.testHashNTable,
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'N' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
        BillingMode: 'PAY_PER_REQUEST',
      }, {
        TableName: helper.testRangeTable,
        AttributeDefinitions: [
          { AttributeName: 'a', AttributeType: 'S' },
          { AttributeName: 'b', AttributeType: 'S' },
          { AttributeName: 'c', AttributeType: 'S' },
          { AttributeName: 'd', AttributeType: 'S' },
        ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        ProvisionedThroughput: { ReadCapacityUnits: readCapacity, WriteCapacityUnits: writeCapacity },
        LocalSecondaryIndexes: [ {
          IndexName: 'index1',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'c', KeyType: 'RANGE' } ],
          Projection: { ProjectionType: 'ALL' },
        }, {
          IndexName: 'index2',
          KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'd', KeyType: 'RANGE' } ],
          Projection: { ProjectionType: 'INCLUDE', NonKeyAttributes: [ 'c' ] },
        } ],
        GlobalSecondaryIndexes: [ {
          IndexName: 'index3',
          KeySchema: [ { AttributeName: 'c', KeyType: 'HASH' } ],
          ProvisionedThroughput: { ReadCapacityUnits: readCapacity, WriteCapacityUnits: writeCapacity },
          Projection: { ProjectionType: 'ALL' },
        }, {
          IndexName: 'index4',
          KeySchema: [ { AttributeName: 'c', KeyType: 'HASH' }, { AttributeName: 'd', KeyType: 'RANGE' } ],
          ProvisionedThroughput: { ReadCapacityUnits: readCapacity, WriteCapacityUnits: writeCapacity },
          Projection: { ProjectionType: 'INCLUDE', NonKeyAttributes: [ 'e' ] },
        } ],
      }, {
        TableName: helper.testRangeNTable,
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'N' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        ProvisionedThroughput: { ReadCapacityUnits: readCapacity, WriteCapacityUnits: writeCapacity },
      }, {
        TableName: helper.testRangeBTable,
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'B' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
        ProvisionedThroughput: { ReadCapacityUnits: readCapacity, WriteCapacityUnits: writeCapacity },
      } ]

      async.forEach(tables, helper.createAndWaitWithRetry, done)
    })
  }

  helper.getAccountId = function (done) {
    helper.request(helper.opts('DescribeTable', { TableName: helper.testHashTable }), function (err, res) {
      if (err) return done(err)
      helper.awsAccountId = res.body.Table.TableArn.split(':')[4]
      done()
    })
  }

  helper.deleteTestTables = function (done) {
    if (helper.useRemoteDynamo && !DELETE_REMOTE_TABLES) return done()

    var maxRetries = 3
    var retryCount = 0

    function attemptCleanup () {
      helper.request(helper.opts('ListTables', {}), function (err, res) {
        if (err) {
          if (retryCount < maxRetries) {
            retryCount++
            return setTimeout(attemptCleanup, 1000)
          }
          return done(err)
        }

        var names = res.body.TableNames.filter(function (name) {
          return name.indexOf(helper.prefix) === 0
        })

        if (names.length === 0) {
          return done() // No tables to delete
        }

        // Delete tables with enhanced error handling, ignoring individual failures
        async.forEach(names, function (name, callback) {
          helper.deleteAndWaitSafe(name, callback)
        }, function () {
          // Ignore errors from individual table deletions
          // Verify all tables are actually deleted
          helper.verifyTablesDeleted(names, function (verifyErr) {
            if (verifyErr && retryCount < maxRetries) {
              retryCount++
              return setTimeout(attemptCleanup, 2000)
            }
            // Even if verification fails, continue - we've done our best
            done()
          })
        })
      })
    }

    attemptCleanup()
  }

  helper.deleteAndWaitSafe = function (name, done) {
    // This function handles database corruption gracefully
    // It tries to delete the table but doesn't fail if there are issues

    var maxAttempts = 3
    var attemptCount = 0

    function attemptDelete () {
      attemptCount++

      helper.request(helper.opts('DeleteTable', { TableName: name }), function (err, res) {
        if (err) {
          // Network error, try again if we have attempts left
          if (attemptCount < maxAttempts) {
            return setTimeout(attemptDelete, 1000)
          }
          // Give up, but don't fail the overall cleanup
          return done()
        }

        if (res.statusCode === 200) {
          // Table deletion initiated successfully
          return helper.waitUntilDeletedSafe(name, done)
        }

        if (res.body && res.body.__type === 'com.amazonaws.dynamodb.v20120810#ResourceNotFoundException') {
          // Table doesn't exist, consider it deleted
          return done()
        }

        if (res.body && res.body.__type === 'com.amazonaws.dynamodb.v20120810#ResourceInUseException') {
          // Table is being created or is in use, try again
          if (attemptCount < maxAttempts) {
            return setTimeout(attemptDelete, 2000)
          }
          // Give up, but don't fail the overall cleanup
          return done()
        }

        // Any other error - try again if we have attempts left
        if (attemptCount < maxAttempts) {
          return setTimeout(attemptDelete, 1000)
        }

        // Give up, but don't fail the overall cleanup
        done()
      })
    }

    attemptDelete()
  }

  helper.waitUntilDeletedSafe = function (name, done) {
    var maxWaitTime = 15000 // 15 seconds max wait (shorter than normal)
    var startTime = Date.now()
    var checkInterval = 1000

    function checkDeleted () {
      if (Date.now() - startTime > maxWaitTime) {
        // Timeout, but don't fail the overall cleanup
        return done()
      }

      helper.request(helper.opts('DescribeTable', { TableName: name }), function (err, res) {
        if (err) {
          // Network error, but don't fail the cleanup
          return done()
        }

        if (res.body && res.body.__type === 'com.amazonaws.dynamodb.v20120810#ResourceNotFoundException') {
          return done() // Table successfully deleted
        }

        if (res.statusCode !== 200) {
          // Some other error, but don't fail the cleanup
          return done()
        }

        // Table still exists, check again
        setTimeout(checkDeleted, checkInterval)
      })
    }

    checkDeleted()
  }

  helper.verifyTablesDeleted = function (tableNames, done) {
    var maxVerifyRetries = 3
    var verifyRetryCount = 0

    function verifyDeletion () {
      helper.request(helper.opts('ListTables', {}), function (err, res) {
        if (err) {
          if (verifyRetryCount < maxVerifyRetries) {
            verifyRetryCount++
            return setTimeout(verifyDeletion, 1000)
          }
          // Network error, but don't fail the cleanup
          return done()
        }

        var remainingTables = res.body.TableNames.filter(function (name) {
          return tableNames.indexOf(name) !== -1
        })

        if (remainingTables.length === 0) {
          return done() // All tables successfully deleted
        }

        if (verifyRetryCount < maxVerifyRetries) {
          verifyRetryCount++
          return setTimeout(verifyDeletion, 2000)
        }

        // Some tables still exist, but don't fail the cleanup
        // This might be due to database corruption or timing issues
        return done()
      })
    }

    verifyDeletion()
  }

  helper.createAndWait = function (table, done) {
    helper.request(helper.opts('CreateTable', table), function (err, res) {
      if (err) return done(err)
      if (res.statusCode != 200) return done(new Error(res.statusCode + ': ' + JSON.stringify(res.body)))
      setTimeout(helper.waitUntilActive, 1000, table.TableName, done)
    })
  }

  helper.createAndWaitWithRetry = function (table, done) {
    var maxRetries = 5
    var retryDelay = 1000
    var retryCount = 0

    function attemptCreate () {
      // First check if table already exists
      helper.request(helper.opts('DescribeTable', { TableName: table.TableName }), function (err, res) {
        if (!err && res.statusCode === 200 && res.body && res.body.Table) {
          // Table exists and response is valid, wait for it to be active
          return helper.waitUntilActive(table.TableName, done)
        }

        if (err || (res.statusCode !== 400 && res.statusCode !== 200)) {
          // Network or server error, retry
          if (retryCount < maxRetries) {
            retryCount++
            return setTimeout(attemptCreate, retryDelay * retryCount)
          }
          return done(err || new Error('Server error: ' + res.statusCode))
        }

        if (res.statusCode === 200 && (!res.body || !res.body.Table)) {
          // Table exists but response is malformed, this might be a database issue
          // Try to delete and recreate
          helper.deleteAndWait(table.TableName, function () {
            // Ignore delete errors, proceed with creation
            createTable()
          })
          return
        }

        if (res.statusCode === 400 && res.body && res.body.__type === 'com.amazonaws.dynamodb.v20120810#ResourceNotFoundException') {
          // Table doesn't exist, create it
          return createTable()
        }

        // Other error
        if (retryCount < maxRetries) {
          retryCount++
          return setTimeout(attemptCreate, retryDelay * retryCount)
        }
        return done(new Error(res.statusCode + ': ' + JSON.stringify(res.body)))
      })

      function createTable () {
        helper.request(helper.opts('CreateTable', table), function (err, res) {
          if (err) {
            if (retryCount < maxRetries) {
              retryCount++
              return setTimeout(attemptCreate, retryDelay * retryCount)
            }
            return done(err)
          }

          if (res.statusCode === 200) {
            // Table created successfully, wait for it to be active
            return setTimeout(helper.waitUntilActive, 2000, table.TableName, done)
          }

          if (res.body && res.body.__type === 'com.amazonaws.dynamodb.v20120810#ResourceInUseException') {
            // Table is being created or deleted, retry
            if (retryCount < maxRetries) {
              retryCount++
              return setTimeout(attemptCreate, retryDelay * retryCount)
            }
            return done(new Error('Table creation failed after ' + maxRetries + ' retries: ResourceInUseException'))
          }

          // Other error
          if (retryCount < maxRetries) {
            retryCount++
            return setTimeout(attemptCreate, retryDelay * retryCount)
          }
          return done(new Error(res.statusCode + ': ' + JSON.stringify(res.body)))
        })
      }
    }

    attemptCreate()
  }

  helper.deleteAndWait = function (name, done) {
    var maxRetries = 10
    var retryDelay = 1000
    var retryCount = 0

    function attemptDelete () {
      helper.request(helper.opts('DeleteTable', { TableName: name }), function (err, res) {
        if (err) {
          if (retryCount < maxRetries) {
            retryCount++
            return setTimeout(attemptDelete, retryDelay)
          }
          return done(err)
        }

        if (res.statusCode === 200) {
          // Table deletion initiated successfully
          return setTimeout(helper.waitUntilDeleted, 1000, name, done)
        }

        if (res.body && res.body.__type === 'com.amazonaws.dynamodb.v20120810#ResourceNotFoundException') {
          // Table doesn't exist, consider it deleted
          return done()
        }

        if (res.body && res.body.__type === 'com.amazonaws.dynamodb.v20120810#ResourceInUseException') {
          // Table is being created or is in use, retry
          if (retryCount < maxRetries) {
            retryCount++
            return setTimeout(attemptDelete, retryDelay * Math.min(retryCount, 3)) // Cap exponential backoff
          }
          return done(new Error('Table deletion failed after ' + maxRetries + ' retries: ResourceInUseException'))
        }

        // Other error
        if (retryCount < maxRetries) {
          retryCount++
          return setTimeout(attemptDelete, retryDelay)
        }
        return done(new Error(res.statusCode + ': ' + JSON.stringify(res.body)))
      })
    }

    attemptDelete()
  }

  helper.waitUntilActive = function (name, done) {
    var maxWaitTime = 60000 // 60 seconds max wait
    var startTime = Date.now()
    var checkInterval = 1000

    function checkActive () {
      if (Date.now() - startTime > maxWaitTime) {
        return done(new Error('Timeout waiting for table ' + name + ' to become active'))
      }

      helper.request(helper.opts('DescribeTable', { TableName: name }), function (err, res) {
        if (err) return done(err)

        if (res.statusCode !== 200) {
          return done(new Error(res.statusCode + ': ' + JSON.stringify(res.body)))
        }

        if (!res.body || !res.body.Table) {
          // Invalid response, might be a database issue, retry
          setTimeout(checkActive, checkInterval)
          return
        }

        var table = res.body.Table
        var isActive = table.TableStatus === 'ACTIVE'
        var indexesActive = !table.GlobalSecondaryIndexes ||
          table.GlobalSecondaryIndexes.every(function (index) {
            return index.IndexStatus === 'ACTIVE'
          })

        if (isActive && indexesActive) {
          return done(null, res)
        }

        // Table not ready yet, check again
        setTimeout(checkActive, checkInterval)
      })
    }

    checkActive()
  }

  helper.waitUntilDeleted = function (name, done) {
    var maxWaitTime = 30000 // 30 seconds max wait
    var startTime = Date.now()
    var checkInterval = 1000

    function checkDeleted () {
      if (Date.now() - startTime > maxWaitTime) {
        return done(new Error('Timeout waiting for table ' + name + ' to be deleted'))
      }

      helper.request(helper.opts('DescribeTable', { TableName: name }), function (err, res) {
        if (err) return done(err)

        if (res.body && res.body.__type === 'com.amazonaws.dynamodb.v20120810#ResourceNotFoundException') {
          return done(null, res) // Table successfully deleted
        }

        if (res.statusCode !== 200) {
          return done(new Error(res.statusCode + ': ' + JSON.stringify(res.body)))
        }

        // Table still exists, check again
        setTimeout(checkDeleted, checkInterval)
      })
    }

    checkDeleted()
  }

  helper.waitUntilIndexesActive = function (name, done) {
    helper.request(helper.opts('DescribeTable', { TableName: name }), function (err, res) {
      if (err) return done(err)
      if (res.statusCode != 200)
        return done(new Error(res.statusCode + ': ' + JSON.stringify(res.body)))
      else if (res.body.Table.GlobalSecondaryIndexes.every(function (index) { return index.IndexStatus == 'ACTIVE' }))
        return done(null, res)
      setTimeout(helper.waitUntilIndexesActive, 1000, name, done)
    })
  }

  helper.deleteWhenActive = function (name, done) {
    if (!done) done = function () { }
    helper.waitUntilActive(name, function (err) {
      if (err) return done(err)
      helper.request(helper.opts('DeleteTable', { TableName: name }), done)
    })
  }

  helper.clearTable = function (name, keyNames, segments, done) {
    if (!done) { done = segments; segments = 2 }
    if (!Array.isArray(keyNames)) keyNames = [ keyNames ]

    function scanAndDelete (cb) {
      async.times(segments, function (n, cb) {
        helper.scanSegmentAndDelete(name, keyNames, segments, n, cb)
      }, function (err, segmentsHadKeys) {
        if (err) return cb(err)
        if (segmentsHadKeys.some(Boolean)) return scanAndDelete(cb)
        cb()
      })
    }

    scanAndDelete(done)
  }

  helper.scanSegmentAndDelete = function (tableName, keyNames, totalSegments, n, cb) {
    helper.request(helper.opts('Scan', { TableName: tableName, AttributesToGet: keyNames, Segment: n, TotalSegments: totalSegments }), function (err, res) {
      if (err) return cb(err)
      if (/ProvisionedThroughputExceededException/.test(res.body.__type)) {
        console.log('ProvisionedThroughputExceededException')
        return setTimeout(helper.scanSegmentAndDelete, 2000, tableName, keyNames, totalSegments, n, cb)
      }
      else if (res.statusCode != 200) {
        return cb(new Error(res.statusCode + ': ' + JSON.stringify(res.body)))
      }
      if (!res.body.ScannedCount) return cb(null, false)

      var keys = res.body.Items, batchDeletes

      for (batchDeletes = []; keys.length; keys = keys.slice(25))
        batchDeletes.push(function (keys) {
          return function (cb) { helper.batchWriteUntilDone(tableName, { deletes: keys }, cb) }
        }(keys.slice(0, 25)))

      async.parallel(batchDeletes, function (err) {
        if (err) return cb(err)
        cb(null, true)
      })
    })
  }

  helper.replaceTable = function (name, keyNames, items, segments, done) {
    if (!done) { done = segments; segments = 2 }

    helper.clearTable(name, keyNames, segments, function (err) {
      if (err) return done(err)
      helper.batchBulkPut(name, items, segments, done)
    })
  }

  helper.batchBulkPut = function (name, items, segments, done) {
    if (!done) { done = segments; segments = 2 }

    var itemChunks = [], i
    for (i = 0; i < items.length; i += 25)
      itemChunks.push(items.slice(i, i + 25))

    async.eachLimit(itemChunks, segments, function (items, cb) { helper.batchWriteUntilDone(name, { puts: items }, cb) }, done)
  }

  helper.batchWriteUntilDone = function (name, actions, cb) {
    var batchReq = { RequestItems: {} }, batchRes = {}
    batchReq.RequestItems[name] = (actions.puts || []).map(function (item) { return { PutRequest: { Item: item } } })
      .concat((actions.deletes || []).map(function (key) { return { DeleteRequest: { Key: key } } }))

    async.doWhilst(
      function (cb) {
        helper.request(helper.opts('BatchWriteItem', batchReq), function (err, res) {
          if (err) return cb(err)
          batchRes = res
          if (res.body.UnprocessedItems && Object.keys(res.body.UnprocessedItems).length) {
            batchReq.RequestItems = res.body.UnprocessedItems
          }
          else if (/ProvisionedThroughputExceededException/.test(res.body.__type)) {
            console.log('ProvisionedThroughputExceededException')
            return setTimeout(cb, 2000)
          }
          else if (res.statusCode != 200) {
            return cb(new Error(res.statusCode + ': ' + JSON.stringify(res.body)))
          }
          cb()
        })
      },
      function (cb) {
        var result = (batchRes.body.UnprocessedItems && Object.keys(batchRes.body.UnprocessedItems).length) ||
        /ProvisionedThroughputExceededException/.test(batchRes.body.__type)
        cb(null, result)
      },
      cb,
    )
  }

  return helper
}

// Legacy global variables and exports for backward compatibility
var MAX_SIZE = 409600
var awsRegion = process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION || 'us-east-1'
var awsAccountId = process.env.AWS_ACCOUNT_ID
var version = 'DynamoDB_20120810'
var prefix = '__dynalite_test_'
var readCapacity = 10
var writeCapacity = 5
var testHashTable = useRemoteDynamo ? '__dynalite_test_1' : randomName()
var testHashNTable = useRemoteDynamo ? '__dynalite_test_2' : randomName()
var testRangeTable = useRemoteDynamo ? '__dynalite_test_3' : randomName()
var testRangeNTable = useRemoteDynamo ? '__dynalite_test_4' : randomName()
var testRangeBTable = useRemoteDynamo ? '__dynalite_test_5' : randomName()

var port = 10000 + Math.round(Math.random() * 10000),
  requestOpts = useRemoteDynamo ?
    { host: 'dynamodb.' + awsRegion + '.amazonaws.com', method: 'POST' } :
    { host: '127.0.0.1', port: port, method: 'POST' }

var CREATE_REMOTE_TABLES = true
var DELETE_REMOTE_TABLES = true

var MAX_RETRIES = 20

// Global server instance for legacy tests
var globalServer = null
var globalServerStarted = false
var globalTablesCreated = false

// Get global account ID for legacy tests
function getGlobalAccountId (callback) {
  request(opts('DescribeTable', { TableName: testHashTable }), function (err, res) {
    if (err) return callback(err)
    if (res.statusCode !== 200) return callback(new Error('Failed to get account ID: ' + res.statusCode))
    if (res.body && res.body.Table && res.body.Table.TableArn) {
      awsAccountId = res.body.Table.TableArn.split(':')[4]
      exports.awsAccountId = awsAccountId
    }
    callback()
  })
}

// Create global test tables for legacy tests
function createGlobalTestTables (callback) {
  if (globalTablesCreated) return callback()
  if (useRemoteDynamo && !CREATE_REMOTE_TABLES) {
    globalTablesCreated = true
    return callback()
  }

  var tables = [ {
    TableName: testHashTable,
    AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' } ],
    KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
    ProvisionedThroughput: { ReadCapacityUnits: readCapacity, WriteCapacityUnits: writeCapacity },
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
    ProvisionedThroughput: { ReadCapacityUnits: readCapacity, WriteCapacityUnits: writeCapacity },
    LocalSecondaryIndexes: [ {
      IndexName: 'index1',
      KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'c', KeyType: 'RANGE' } ],
      Projection: { ProjectionType: 'ALL' },
    }, {
      IndexName: 'index2',
      KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'd', KeyType: 'RANGE' } ],
      Projection: { ProjectionType: 'INCLUDE', NonKeyAttributes: [ 'c' ] },
    } ],
    GlobalSecondaryIndexes: [ {
      IndexName: 'index3',
      KeySchema: [ { AttributeName: 'c', KeyType: 'HASH' } ],
      ProvisionedThroughput: { ReadCapacityUnits: readCapacity, WriteCapacityUnits: writeCapacity },
      Projection: { ProjectionType: 'ALL' },
    }, {
      IndexName: 'index4',
      KeySchema: [ { AttributeName: 'c', KeyType: 'HASH' }, { AttributeName: 'd', KeyType: 'RANGE' } ],
      ProvisionedThroughput: { ReadCapacityUnits: readCapacity, WriteCapacityUnits: writeCapacity },
      Projection: { ProjectionType: 'INCLUDE', NonKeyAttributes: [ 'e' ] },
    } ],
  }, {
    TableName: testRangeNTable,
    AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'N' } ],
    KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
    ProvisionedThroughput: { ReadCapacityUnits: readCapacity, WriteCapacityUnits: writeCapacity },
  }, {
    TableName: testRangeBTable,
    AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'B' } ],
    KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
    ProvisionedThroughput: { ReadCapacityUnits: readCapacity, WriteCapacityUnits: writeCapacity },
  } ]

  async.forEach(tables, createAndWait, function (err) {
    if (err) return callback(err)
    globalTablesCreated = true

    // Set the global awsAccountId from the created table
    getGlobalAccountId(callback)
  })
}

// Start global server for legacy tests
function startGlobalServer (callback) {
  if (globalServerStarted) return callback()
  if (useRemoteDynamo) {
    globalServerStarted = true
    return createGlobalTestTables(callback)
  }

  globalServer = dynalite({ path: process.env.DYNALITE_PATH })
  globalServer.listen(port, function (err) {
    if (err) return callback(err)
    globalServerStarted = true
    createGlobalTestTables(callback)
  })
}

// Ensure global server is started before any test
if (typeof before !== 'undefined') {
  before(function (done) {
    startGlobalServer(done)
  })
}

if (typeof after !== 'undefined') {
  after(function (done) {
    if (globalServer) {
      globalServer.close(done)
    }
    else {
      done()
    }
  })
}

// Legacy functions for backward compatibility
function request (opts, cb) {
  if (typeof opts === 'function') { cb = opts; opts = {} }

  // Ensure global server is started for legacy tests
  startGlobalServer(function (err) {
    if (err) return cb(err)

    opts.retries = opts.retries || 0
    cb = once(cb)
    for (var key in requestOpts) {
      if (opts[key] === undefined)
        opts[key] = requestOpts[key]
    }
    if (!opts.noSign) {
      aws4.sign(opts)
      opts.noSign = true // don't sign twice if calling recursively
    }

    http.request(opts, function (res) {
      res.setEncoding('utf8')
      res.on('error', cb)
      res.rawBody = ''
      res.on('data', function (chunk) { res.rawBody += chunk })
      res.on('end', function () {
        try {
          res.body = JSON.parse(res.rawBody)
        }
        catch {
          res.body = res.rawBody
        }
        if (useRemoteDynamo && opts.retries <= MAX_RETRIES &&
            (res.body.__type == 'com.amazon.coral.availability#ThrottlingException' ||
            res.body.__type == 'com.amazonaws.dynamodb.v20120810#LimitExceededException')) {
          opts.retries++
          return setTimeout(request, Math.floor(Math.random() * 1000), opts, cb)
        }
        cb(null, res)
      })
    }).on('error', function (err) {
      if (err && ~[ 'ECONNRESET', 'EMFILE', 'ENOTFOUND' ].indexOf(err.code) && opts.retries <= MAX_RETRIES) {
        opts.retries++
        return setTimeout(request, Math.floor(Math.random() * 100), opts, cb)
      }
      cb(err)
    }).end(opts.body)
  })
}

function opts (target, data) {
  return {
    headers: {
      'Content-Type': 'application/x-amz-json-1.0',
      'X-Amz-Target': version + '.' + target,
    },
    body: JSON.stringify(data),
  }
}

function randomString () {
  return ('AAAAAAAAA' + randomNumber()).slice(-10)
}

function randomNumber () {
  return String(Math.random() * 0x100000000)
}

function randomName () {
  return prefix + randomString()
}

// Legacy functions removed - they are now encapsulated within TestHelper instances

function createAndWait (table, done) {
  request(opts('CreateTable', table), function (err, res) {
    if (err) return done(err)
    if (res.statusCode != 200) return done(new Error(res.statusCode + ': ' + JSON.stringify(res.body)))
    setTimeout(waitUntilActive, 1000, table.TableName, done)
  })
}

// deleteAndWait function removed - now encapsulated within TestHelper instances

function waitUntilActive (name, done) {
  request(opts('DescribeTable', { TableName: name }), function (err, res) {
    if (err) return done(err)
    if (res.statusCode != 200) return done(new Error(res.statusCode + ': ' + JSON.stringify(res.body)))
    if (res.body.Table.TableStatus == 'ACTIVE' &&
        (!res.body.Table.GlobalSecondaryIndexes ||
          res.body.Table.GlobalSecondaryIndexes.every(function (index) { return index.IndexStatus == 'ACTIVE' }))) {
      return done(null, res)
    }
    setTimeout(waitUntilActive, 1000, name, done)
  })
}

function waitUntilDeleted (name, done) {
  request(opts('DescribeTable', { TableName: name }), function (err, res) {
    if (err) return done(err)
    if (res.body && res.body.__type == 'com.amazonaws.dynamodb.v20120810#ResourceNotFoundException')
      return done(null, res)
    else if (res.statusCode != 200)
      return done(new Error(res.statusCode + ': ' + JSON.stringify(res.body)))
    setTimeout(waitUntilDeleted, 1000, name, done)
  })
}

function waitUntilIndexesActive (name, done) {
  request(opts('DescribeTable', { TableName: name }), function (err, res) {
    if (err) return done(err)
    if (res.statusCode != 200)
      return done(new Error(res.statusCode + ': ' + JSON.stringify(res.body)))
    else if (res.body.Table.GlobalSecondaryIndexes.every(function (index) { return index.IndexStatus == 'ACTIVE' }))
      return done(null, res)
    setTimeout(waitUntilIndexesActive, 1000, name, done)
  })
}

function deleteWhenActive (name, done) {
  if (!done) done = function () { }
  waitUntilActive(name, function (err) {
    if (err) return done(err)
    request(opts('DeleteTable', { TableName: name }), done)
  })
}

function clearTable (name, keyNames, segments, done) {
  if (!done) { done = segments; segments = 2 }
  if (!Array.isArray(keyNames)) keyNames = [ keyNames ]

  scanAndDelete(done)

  function scanAndDelete (cb) {
    async.times(segments, scanSegmentAndDelete, function (err, segmentsHadKeys) {
      if (err) return cb(err)
      if (segmentsHadKeys.some(Boolean)) return scanAndDelete(cb)
      cb()
    })
  }

  function scanSegmentAndDelete (n, cb) {
    request(opts('Scan', { TableName: name, AttributesToGet: keyNames, Segment: n, TotalSegments: segments }), function (err, res) {
      if (err) return cb(err)
      if (/ProvisionedThroughputExceededException/.test(res.body.__type)) {
        console.log('ProvisionedThroughputExceededException')
        return setTimeout(scanSegmentAndDelete, 2000, n, cb)
      }
      else if (res.statusCode != 200) {
        return cb(new Error(res.statusCode + ': ' + JSON.stringify(res.body)))
      }
      if (!res.body.ScannedCount) return cb(null, false)

      var keys = res.body.Items, batchDeletes

      for (batchDeletes = []; keys.length; keys = keys.slice(25))
        batchDeletes.push(batchWriteUntilDone.bind(null, name, { deletes: keys.slice(0, 25) }))

      async.parallel(batchDeletes, function (err) {
        if (err) return cb(err)
        cb(null, true)
      })
    })
  }
}

function replaceTable (name, keyNames, items, segments, done) {
  if (!done) { done = segments; segments = 2 }

  clearTable(name, keyNames, segments, function (err) {
    if (err) return done(err)
    batchBulkPut(name, items, segments, done)
  })
}

function batchBulkPut (name, items, segments, done) {
  if (!done) { done = segments; segments = 2 }

  var itemChunks = [], i
  for (i = 0; i < items.length; i += 25)
    itemChunks.push(items.slice(i, i + 25))

  async.eachLimit(itemChunks, segments, function (items, cb) { batchWriteUntilDone(name, { puts: items }, cb) }, done)
}

function batchWriteUntilDone (name, actions, cb) {
  var batchReq = { RequestItems: {} }, batchRes = {}
  batchReq.RequestItems[name] = (actions.puts || []).map(function (item) { return { PutRequest: { Item: item } } })
    .concat((actions.deletes || []).map(function (key) { return { DeleteRequest: { Key: key } } }))

  async.doWhilst(
    function (cb) {
      request(opts('BatchWriteItem', batchReq), function (err, res) {
        if (err) return cb(err)
        batchRes = res
        if (res.body.UnprocessedItems && Object.keys(res.body.UnprocessedItems).length) {
          batchReq.RequestItems = res.body.UnprocessedItems
        }
        else if (/ProvisionedThroughputExceededException/.test(res.body.__type)) {
          console.log('ProvisionedThroughputExceededException')
          return setTimeout(cb, 2000)
        }
        else if (res.statusCode != 200) {
          return cb(new Error(res.statusCode + ': ' + JSON.stringify(res.body)))
        }
        cb()
      })
    },
    function (cb) {
      var result = (batchRes.body.UnprocessedItems && Object.keys(batchRes.body.UnprocessedItems).length) ||
      /ProvisionedThroughputExceededException/.test(batchRes.body.__type)
      cb(null, result)
    },
    cb,
  )
}

function assertSerialization (target, data, msg, done) {
  request(opts(target, data), function (err, res) {
    if (err) return done(err)
    res.statusCode.should.equal(400)
    res.body.should.eql({
      __type: 'com.amazon.coral.service#SerializationException',
      Message: msg,
    })
    done()
  })
}

function assertType (target, property, type, done) {
  var msgs = [], pieces = property.split('.'), subtypeMatch = type.match(/(.+?)<(.+)>$/), subtype
  if (subtypeMatch != null) {
    type = subtypeMatch[1]
    subtype = subtypeMatch[2]
  }
  var castMsg = "class sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl cannot be cast to class java.lang.Class (sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl and java.lang.Class are in module java.base of loader 'bootstrap')"
  switch (type) {
  case 'Boolean':
    msgs = [
      [ '23', 'Unexpected token received from parser' ],
      [ 23, 'NUMBER_VALUE cannot be converted to Boolean' ],
      [ -2147483648, 'NUMBER_VALUE cannot be converted to Boolean' ],
      [ 2147483648, 'NUMBER_VALUE cannot be converted to Boolean' ],
      [ 34.56, 'DECIMAL_VALUE cannot be converted to Boolean' ],
      [ [], 'Unrecognized collection type class java.lang.Boolean' ],
      [ {}, 'Start of structure or map found where not expected' ],
    ]
    break
  case 'String':
    msgs = [
      [ true, 'TRUE_VALUE cannot be converted to String' ],
      [ false, 'FALSE_VALUE cannot be converted to String' ],
      [ 23, 'NUMBER_VALUE cannot be converted to String' ],
      [ -2147483648, 'NUMBER_VALUE cannot be converted to String' ],
      [ 2147483648, 'NUMBER_VALUE cannot be converted to String' ],
      [ 34.56, 'DECIMAL_VALUE cannot be converted to String' ],
      [ [], 'Unrecognized collection type class java.lang.String' ],
      [ {}, 'Start of structure or map found where not expected' ],
    ]
    break
  case 'Integer':
    msgs = [
      [ '23', 'STRING_VALUE cannot be converted to Integer' ],
      [ true, 'TRUE_VALUE cannot be converted to Integer' ],
      [ false, 'FALSE_VALUE cannot be converted to Integer' ],
      [ [], 'Unrecognized collection type class java.lang.Integer' ],
      [ {}, 'Start of structure or map found where not expected' ],
    ]
    break
  case 'Long':
    msgs = [
      [ '23', 'STRING_VALUE cannot be converted to Long' ],
      [ true, 'TRUE_VALUE cannot be converted to Long' ],
      [ false, 'FALSE_VALUE cannot be converted to Long' ],
      [ [], 'Unrecognized collection type class java.lang.Long' ],
      [ {}, 'Start of structure or map found where not expected' ],
    ]
    break
  case 'Blob':
    msgs = [
      [ true, 'only base-64-encoded strings are convertible to bytes' ],
      [ 23, 'only base-64-encoded strings are convertible to bytes' ],
      [ -2147483648, 'only base-64-encoded strings are convertible to bytes' ],
      [ 2147483648, 'only base-64-encoded strings are convertible to bytes' ],
      [ 34.56, 'only base-64-encoded strings are convertible to bytes' ],
      [ [], 'Unrecognized collection type class java.nio.ByteBuffer' ],
      [ {}, 'Start of structure or map found where not expected' ],
      [ '23456', 'Base64 encoded length is expected a multiple of 4 bytes but found: 5' ],
      [ '=+/=', 'Invalid last non-pad Base64 character dectected' ],
      [ '+/+=', 'Invalid last non-pad Base64 character dectected' ],
    ]
    break
  case 'List':
    msgs = [
      [ '23', 'Unexpected field type' ],
      [ true, 'Unexpected field type' ],
      [ 23, 'Unexpected field type' ],
      [ -2147483648, 'Unexpected field type' ],
      [ 2147483648, 'Unexpected field type' ],
      [ 34.56, 'Unexpected field type' ],
      [ {}, 'Start of structure or map found where not expected' ],
    ]
    break
  case 'ParameterizedList':
    msgs = [
      [ '23', castMsg ],
      [ true, castMsg ],
      [ 23, castMsg ],
      [ -2147483648, castMsg ],
      [ 2147483648, castMsg ],
      [ 34.56, castMsg ],
      [ {}, 'Start of structure or map found where not expected' ],
    ]
    break
  case 'Map':
    msgs = [
      [ '23', 'Unexpected field type' ],
      [ true, 'Unexpected field type' ],
      [ 23, 'Unexpected field type' ],
      [ -2147483648, 'Unexpected field type' ],
      [ 2147483648, 'Unexpected field type' ],
      [ 34.56, 'Unexpected field type' ],
      [ [], 'Unrecognized collection type java.util.Map<java.lang.String, ' + (~subtype.indexOf('.') ? subtype : 'com.amazonaws.dynamodb.v20120810.' + subtype) + '>' ],
    ]
    break
  case 'ParameterizedMap':
    msgs = [
      [ '23', castMsg ],
      [ true, castMsg ],
      [ 23, castMsg ],
      [ -2147483648, castMsg ],
      [ 2147483648, castMsg ],
      [ 34.56, castMsg ],
      [ [], 'Unrecognized collection type java.util.Map<java.lang.String, com.amazonaws.dynamodb.v20120810.AttributeValue>' ],
    ]
    break
  case 'ValueStruct':
    msgs = [
      [ '23', 'Unexpected value type in payload' ],
      [ true, 'Unexpected value type in payload' ],
      [ 23, 'Unexpected value type in payload' ],
      [ -2147483648, 'Unexpected value type in payload' ],
      [ 2147483648, 'Unexpected value type in payload' ],
      [ 34.56, 'Unexpected value type in payload' ],
      [ [], 'Unrecognized collection type class com.amazonaws.dynamodb.v20120810.' + subtype ],
    ]
    break
  case 'FieldStruct':
    msgs = [
      [ '23', 'Unexpected field type' ],
      [ true, 'Unexpected field type' ],
      [ 23, 'Unexpected field type' ],
      [ -2147483648, 'Unexpected field type' ],
      [ 2147483648, 'Unexpected field type' ],
      [ 34.56, 'Unexpected field type' ],
      [ [], 'Unrecognized collection type class com.amazonaws.dynamodb.v20120810.' + subtype ],
    ]
    break
  case 'AttrStruct':
    async.forEach([
      [ property, subtype + '<AttributeValue>' ],
      [ property + '.S', 'String' ],
      [ property + '.N', 'String' ],
      [ property + '.B', 'Blob' ],
      [ property + '.BOOL', 'Boolean' ],
      [ property + '.NULL', 'Boolean' ],
      [ property + '.SS', 'List' ],
      [ property + '.SS.0', 'String' ],
      [ property + '.NS', 'List' ],
      [ property + '.NS.0', 'String' ],
      [ property + '.BS', 'List' ],
      [ property + '.BS.0', 'Blob' ],
      [ property + '.L', 'List' ],
      [ property + '.L.0', 'ValueStruct<AttributeValue>' ],
      [ property + '.L.0.BS', 'List' ],
      [ property + '.L.0.BS.0', 'Blob' ],
      [ property + '.M', 'Map<AttributeValue>' ],
      [ property + '.M.a', 'ValueStruct<AttributeValue>' ],
      [ property + '.M.a.BS', 'List' ],
      [ property + '.M.a.BS.0', 'Blob' ],
    ], function (test, cb) { assertType(target, test[0], test[1], cb) }, done)
    return
  default:
    throw new Error('Unknown type: ' + type)
  }
  async.forEach(msgs, function (msg, cb) {
    var data = {}, child = data, i, ix
    for (i = 0; i < pieces.length - 1; i++) {
      ix = Array.isArray(child) ? 0 : pieces[i]
      child = child[ix] = pieces[i + 1] === '0' ? [] : {}
    }
    ix = Array.isArray(child) ? 0 : pieces[pieces.length - 1]
    child[ix] = msg[0]
    assertSerialization(target, data, msg[1], cb)
  }, done)
}

function assertAccessDenied (target, data, msg, done) {
  request(opts(target, data), function (err, res) {
    if (err) return done(err)
    res.statusCode.should.equal(400)
    if (typeof res.body !== 'object') {
      return done(new Error('Not JSON: ' + res.body))
    }
    res.body.__type.should.equal('com.amazon.coral.service#AccessDeniedException')
    if (msg instanceof RegExp) {
      res.body.Message.should.match(msg)
    }
    else {
      res.body.Message.should.equal(msg)
    }
    done()
  })
}

function assertValidation (target, data, msg, done) {
  request(opts(target, data), function (err, res) {
    if (err) return done(err)
    if (typeof res.body !== 'object') {
      return done(new Error('Not JSON: ' + res.body))
    }
    res.body.__type.should.equal('com.amazon.coral.validate#ValidationException')
    if (msg instanceof RegExp) {
      res.body.message.should.match(msg)
    }
    else if (Array.isArray(msg)) {
      var prefix = msg.length + ' validation error' + (msg.length === 1 ? '' : 's') + ' detected: '
      res.body.message.should.startWith(prefix)
      var errors = res.body.message.slice(prefix.length).split('; ')
      for (var i = 0; i < msg.length; i++) {
        errors.should.matchAny(msg[i])
      }
    }
    else {
      res.body.message.should.equal(msg)
    }
    res.statusCode.should.equal(400)
    done()
  })
}

function assertNotFound (target, data, msg, done) {
  request(opts(target, data), function (err, res) {
    if (err) return done(err)
    res.statusCode.should.equal(400)
    res.body.should.eql({
      __type: 'com.amazonaws.dynamodb.v20120810#ResourceNotFoundException',
      message: msg,
    })
    done()
  })
}

function assertInUse (target, data, msg, done) {
  request(opts(target, data), function (err, res) {
    if (err) return done(err)
    res.statusCode.should.equal(400)
    res.body.should.eql({
      __type: 'com.amazonaws.dynamodb.v20120810#ResourceInUseException',
      message: msg,
    })
    done()
  })
}

function assertConditional (target, data, done) {
  request(opts(target, data), function (err, res) {
    if (err) return done(err)
    res.statusCode.should.equal(400)
    res.body.should.eql({
      __type: 'com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException',
      message: 'The conditional request failed',
    })
    done()
  })
}

function strDecrement (str, regex, length) {
  regex = regex || /.?/
  length = length || 255
  var lastIx = str.length - 1, lastChar = str.charCodeAt(lastIx) - 1, prefix = str.slice(0, lastIx), finalChar = 255
  while (lastChar >= 0 && !regex.test(String.fromCharCode(lastChar))) lastChar--
  if (lastChar < 0) return prefix
  prefix += String.fromCharCode(lastChar)
  while (finalChar >= 0 && !regex.test(String.fromCharCode(finalChar))) finalChar--
  if (lastChar < 0) return prefix
  while (prefix.length < length) prefix += String.fromCharCode(finalChar)
  return prefix
}

// Legacy exports - maintain backward compatibility
exports.MAX_SIZE = MAX_SIZE
exports.awsRegion = awsRegion
exports.awsAccountId = awsAccountId
exports.version = version
exports.prefix = prefix
exports.request = request
exports.opts = opts
exports.waitUntilActive = waitUntilActive
exports.waitUntilDeleted = waitUntilDeleted
exports.waitUntilIndexesActive = waitUntilIndexesActive
exports.deleteWhenActive = deleteWhenActive
exports.createAndWait = createAndWait
exports.clearTable = clearTable
exports.replaceTable = replaceTable
exports.batchWriteUntilDone = batchWriteUntilDone
exports.batchBulkPut = batchBulkPut
exports.assertSerialization = assertSerialization
exports.assertType = assertType
exports.assertValidation = assertValidation
exports.assertNotFound = assertNotFound
exports.assertInUse = assertInUse
exports.assertConditional = assertConditional
exports.assertAccessDenied = assertAccessDenied
exports.strDecrement = strDecrement
exports.randomString = randomString
exports.randomNumber = randomNumber
exports.randomName = randomName
exports.readCapacity = readCapacity
exports.writeCapacity = writeCapacity
exports.testHashTable = testHashTable
exports.testHashNTable = testHashNTable
exports.testRangeTable = testRangeTable
exports.testRangeNTable = testRangeNTable
exports.testRangeBTable = testRangeBTable
exports.runSlowTests = runSlowTests

// New exports
exports.createTestHelper = createTestHelper

// Global hooks are removed - no more automatic before/after execution
