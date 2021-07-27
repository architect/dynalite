var http = require('http'),
    aws4 = require('aws4'),
    async = require('async'),
    once = require('once'),
    dynalite = require('..')

http.globalAgent.maxSockets = Infinity

exports.MAX_SIZE = 409600
exports.awsRegion = process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION || 'us-east-1'
exports.awsAccountId = process.env.AWS_ACCOUNT_ID // will be set programatically below
exports.version = 'DynamoDB_20120810'
exports.prefix = '__dynalite_test_'
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
exports.assertTransactionCanceled = assertTransactionCanceled
exports.assertNotFound = assertNotFound
exports.assertInUse = assertInUse
exports.assertConditional = assertConditional
exports.assertAccessDenied = assertAccessDenied
exports.strDecrement = strDecrement
exports.randomString = randomString
exports.randomNumber = randomNumber
exports.randomName = randomName
exports.readCapacity = 10
exports.writeCapacity = 5
exports.testHashTable = process.env.REMOTE ? '__dynalite_test_1' : randomName()
exports.testHashNTable = process.env.REMOTE ? '__dynalite_test_2' : randomName()
exports.testRangeTable = process.env.REMOTE ? '__dynalite_test_3' : randomName()
exports.testRangeNTable = process.env.REMOTE ? '__dynalite_test_4' : randomName()
exports.testRangeBTable = process.env.REMOTE ? '__dynalite_test_5' : randomName()

var port = 10000 + Math.round(Math.random() * 10000),
    requestOpts = process.env.REMOTE ?
      {host: 'dynamodb.' + exports.awsRegion + '.amazonaws.com', method: 'POST'} :
      {host: '127.0.0.1', port: port, method: 'POST'}

var dynaliteServer = dynalite({path: process.env.DYNALITE_PATH})

var CREATE_REMOTE_TABLES = true
var DELETE_REMOTE_TABLES = true

before(function(done) {
  this.timeout(200000)
  dynaliteServer.listen(port, function(err) {
    if (err) return done(err)
    createTestTables(function(err) {
      if (err) return done(err)
      getAccountId(done)
    })
  })
})

after(function(done) {
  this.timeout(500000)
  deleteTestTables(function(err) {
    if (err) return done(err)
    dynaliteServer.close(done)
  })
})

var MAX_RETRIES = 20

function request(opts, cb) {
  if (typeof opts === 'function') { cb = opts; opts = {} }
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
  // console.log(opts)
  http.request(opts, function(res) {
    res.setEncoding('utf8')
    res.on('error', cb)
    res.rawBody = ''
    res.on('data', function(chunk) { res.rawBody += chunk })
    res.on('end', function() {
      try {
        res.body = JSON.parse(res.rawBody)
      } catch (e) {
        res.body = res.rawBody
      }
      if (process.env.REMOTE && opts.retries <= MAX_RETRIES &&
          (res.body.__type == 'com.amazon.coral.availability#ThrottlingException' ||
          res.body.__type == 'com.amazonaws.dynamodb.v20120810#LimitExceededException')) {
        opts.retries++
        return setTimeout(request, Math.floor(Math.random() * 1000), opts, cb)
      }
      cb(null, res)
    })
  }).on('error', function(err) {
    if (err && ~['ECONNRESET', 'EMFILE', 'ENOTFOUND'].indexOf(err.code) && opts.retries <= MAX_RETRIES) {
      opts.retries++
      return setTimeout(request, Math.floor(Math.random() * 100), opts, cb)
    }
    cb(err)
  }).end(opts.body)
}

function opts(target, data) {
  return {
    headers: {
      'Content-Type': 'application/x-amz-json-1.0',
      'X-Amz-Target': exports.version + '.' + target,
    },
    body: JSON.stringify(data),
  }
}

function randomString() {
  return ('AAAAAAAAA' + randomNumber()).slice(-10)
}

function randomNumber() {
  return String(Math.random() * 0x100000000)
}

function randomName() {
  return exports.prefix + randomString()
}

function createTestTables(done) {
  if (process.env.REMOTE && !CREATE_REMOTE_TABLES) return done()
  var readCapacity = exports.readCapacity, writeCapacity = exports.writeCapacity
  var tables = [{
    TableName: exports.testHashTable,
    AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}],
    KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}],
    ProvisionedThroughput: {ReadCapacityUnits: readCapacity, WriteCapacityUnits: writeCapacity},
  }, {
    TableName: exports.testHashNTable,
    AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'N'}],
    KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}],
    BillingMode: 'PAY_PER_REQUEST',
  }, {
    TableName: exports.testRangeTable,
    AttributeDefinitions: [
      {AttributeName: 'a', AttributeType: 'S'},
      {AttributeName: 'b', AttributeType: 'S'},
      {AttributeName: 'c', AttributeType: 'S'},
      {AttributeName: 'd', AttributeType: 'S'},
    ],
    KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'RANGE', AttributeName: 'b'}],
    ProvisionedThroughput: {ReadCapacityUnits: readCapacity, WriteCapacityUnits: writeCapacity},
    LocalSecondaryIndexes: [{
      IndexName: 'index1',
      KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}, {AttributeName: 'c', KeyType: 'RANGE'}],
      Projection: {ProjectionType: 'ALL'},
    }, {
      IndexName: 'index2',
      KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}, {AttributeName: 'd', KeyType: 'RANGE'}],
      Projection: {ProjectionType: 'INCLUDE', NonKeyAttributes: ['c']},
    }],
    GlobalSecondaryIndexes: [{
      IndexName: 'index3',
      KeySchema: [{AttributeName: 'c', KeyType: 'HASH'}],
      ProvisionedThroughput: {ReadCapacityUnits: readCapacity, WriteCapacityUnits: writeCapacity},
      Projection: {ProjectionType: 'ALL'},
    }, {
      IndexName: 'index4',
      KeySchema: [{AttributeName: 'c', KeyType: 'HASH'}, {AttributeName: 'd', KeyType: 'RANGE'}],
      ProvisionedThroughput: {ReadCapacityUnits: readCapacity, WriteCapacityUnits: writeCapacity},
      Projection: {ProjectionType: 'INCLUDE', NonKeyAttributes: ['e']},
    }],
  }, {
    TableName: exports.testRangeNTable,
    AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'N'}],
    KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'RANGE', AttributeName: 'b'}],
    ProvisionedThroughput: {ReadCapacityUnits: readCapacity, WriteCapacityUnits: writeCapacity},
  }, {
    TableName: exports.testRangeBTable,
    AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'B'}],
    KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'RANGE', AttributeName: 'b'}],
    ProvisionedThroughput: {ReadCapacityUnits: readCapacity, WriteCapacityUnits: writeCapacity},
  }]
  async.forEach(tables, createAndWait, done)
}

function getAccountId(done) {
  request(opts('DescribeTable', {TableName: exports.testHashTable}), function(err, res) {
    if (err) return done(err)
    exports.awsAccountId = res.body.Table.TableArn.split(':')[4]
    done()
  })
}

function deleteTestTables(done) {
  if (process.env.REMOTE && !DELETE_REMOTE_TABLES) return done()
  request(opts('ListTables', {}), function(err, res) {
    if (err) return done(err)
    var names = res.body.TableNames.filter(function(name) { return name.indexOf(exports.prefix) === 0 })
    async.forEach(names, deleteAndWait, done)
  })
}

function createAndWait(table, done) {
  request(opts('CreateTable', table), function(err, res) {
    if (err) return done(err)
    if (res.statusCode != 200) return done(new Error(res.statusCode + ': ' + JSON.stringify(res.body)))
    setTimeout(waitUntilActive, 1000, table.TableName, done)
  })
}

function deleteAndWait(name, done) {
  request(opts('DeleteTable', {TableName: name}), function(err, res) {
    if (err) return done(err)
    if (res.body && res.body.__type == 'com.amazonaws.dynamodb.v20120810#ResourceInUseException')
      return setTimeout(deleteAndWait, 1000, name, done)
    else if (res.statusCode != 200)
      return done(new Error(res.statusCode + ': ' + JSON.stringify(res.body)))
    setTimeout(waitUntilDeleted, 1000, name, done)
  })
}

function waitUntilActive(name, done) {
  request(opts('DescribeTable', {TableName: name}), function(err, res) {
    if (err) return done(err)
    if (res.statusCode != 200) return done(new Error(res.statusCode + ': ' + JSON.stringify(res.body)))
    if (res.body.Table.TableStatus == 'ACTIVE' &&
        (!res.body.Table.GlobalSecondaryIndexes ||
          res.body.Table.GlobalSecondaryIndexes.every(function(index) { return index.IndexStatus == 'ACTIVE' }))) {
      return done(null, res)
    }
    setTimeout(waitUntilActive, 1000, name, done)
  })
}

function waitUntilDeleted(name, done) {
  request(opts('DescribeTable', {TableName: name}), function(err, res) {
    if (err) return done(err)
    if (res.body && res.body.__type == 'com.amazonaws.dynamodb.v20120810#ResourceNotFoundException')
      return done(null, res)
    else if (res.statusCode != 200)
      return done(new Error(res.statusCode + ': ' + JSON.stringify(res.body)))
    setTimeout(waitUntilDeleted, 1000, name, done)
  })
}

function waitUntilIndexesActive(name, done) {
  request(opts('DescribeTable', {TableName: name}), function(err, res) {
    if (err) return done(err)
    if (res.statusCode != 200)
      return done(new Error(res.statusCode + ': ' + JSON.stringify(res.body)))
    else if (res.body.Table.GlobalSecondaryIndexes.every(function(index) { return index.IndexStatus == 'ACTIVE' }))
      return done(null, res)
    setTimeout(waitUntilIndexesActive, 1000, name, done)
  })
}

function deleteWhenActive(name, done) {
  if (!done) done = function() { }
  waitUntilActive(name, function(err) {
    if (err) return done(err)
    request(opts('DeleteTable', {TableName: name}), done)
  })
}

function clearTable(name, keyNames, segments, done) {
  if (!done) { done = segments; segments = 2 }
  if (!Array.isArray(keyNames)) keyNames = [keyNames]

  scanAndDelete(done)

  function scanAndDelete(cb) {
    async.times(segments, scanSegmentAndDelete, function(err, segmentsHadKeys) {
      if (err) return cb(err)
      if (segmentsHadKeys.some(Boolean)) return scanAndDelete(cb)
      cb()
    })
  }

  function scanSegmentAndDelete(n, cb) {
    request(opts('Scan', {TableName: name, AttributesToGet: keyNames, Segment: n, TotalSegments: segments}), function(err, res) {
      if (err) return cb(err)
      if (/ProvisionedThroughputExceededException/.test(res.body.__type)) {
        console.log('ProvisionedThroughputExceededException') // eslint-disable-line no-console
        return setTimeout(scanSegmentAndDelete, 2000, n, cb)
      } else if (res.statusCode != 200) {
        return cb(new Error(res.statusCode + ': ' + JSON.stringify(res.body)))
      }
      if (!res.body.ScannedCount) return cb(null, false)

      var keys = res.body.Items, batchDeletes

      for (batchDeletes = []; keys.length; keys = keys.slice(25))
        batchDeletes.push(batchWriteUntilDone.bind(null, name, {deletes: keys.slice(0, 25)}))

      async.parallel(batchDeletes, function(err) {
        if (err) return cb(err)
        cb(null, true)
      })
    })
  }
}

function replaceTable(name, keyNames, items, segments, done) {
  if (!done) { done = segments; segments = 2 }

  clearTable(name, keyNames, segments, function(err) {
    if (err) return done(err)
    batchBulkPut(name, items, segments, done)
  })
}

function batchBulkPut(name, items, segments, done) {
  if (!done) { done = segments; segments = 2 }

  var itemChunks = [], i
  for (i = 0; i < items.length; i += 25)
    itemChunks.push(items.slice(i, i + 25))

  async.eachLimit(itemChunks, segments, function(items, cb) { batchWriteUntilDone(name, {puts: items}, cb) }, done)
}

function batchWriteUntilDone(name, actions, cb) {
  var batchReq = {RequestItems: {}}, batchRes = {}
  batchReq.RequestItems[name] = (actions.puts || []).map(function(item) { return {PutRequest: {Item: item}} })
    .concat((actions.deletes || []).map(function(key) { return {DeleteRequest: {Key: key}} }))

  async.doWhilst(
    function(cb) {
      request(opts('BatchWriteItem', batchReq), function(err, res) {
        if (err) return cb(err)
        batchRes = res
        if (res.body.UnprocessedItems && Object.keys(res.body.UnprocessedItems).length) {
          batchReq.RequestItems = res.body.UnprocessedItems
        } else if (/ProvisionedThroughputExceededException/.test(res.body.__type)) {
          console.log('ProvisionedThroughputExceededException') // eslint-disable-line no-console
          return setTimeout(cb, 2000)
        } else if (res.statusCode != 200) {
          return cb(new Error(res.statusCode + ': ' + JSON.stringify(res.body)))
        }
        cb()
      })
    },
    function() {
      return (batchRes.body.UnprocessedItems && Object.keys(batchRes.body.UnprocessedItems).length) ||
        /ProvisionedThroughputExceededException/.test(batchRes.body.__type)
    },
    cb
  )
}

function assertSerialization(target, data, msg, done) {
  request(opts(target, data), function(err, res) {
    if (err) return done(err)
    res.statusCode.should.equal(400)
    res.body.should.eql({
      __type: 'com.amazon.coral.service#SerializationException',
      Message: msg,
    })
    done()
  })
}

function assertType(target, property, type, done) {
  var msgs = [], pieces = property.split('.'), subtypeMatch = type.match(/(.+?)<(.+)>$/), subtype
  if (subtypeMatch != null) {
    type = subtypeMatch[1]
    subtype = subtypeMatch[2]
  }
  var castMsg = "class sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl cannot be cast to class java.lang.Class (sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl and java.lang.Class are in module java.base of loader 'bootstrap')"
  switch (type) {
    case 'Boolean':
      msgs = [
        ['23', 'Unexpected token received from parser'],
        [23, 'NUMBER_VALUE cannot be converted to Boolean'],
        [-2147483648, 'NUMBER_VALUE cannot be converted to Boolean'],
        [2147483648, 'NUMBER_VALUE cannot be converted to Boolean'],
        [34.56, 'DECIMAL_VALUE cannot be converted to Boolean'],
        [[], 'Unrecognized collection type class java.lang.Boolean'],
        [{}, 'Start of structure or map found where not expected'],
      ]
      break
    case 'String':
      msgs = [
        [true, 'TRUE_VALUE cannot be converted to String'],
        [false, 'FALSE_VALUE cannot be converted to String'],
        [23, 'NUMBER_VALUE cannot be converted to String'],
        [-2147483648, 'NUMBER_VALUE cannot be converted to String'],
        [2147483648, 'NUMBER_VALUE cannot be converted to String'],
        [34.56, 'DECIMAL_VALUE cannot be converted to String'],
        [[], 'Unrecognized collection type class java.lang.String'],
        [{}, 'Start of structure or map found where not expected'],
      ]
      break
    case 'Integer':
      msgs = [
        ['23', 'STRING_VALUE cannot be converted to Integer'],
        [true, 'TRUE_VALUE cannot be converted to Integer'],
        [false, 'FALSE_VALUE cannot be converted to Integer'],
        [[], 'Unrecognized collection type class java.lang.Integer'],
        [{}, 'Start of structure or map found where not expected'],
      ]
      break
    case 'Long':
      msgs = [
        ['23', 'STRING_VALUE cannot be converted to Long'],
        [true, 'TRUE_VALUE cannot be converted to Long'],
        [false, 'FALSE_VALUE cannot be converted to Long'],
        [[], 'Unrecognized collection type class java.lang.Long'],
        [{}, 'Start of structure or map found where not expected'],
      ]
      break
    case 'Blob':
      msgs = [
        [true, 'only base-64-encoded strings are convertible to bytes'],
        [23, 'only base-64-encoded strings are convertible to bytes'],
        [-2147483648, 'only base-64-encoded strings are convertible to bytes'],
        [2147483648, 'only base-64-encoded strings are convertible to bytes'],
        [34.56, 'only base-64-encoded strings are convertible to bytes'],
        [[], 'Unrecognized collection type class java.nio.ByteBuffer'],
        [{}, 'Start of structure or map found where not expected'],
        ['23456', 'Base64 encoded length is expected a multiple of 4 bytes but found: 5'],
        ['=+/=', 'Invalid last non-pad Base64 character dectected'],
        ['+/+=', 'Invalid last non-pad Base64 character dectected'],
      ]
      break
    case 'List':
      msgs = [
        ['23', 'Unexpected field type'],
        [true, 'Unexpected field type'],
        [23, 'Unexpected field type'],
        [-2147483648, 'Unexpected field type'],
        [2147483648, 'Unexpected field type'],
        [34.56, 'Unexpected field type'],
        [{}, 'Start of structure or map found where not expected'],
      ]
      break
    case 'ParameterizedList':
      msgs = [
        ['23', castMsg],
        [true, castMsg],
        [23, castMsg],
        [-2147483648, castMsg],
        [2147483648, castMsg],
        [34.56, castMsg],
        [{}, 'Start of structure or map found where not expected'],
      ]
      break
    case 'Map':
      msgs = [
        ['23', 'Unexpected field type'],
        [true, 'Unexpected field type'],
        [23, 'Unexpected field type'],
        [-2147483648, 'Unexpected field type'],
        [2147483648, 'Unexpected field type'],
        [34.56, 'Unexpected field type'],
        [[], 'Unrecognized collection type java.util.Map<java.lang.String, ' + (~subtype.indexOf('.') ? subtype : 'com.amazonaws.dynamodb.v20120810.' + subtype) + '>'],
      ]
      break
    case 'ParameterizedMap':
      msgs = [
        ['23', castMsg],
        [true, castMsg],
        [23, castMsg],
        [-2147483648, castMsg],
        [2147483648, castMsg],
        [34.56, castMsg],
        [[], 'Unrecognized collection type java.util.Map<java.lang.String, com.amazonaws.dynamodb.v20120810.AttributeValue>'],
      ]
      break
    case 'ValueStruct':
      msgs = [
        ['23', 'Unexpected value type in payload'],
        [true, 'Unexpected value type in payload'],
        [23, 'Unexpected value type in payload'],
        [-2147483648, 'Unexpected value type in payload'],
        [2147483648, 'Unexpected value type in payload'],
        [34.56, 'Unexpected value type in payload'],
        [[], 'Unrecognized collection type class com.amazonaws.dynamodb.v20120810.' + subtype],
      ]
      break
    case 'FieldStruct':
      msgs = [
        ['23', 'Unexpected field type'],
        [true, 'Unexpected field type'],
        [23, 'Unexpected field type'],
        [-2147483648, 'Unexpected field type'],
        [2147483648, 'Unexpected field type'],
        [34.56, 'Unexpected field type'],
        [[], 'Unrecognized collection type class com.amazonaws.dynamodb.v20120810.' + subtype],
      ]
      break
    case 'AttrStruct':
      async.forEach([
        [property, subtype + '<AttributeValue>'],
        [property + '.S', 'String'],
        [property + '.N', 'String'],
        [property + '.B', 'Blob'],
        [property + '.BOOL', 'Boolean'],
        [property + '.NULL', 'Boolean'],
        [property + '.SS', 'List'],
        [property + '.SS.0', 'String'],
        [property + '.NS', 'List'],
        [property + '.NS.0', 'String'],
        [property + '.BS', 'List'],
        [property + '.BS.0', 'Blob'],
        [property + '.L', 'List'],
        [property + '.L.0', 'ValueStruct<AttributeValue>'],
        [property + '.L.0.BS', 'List'],
        [property + '.L.0.BS.0', 'Blob'],
        [property + '.M', 'Map<AttributeValue>'],
        [property + '.M.a', 'ValueStruct<AttributeValue>'],
        [property + '.M.a.BS', 'List'],
        [property + '.M.a.BS.0', 'Blob'],
      ], function(test, cb) { assertType(target, test[0], test[1], cb) }, done)
      return
    default:
      throw new Error('Unknown type: ' + type)
  }
  async.forEach(msgs, function(msg, cb) {
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

function assertAccessDenied(target, data, msg, done) {
  request(opts(target, data), function(err, res) {
    if (err) return done(err)
    res.statusCode.should.equal(400)
    if (typeof res.body !== 'object') {
      return done(new Error('Not JSON: ' + res.body))
    }
    res.body.__type.should.equal('com.amazon.coral.service#AccessDeniedException')
    if (msg instanceof RegExp) {
      res.body.Message.should.match(msg)
    } else {
      res.body.Message.should.equal(msg)
    }
    done()
  })
}

function assertValidation(target, data, msg, done) {
  request(opts(target, data), function(err, res) {
    if (err) return done(err)
    if (typeof res.body !== 'object') {
      return done(new Error('Not JSON: ' + res.body))
    }
    res.body.__type.should.equal('com.amazon.coral.validate#ValidationException')
    if (msg instanceof RegExp) {
      res.body.message.should.match(msg)
    } else if (Array.isArray(msg)) {
      var prefix = msg.length + ' validation error' + (msg.length === 1 ? '' : 's') + ' detected: '
      res.body.message.should.startWith(prefix)
      var errors = res.body.message.slice(prefix.length).split('; ')
      for (var i = 0; i < msg.length; i++) {
        errors.should.matchAny(msg[i])
      }
    } else {
      res.body.message.should.equal(msg)
    }
    res.statusCode.should.equal(400)
    done()
  })
}

function assertTransactionCanceled(target, data, msg, done) {
  request(opts(target, data), function(err, res) {
    if (err) return done(err)
    if (typeof res.body !== 'object') {
      return done(new Error('Not JSON: ' + res.body))
    }
    res.body.__type.should.equal('com.amazonaws.dynamodb.v20120810#TransactionCanceledException')
    if (msg instanceof RegExp) {
      res.body.message.should.match(msg)
    } else {
      res.body.message.should.equal(msg)
    }
    res.statusCode.should.equal(400)
    done()
  })
}

function assertNotFound(target, data, msg, done) {
  request(opts(target, data), function(err, res) {
    if (err) return done(err)
    res.statusCode.should.equal(400)
    res.body.should.eql({
      __type: 'com.amazonaws.dynamodb.v20120810#ResourceNotFoundException',
      message: msg,
    })
    done()
  })
}

function assertInUse(target, data, msg, done) {
  request(opts(target, data), function(err, res) {
    if (err) return done(err)
    res.statusCode.should.equal(400)
    res.body.should.eql({
      __type: 'com.amazonaws.dynamodb.v20120810#ResourceInUseException',
      message: msg,
    })
    done()
  })
}

function assertConditional(target, data, done) {
  request(opts(target, data), function(err, res) {
    if (err) return done(err)
    res.statusCode.should.equal(400)
    res.body.should.eql({
      __type: 'com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException',
      message: 'The conditional request failed',
    })
    done()
  })
}

function strDecrement(str, regex, length) {
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
