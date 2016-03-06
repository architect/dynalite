var http = require('http'),
    aws4 = require('aws4'),
    async = require('async'),
    once = require('once'),
    dynalite = require('..')

http.globalAgent.maxSockets = Infinity

exports.awsRegion = process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION || 'us-east-1'
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
exports.assertNotFound = assertNotFound
exports.assertInUse = assertInUse
exports.assertConditional = assertConditional
exports.strDecrement = strDecrement
exports.randomString = randomString
exports.randomNumber = randomNumber
exports.randomName = randomName
exports.testHashTable = randomName()
exports.testHashNTable = randomName()
exports.testRangeTable = randomName()
exports.testRangeNTable = randomName()
exports.testRangeBTable = randomName()
// For testing:
// exports.testHashTable = '__dynalite_test_1'
// exports.testHashNTable = '__dynalite_test_2'
// exports.testRangeTable = '__dynalite_test_3'
// exports.testRangeNTable = '__dynalite_test_4'
// exports.testRangeBTable = '__dynalite_test_5'

var port = 10000 + Math.round(Math.random() * 10000),
    requestOpts = process.env.REMOTE ?
      {host: 'dynamodb.us-east-1.amazonaws.com', method: 'POST'} :
      {host: '127.0.0.1', port: port, method: 'POST'}

var dynaliteServer = dynalite({path: process.env.DYNALITE_PATH})

before(function(done) {
  this.timeout(200000)
  dynaliteServer.listen(port, function(err) {
    if (err) return done(err)
    createTestTables(done)
    // done()
  })
})

after(function(done) {
  this.timeout(200000)
  deleteTestTables(function(err) {
    if (err) return done(err)
    dynaliteServer.close(done)
  })
})

function request(opts, cb) {
  if (typeof opts === 'function') { cb = opts; opts = {} }
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
    res.body = ''
    res.on('data', function(chunk) { res.body += chunk })
    res.on('end', function() {
      try { res.body = JSON.parse(res.body) } catch (e) {} // eslint-disable-line no-empty
      if (res.body.__type == 'com.amazon.coral.availability#ThrottlingException' ||
          res.body.__type == 'com.amazonaws.dynamodb.v20120810#LimitExceededException')
        return setTimeout(request, Math.floor(Math.random() * 1000), opts, cb)
      cb(null, res)
    })
  }).on('error', function(err) {
    if (err && ~['ECONNRESET', 'EMFILE'].indexOf(err.code))
      return setTimeout(request, Math.floor(Math.random() * 100), opts, cb)
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
  var tables = [{
    TableName: exports.testHashTable,
    AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}],
    KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}],
    ProvisionedThroughput: {ReadCapacityUnits: 2, WriteCapacityUnits: 2},
  }, {
    TableName: exports.testHashNTable,
    AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'N'}],
    KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}],
    ProvisionedThroughput: {ReadCapacityUnits: 2, WriteCapacityUnits: 2},
  }, {
    TableName: exports.testRangeTable,
    AttributeDefinitions: [
      {AttributeName: 'a', AttributeType: 'S'},
      {AttributeName: 'b', AttributeType: 'S'},
      {AttributeName: 'c', AttributeType: 'S'},
      {AttributeName: 'd', AttributeType: 'S'},
    ],
    KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'RANGE', AttributeName: 'b'}],
    ProvisionedThroughput: {ReadCapacityUnits: 2, WriteCapacityUnits: 2},
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
      ProvisionedThroughput: {ReadCapacityUnits: 2, WriteCapacityUnits: 2},
      Projection: {ProjectionType: 'ALL'},
    }, {
      IndexName: 'index4',
      KeySchema: [{AttributeName: 'c', KeyType: 'HASH'}, {AttributeName: 'd', KeyType: 'RANGE'}],
      ProvisionedThroughput: {ReadCapacityUnits: 2, WriteCapacityUnits: 2},
      Projection: {ProjectionType: 'INCLUDE', NonKeyAttributes: ['e']},
    }],
  }, {
    TableName: exports.testRangeNTable,
    AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'N'}],
    KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'RANGE', AttributeName: 'b'}],
    ProvisionedThroughput: {ReadCapacityUnits: 2, WriteCapacityUnits: 2},
  }, {
    TableName: exports.testRangeBTable,
    AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'B'}],
    KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'RANGE', AttributeName: 'b'}],
    ProvisionedThroughput: {ReadCapacityUnits: 2, WriteCapacityUnits: 2},
  }]
  async.forEach(tables, createAndWait, done)
}

function deleteTestTables(done) {
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
    if (res.body.Table.TableStatus == 'ACTIVE')
      return done(null, res)
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
      if (res.statusCode != 200) return cb(new Error(res.statusCode + ': ' + JSON.stringify(res.body)))
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
          console.log('ProvisionedThroughputExceededException')
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
  var msgs = [], pieces = property.split('.')
  switch (type) {
    case 'Boolean':
      msgs = [
        ['23', '\'23\' can not be converted to an Boolean'],
        [23, 'class com.amazon.coral.value.json.numbers.TruncatingBigNumber can not be converted to an Boolean'],
        [-2147483648, 'class com.amazon.coral.value.json.numbers.TruncatingBigNumber can not be converted to an Boolean'],
        [2147483648, 'class com.amazon.coral.value.json.numbers.TruncatingBigNumber can not be converted to an Boolean'],
        [34.56, 'class com.amazon.coral.value.json.numbers.TruncatingBigNumber can not be converted to an Boolean'],
        [[], 'Start of list found where not expected'],
        [{}, 'Start of structure or map found where not expected.'],
      ]
      break
    case 'String':
      msgs = [
        [true, 'class java.lang.Boolean can not be converted to an String'],
        [23, 'class com.amazon.coral.value.json.numbers.TruncatingBigNumber can not be converted to an String'],
        [-2147483648, 'class com.amazon.coral.value.json.numbers.TruncatingBigNumber can not be converted to an String'],
        [2147483648, 'class com.amazon.coral.value.json.numbers.TruncatingBigNumber can not be converted to an String'],
        [34.56, 'class com.amazon.coral.value.json.numbers.TruncatingBigNumber can not be converted to an String'],
        [[], 'Start of list found where not expected'],
        [{}, 'Start of structure or map found where not expected.'],
      ]
      break
    case 'Integer':
      msgs = [
        ['23', 'class java.lang.String can not be converted to an Integer'],
        [true, 'class java.lang.Boolean can not be converted to an Integer'],
        [[], 'Start of list found where not expected'],
        [{}, 'Start of structure or map found where not expected.'],
      ]
      break
    case 'Long':
      msgs = [
        ['23', 'class java.lang.String can not be converted to an Long'],
        [true, 'class java.lang.Boolean can not be converted to an Long'],
        [[], 'Start of list found where not expected'],
        [{}, 'Start of structure or map found where not expected.'],
      ]
      break
    case 'Blob':
      msgs = [
        [true, 'class java.lang.Boolean can not be converted to a Blob'],
        [23, 'class com.amazon.coral.value.json.numbers.TruncatingBigNumber can not be converted to a Blob'],
        [-2147483648, 'class com.amazon.coral.value.json.numbers.TruncatingBigNumber can not be converted to a Blob'],
        [2147483648, 'class com.amazon.coral.value.json.numbers.TruncatingBigNumber can not be converted to a Blob'],
        [34.56, 'class com.amazon.coral.value.json.numbers.TruncatingBigNumber can not be converted to a Blob'],
        [[], 'Start of list found where not expected'],
        [{}, 'Start of structure or map found where not expected.'],
        ['23456', '\'23456\' can not be converted to a Blob: Base64 encoded length is expected a multiple of 4 bytes but found: 5'],
        ['=+/=', '\'=+/=\' can not be converted to a Blob: Invalid Base64 character: \'=\''],
        ['+/+=', '\'+/+=\' can not be converted to a Blob: Invalid last non-pad Base64 character dectected'],
      ]
      break
    case 'List':
      msgs = [
        ['23', 'Expected list or null'],
        [true, 'Expected list or null'],
        [23, 'Expected list or null'],
        [-2147483648, 'Expected list or null'],
        [2147483648, 'Expected list or null'],
        [34.56, 'Expected list or null'],
        [{}, 'Start of structure or map found where not expected.'],
      ]
      break
    case 'Map':
      msgs = [
        ['23', 'Expected map or null'],
        [true, 'Expected map or null'],
        [23, 'Expected map or null'],
        [-2147483648, 'Expected map or null'],
        [2147483648, 'Expected map or null'],
        [34.56, 'Expected map or null'],
        [[], 'Start of list found where not expected'],
      ]
      break
    case 'Structure':
      msgs = [
        ['23', 'Expected null'],
        [true, 'Expected null'],
        [23, 'Expected null'],
        [-2147483648, 'Expected null'],
        [2147483648, 'Expected null'],
        [34.56, 'Expected null'],
        [[], 'Start of list found where not expected'],
      ]
      break
    case 'AttrStructure':
      async.forEach([
        [property, 'Structure'],
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
        [property + '.L.0', 'Structure'],
        [property + '.L.0.BS', 'List'],
        [property + '.L.0.BS.0', 'Blob'],
        [property + '.M', 'Map'],
        [property + '.M.a', 'Structure'],
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

function assertValidation(target, data, msg, done) {
  request(opts(target, data), function(err, res) {
    if (err) return done(err)
    res.statusCode.should.equal(400)
    if (msg instanceof RegExp) {
      res.body.__type.should.equal('com.amazon.coral.validate#ValidationException')
      res.body.message.should.match(msg)
    } else {
      res.body.should.eql({
        __type: 'com.amazon.coral.validate#ValidationException',
        message: msg,
      })
    }
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

