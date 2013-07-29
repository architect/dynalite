var http = require('http'),
    aws4 = require('aws4'),
    async = require('async'),
    once = require('once'),
    dynalite = require('..')

var requestOpts = process.env.REMOTE ?  {host: 'dynamodb.ap-southeast-2.amazonaws.com', method: 'POST'} :
  {host: 'localhost', port: 4567, method: 'POST'}

exports.version = 'DynamoDB_20120810'
exports.prefix = '__dynalite_test_'
exports.request = request
exports.opts = opts
exports.waitUntilActive = waitUntilActive
exports.waitUntilDeleted = waitUntilDeleted
exports.assertSerialization = assertSerialization
exports.assertType = assertType
exports.assertValidation = assertValidation
exports.assertNotFound = assertNotFound
exports.assertInUse = assertInUse
exports.assertConditional = assertConditional
exports.strDecrement = strDecrement
exports.randomString = randomString
exports.randomName = randomName
exports.testHashTable = randomName()
exports.testRangeTable = randomName()

before(function(done) {
  this.timeout(200000)
  dynalite.listen(4567, function(err) {
    if (err) return done(err)
    createTestTables(done)
  })
})

after(function(done) {
  this.timeout(200000)
  deleteTestTables(function(err) {
    if (err) return done(err)
    dynalite.close(done)
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
  //console.log(opts)
  http.request(opts, function(res) {
    res.setEncoding('utf8')
    res.on('error', cb)
    res.body = ''
    res.on('data', function(chunk) { res.body += chunk })
    res.on('end', function() {
      try { res.body = JSON.parse(res.body) } catch (e) {}
      if (res.body.__type == 'com.amazon.coral.availability#ThrottlingException' ||
          res.body.__type == 'com.amazonaws.dynamodb.v20120810#LimitExceededException')
        return setTimeout(request, 1000, opts, cb)
      cb(null, res)
    })
  }).on('error', cb).end(opts.body)
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
    ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1},
  }, {
    TableName: exports.testRangeTable,
    AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}, {AttributeName: 'b', AttributeType: 'S'}],
    KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}, {KeyType: 'RANGE', AttributeName: 'b'}],
    ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1},
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
    if (res.body.__type)
      return done(new Error(res.body.__type + ': ' + res.body.message))
    setTimeout(waitUntilActive, 1000, table.TableName, done)
  })
}

function deleteAndWait(name, done) {
  request(opts('DeleteTable', {TableName: name}), function(err, res) {
    if (err) return done(err)
    if (res.body.__type == 'com.amazonaws.dynamodb.v20120810#ResourceInUseException')
      return setTimeout(deleteAndWait, 1000, name, done)
    else if (res.body.__type)
      return done(new Error(res.body.__type + ': ' + res.body.message))
    setTimeout(waitUntilDeleted, 1000, name, done)
  })
}

function waitUntilActive(name, done) {
  request(opts('DescribeTable', {TableName: name}), function(err, res) {
    if (err) return done(err)
    if (res.body.__type)
      return done(new Error(res.body.__type + ': ' + res.body.message))
    else if (res.body.Table.TableStatus != 'CREATING')
      return done(null, res)
    setTimeout(waitUntilActive, 1000, name, done)
  })
}

function waitUntilDeleted(name, done) {
  request(opts('DescribeTable', {TableName: name}), function(err, res) {
    if (err) return done(err)
    if (res.body.__type == 'com.amazonaws.dynamodb.v20120810#ResourceNotFoundException')
      return done(null, res)
    else if (res.body.__type)
      return done(new Error(res.body.__type + ': ' + res.body.message))
    setTimeout(waitUntilDeleted, 1000, name, done)
  })
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
  switch(type) {
    case 'Boolean':
      msgs = [
        [23, 'class java.lang.Short can not be converted to an Boolean'],
        [-2147483648, 'class java.lang.Integer can not be converted to an Boolean'],
        [2147483648, 'class java.lang.Long can not be converted to an Boolean'],
        // For some reason, doubles are fine
        //[34.56, 'class java.lang.Double can not be converted to an Boolean'],
        [[], 'Start of list found where not expected'],
        [{}, 'Start of structure or map found where not expected.'],
      ]
      break
    case 'String':
      msgs = [
        [true, 'class java.lang.Boolean can not be converted to an String'],
        [23, 'class java.lang.Short can not be converted to an String'],
        [-2147483648, 'class java.lang.Integer can not be converted to an String'],
        [2147483648, 'class java.lang.Long can not be converted to an String'],
        [34.56, 'class java.lang.Double can not be converted to an String'],
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
        [23, 'class java.lang.Short can not be converted to a Blob'],
        [-2147483648, 'class java.lang.Integer can not be converted to a Blob'],
        [2147483648, 'class java.lang.Long can not be converted to a Blob'],
        [34.56, 'class java.lang.Double can not be converted to a Blob'],
        [[], 'Start of list found where not expected'],
        [{}, 'Start of structure or map found where not expected.'],
        ['23456', '\'23456\' can not be converted to a Blob: Base64 encoded length is expected a multiple of 4 bytes but found: 5'],
        ['=+/=', '\'=+/=\' can not be converted to a Blob: Invalid Base64 character: \'=\''],
        ['+/+=', '\'+/+=\' can not be converted to a Blob: Invalid last non-pad Base64 character dectected'],
      ]
      break
    case 'List':
      msgs = [
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
        [true, 'Expected null'],
        [23, 'Expected null'],
        [-2147483648, 'Expected null'],
        [2147483648, 'Expected null'],
        [34.56, 'Expected null'],
        [[], 'Start of list found where not expected'],
      ]
      break
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
    res.body.should.eql({
      __type: 'com.amazon.coral.validate#ValidationException',
      message: msg,
    })
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

