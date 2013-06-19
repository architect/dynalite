var http = require('http'),
    aws4 = require('aws4'),
    async = require('async')

var requestOpts = process.env.REMOTE ?  {host: 'dynamodb.ap-southeast-2.amazonaws.com', method: 'POST'} :
  {host: 'localhost', port: 4567, method: 'POST'}

exports.request = request
exports.opts = opts
exports.assertSerialization = assertSerialization
exports.assertType = assertType
exports.assertValidation = assertValidation
exports.assertNotFound = assertNotFound

function request(opts, cb) {
  if (typeof opts === 'function') { cb = opts; opts = {} }
  for (var key in requestOpts) {
    if (opts[key] === undefined)
      opts[key] = requestOpts[key]
  }
  if (!opts.noSign) aws4.sign(opts)
  //console.log(opts)
  http.request(opts, function(res) {
    res.setEncoding('utf8')
    res.on('error', cb)
    res.body = ''
    res.on('data', function(chunk) { res.body += chunk })
    res.on('end', function() {
      try { res.body = JSON.parse(res.body) } catch (e) {}
      cb(null, res)
    })
  }).on('error', cb).end(opts.body)
}

function opts(target, data) {
  return {
    headers: {
      'Content-Type': 'application/x-amz-json-1.0',
      'X-Amz-Target': target,
    },
    body: JSON.stringify(data),
  }
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


