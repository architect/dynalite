var https = require('https'),
  once = require('once'),
  dynalite = require('..'),
  helpers = require('./helpers')

var request = helpers.request

describe('dynalite connections', function () {

  describe('basic', function () {

    function assert404 (done) {
      return function (err, res) {
        // Sometimes DynamoDB returns weird/bad HTTP responses
        if (err && err.code == 'HPE_INVALID_CONSTANT') return done()
        if (err) return done(err)
        res.statusCode.should.equal(404)
        try {
          res.body.should.equal('<UnknownOperationException/>\n')
          res.headers['x-amz-crc32'].should.equal('3552371480')
          res.headers['content-length'].should.equal('29')
        }
        catch {
          // Sometimes it's an HTML page instead of the above
          res.body.should.equal(
            '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" ' +
            '"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">\n' +
            '<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">\n' +
            '<head>\n  ' +
            '<title>Page Not Found</title>\n' +
            '</head>\n' +
            '<body>Page Not Found</body>\n' +
            '</html>',
          )
          res.headers['x-amz-crc32'].should.equal('2548615100')
          res.headers['content-length'].should.equal('272')
        }
        res.headers['x-amzn-requestid'].should.match(/^[0-9A-Z]{52}$/)
        done()
      }
    }

    it('should return 413 if request too large', function (done) {
      this.timeout(200000)
      var body = Array((16 * 1024 * 1024) + 1), i
      for (i = 0; i < body.length; i++)
        body[i] = 'a'

      request({ body: body.join(''), noSign: true }, function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(413)
        res.headers['transfer-encoding'].should.equal('chunked')
        done()
      })
    })

    it('should not return 413 if request not too large', function (done) {
      this.timeout(200000)
      var body = Array(16 * 1024 * 1024), i
      for (i = 0; i < body.length; i++)
        body[i] = 'a'

      request({ body: body.join(''), noSign: true }, function (err, res) {
        if (err && err.code == 'HPE_INVALID_CONSTANT') return done()
        if (err) return done(err)
        res.statusCode.should.equal(404)
        done()
      })
    })

    it('should return 404 if OPTIONS with no auth', function (done) {
      request({ method: 'OPTIONS', noSign: true }, assert404(done))
    })

    it('should return 200 if a GET', function (done) {
      request({ method: 'GET', noSign: true }, function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.equal('healthy: dynamodb.' + helpers.awsRegion + '.amazonaws.com ')
        res.headers['x-amz-crc32'].should.match(/^[0-9]+$/)
        res.headers['content-length'].should.equal(res.body.length.toString())
        res.headers['x-amzn-requestid'].should.match(/^[0-9A-Z]{52}$/)
        done()
      })
    })

    it('should return 404 if a PUT', function (done) {
      request({ method: 'PUT', noSign: true }, assert404(done))
    })

    it('should return 404 if a DELETE', function (done) {
      request({ method: 'DELETE', noSign: true }, assert404(done))
    })

    it('should return 404 if body but no content-type', function (done) {
      request({ body: 'hi', noSign: true }, assert404(done))
    })

    it('should return 404 if body but incorrect content-type', function (done) {
      request({ body: 'hi', headers: { 'content-type': 'whatever' }, noSign: true }, assert404(done))
    })

    it('should return 404 if body and application/x-amz-json-1.1', function (done) {
      request({ body: 'hi', headers: { 'content-type': 'application/x-amz-json-1.1' }, noSign: true }, assert404(done))
    })

    it('should return 404 if body but slightly different content-type', function (done) {
      request({ body: 'hi', headers: { 'content-type': 'application/jsonasdf' }, noSign: true }, assert404(done))
    })

    it('should connect to SSL', function (done) {
      var port = 10000 + Math.round(Math.random() * 10000), dynaliteServer = dynalite({ ssl: true })

      dynaliteServer.listen(port, function (err) {
        if (err) return done(err)

        done = once(done)

        https.request({ host: '127.0.0.1', port: port, rejectUnauthorized: false }, function (res) {
          res.on('error', done)
          res.on('data', function () {})
          res.on('end', function () {
            res.statusCode.should.equal(200)
            dynaliteServer.close(done)
          })
        }).on('error', done).end()
      })
    })

  })

  describe('JSON', function () {

    function assertBody (body, crc32, contentType, done) {
      if (typeof contentType == 'function') { done = contentType; contentType = 'application/json' }
      return function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(400)
        res.body.should.eql(body)
        res.headers['x-amzn-requestid'].should.match(/^[0-9A-Z]{52}$/)
        res.headers['x-amz-crc32'].should.equal(String(crc32))
        res.headers['content-type'].should.equal(contentType)
        res.headers['content-length'].should.equal(String(Buffer.byteLength(JSON.stringify(res.body), 'utf8')))
        done()
      }
    }

    function assertSerialization (contentType, done) {
      return assertBody({ __type: 'com.amazon.coral.service#SerializationException' }, 3948637019,
        contentType, done)
    }

    function assertUnknownOp (contentType, done) {
      return assertBody({ __type: 'com.amazon.coral.service#UnknownOperationException' }, 1368724161,
        contentType, done)
    }

    function assertMissing (done) {
      return assertBody({
        __type: 'com.amazon.coral.service#MissingAuthenticationTokenException',
        message: 'Request is missing Authentication Token',
      }, 2088342776, done)
    }

    function assertInvalid (done) {
      return assertBody({
        __type: 'com.amazon.coral.service#InvalidSignatureException',
        message: 'Found both \'X-Amz-Algorithm\' as a query-string param and \'Authorization\' as HTTP header.',
      }, 2139606068, done)
    }

    function assertIncomplete (msg, crc32, done) {
      return assertBody({
        __type: 'com.amazon.coral.service#IncompleteSignatureException',
        message: msg,
      }, crc32, done)
    }

    function assertCors (headers, done) {
      return function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.headers['x-amzn-requestid'].should.match(/^[0-9A-Z]{52}$/)
        res.headers['access-control-allow-origin'].should.equal('*')
        Object.keys(headers || {}).forEach(function (header) {
          res.headers[header].should.equal(headers[header])
        })
        res.headers['access-control-max-age'].should.equal('172800')
        res.headers['content-length'].should.equal('0')
        res.body.should.eql('')
        done()
      }
    }

    it('should return SerializationException if body is application/json but not JSON', function (done) {
      request({ body: 'hi', headers: { 'content-type': 'application/json' }, noSign: true },
        assertSerialization(done))
    })

    it('should return SerializationException if body is application/x-amz-json-1.0 but not JSON', function (done) {
      request({ body: 'hi', headers: { 'content-type': 'application/x-amz-json-1.0' }, noSign: true },
        assertSerialization('application/x-amz-json-1.0', done))
    })

    it('should return SerializationException if body is application/json and semicolon but not JSON', function (done) {
      request({ body: 'hi', headers: { 'content-type': 'application/json;' }, noSign: true },
        assertSerialization(done))
    })

    it('should return SerializationException if body is application/json and spaces and semicolon but not JSON', function (done) {
      request({ body: 'hi', headers: { 'content-type': '  application/json  ;   asfd' }, noSign: true },
        assertSerialization(done))
    })

    it('should return SerializationException if body is application/json and nonsense but not JSON', function (done) {
      request({ body: 'hi', headers: { 'content-type': 'application/json;blahblah' }, noSign: true },
        assertSerialization(done))
    })

    it('should return SerializationException if body is application/x-amz-json-1.0 and nonsense but not JSON', function (done) {
      request({ body: 'hi', headers: { 'content-type': 'application/x-amz-json-1.0;blahblah' }, noSign: true },
        assertSerialization('application/x-amz-json-1.0', done))
    })

    it('should return UnknownOperationException if no target', function (done) {
      request({ noSign: true }, assertUnknownOp(done))
    })

    it('should return UnknownOperationException and set CORS if using Origin', function (done) {
      request({ headers: { origin: 'whatever' } }, function (err, res) {
        if (err) return done(err)
        res.headers['access-control-allow-origin'].should.equal('*')
        assertUnknownOp(done)(err, res)
      })
    })

    it('should return UnknownOperationException if body is application/json', function (done) {
      request({ body: '{}', headers: { 'content-type': 'application/json' }, noSign: true },
        assertUnknownOp(done))
    })

    it('should return UnknownOperationException if body is application/x-amz-json-1.0', function (done) {
      request({ body: '{}', headers: { 'content-type': 'application/x-amz-json-1.0' }, noSign: true },
        assertUnknownOp('application/x-amz-json-1.0', done))
    })

    it('should return UnknownOperationException if body is application/json;charset=asfdsaf', function (done) {
      request({ body: '{}', headers: { 'content-type': 'application/json;charset=asfdsaf' }, noSign: true },
        assertUnknownOp(done))
    })

    it('should return UnknownOperationException if incorrect target', function (done) {
      request({ headers: { 'x-amz-target': 'whatever' }, noSign: true }, assertUnknownOp(done))
    })

    it('should return UnknownOperationException if incorrect target operation', function (done) {
      request({ headers: { 'x-amz-target': 'DynamoDB_20120810.ListTable' }, noSign: true }, assertUnknownOp(done))
    })

    it('should return MissingAuthenticationTokenException if no Authorization header', function (done) {
      request({ headers: { 'x-amz-target': 'DynamoDB_20120810.ListTables' }, noSign: true }, assertMissing(done))
    })

    it('should return MissingAuthenticationTokenException if incomplete Authorization header', function (done) {
      request({ headers: { 'x-amz-target': 'DynamoDB_20120810.ListTables', 'Authorization': 'AWS4' }, noSign: true },
        assertMissing(done))
    })

    it('should return MissingAuthenticationTokenException if incomplete Authorization header and X-Amz-Algorithm query', function (done) {
      request({
        path: '/?X-Amz-Algorith',
        headers: { 'x-amz-target': 'DynamoDB_20120810.ListTables', 'Authorization': 'X' },
        noSign: true,
      }, assertMissing(done))
    })

    it('should return MissingAuthenticationTokenException if all query params except X-Amz-Algorithm', function (done) {
      request({
        path: '/?X-Amz-Credential=a&X-Amz-Signature=b&X-Amz-SignedHeaders=c&X-Amz-Date=d',
        headers: { 'x-amz-target': 'DynamoDB_20120810.ListTables' },
        noSign: true,
      }, assertMissing(done))
    })

    it('should return InvalidSignatureException if both Authorization header and X-Amz-Algorithm query', function (done) {
      request({
        path: '/?X-Amz-Algorithm',
        headers: { 'x-amz-target': 'DynamoDB_20120810.ListTables', 'Authorization': 'X' },
        noSign: true,
      }, assertInvalid(done))
    })

    it('should return IncompleteSignatureException if Authorization header is "AWS4-"', function (done) {
      request({
        headers: { 'x-amz-target': 'DynamoDB_20120810.ListTables', 'Authorization': 'AWS4-' },
        noSign: true,
      }, assertIncomplete('Authorization header requires \'Credential\' parameter. ' +
        'Authorization header requires \'Signature\' parameter. ' +
        'Authorization header requires \'SignedHeaders\' parameter. ' +
        'Authorization header requires existence of either a \'X-Amz-Date\' or ' +
        'a \'Date\' header. Authorization=AWS4-', 1828866742, done))
    })

    it('should return IncompleteSignatureException if Authorization header is "AWS4- Signature=b Credential=a"', function (done) {
      request({
        headers: {
          'x-amz-target': 'DynamoDB_20120810.ListTables',
          'Authorization': 'AWS4- Signature=b Credential=a',
          'Date': 'a',
        },
        noSign: true,
      }, assertIncomplete('Authorization header requires \'SignedHeaders\' parameter. ' +
        'Authorization=AWS4- Signature=b Credential=a', 15336762, done))
    })

    it('should return IncompleteSignatureException if Authorization header is "AWS4- Signature=b,Credential=a"', function (done) {
      request({
        headers: {
          'x-amz-target': 'DynamoDB_20120810.ListTables',
          'Authorization': 'AWS4- Signature=b,Credential=a',
          'Date': 'a',
        },
        noSign: true,
      }, assertIncomplete('Authorization header requires \'SignedHeaders\' parameter. ' +
        'Authorization=AWS4- Signature=b,Credential=a', 1159703774, done))
    })

    it('should return IncompleteSignatureException if Authorization header is "AWS4- Signature=b, Credential=a"', function (done) {
      request({
        headers: {
          'x-amz-target': 'DynamoDB_20120810.ListTables',
          'Authorization': 'AWS4- Signature=b, Credential=a',
          'Date': 'a',
        },
        noSign: true,
      }, assertIncomplete('Authorization header requires \'SignedHeaders\' parameter. ' +
        'Authorization=AWS4- Signature=b, Credential=a', 164353342, done))
    })

    it('should return IncompleteSignatureException if empty X-Amz-Algorithm query', function (done) {
      request({
        path: '/?X-Amz-Algorithm',
        headers: { 'x-amz-target': 'DynamoDB_20120810.ListTables' },
        noSign: true,
      }, assertIncomplete('AWS query-string parameters must include \'X-Amz-Algorithm\'. ' +
        'AWS query-string parameters must include \'X-Amz-Credential\'. ' +
        'AWS query-string parameters must include \'X-Amz-Signature\'. ' +
        'AWS query-string parameters must include \'X-Amz-SignedHeaders\'. ' +
        'AWS query-string parameters must include \'X-Amz-Date\'. ' +
        'Re-examine the query-string parameters.', 2900502663, done))
    })

    it('should return IncompleteSignatureException if missing X-Amz-SignedHeaders query', function (done) {
      request({
        path: '/?X-Amz-Algorithm=a&X-Amz-Credential=b&X-Amz-Signature=c&X-Amz-Date=d',
        headers: { 'x-amz-target': 'DynamoDB_20120810.ListTables' },
        noSign: true,
      }, assertIncomplete('AWS query-string parameters must include \'X-Amz-SignedHeaders\'. ' +
        'Re-examine the query-string parameters.', 3712057481, done))
    })

    it('should set CORS if OPTIONS and Origin', function (done) {
      request({ method: 'OPTIONS', headers: { origin: 'whatever' } }, assertCors(null, done))
    })

    it('should set CORS if OPTIONS and Origin and Headers', function (done) {
      request({ method: 'OPTIONS', headers: {
        origin: 'whatever',
        'access-control-request-headers': 'a, b, c',
      } }, assertCors({
        'access-control-allow-headers': 'a, b, c',
      }, done))
    })

    it('should set CORS if OPTIONS and Origin and Headers and Method', function (done) {
      request({ method: 'OPTIONS', headers: {
        origin: 'whatever',
        'access-control-request-headers': 'a, b, c',
        'access-control-request-method': 'd',
      } }, assertCors({
        'access-control-allow-headers': 'a, b, c',
        'access-control-allow-methods': 'd',
      }, done))
    })
  })

})
