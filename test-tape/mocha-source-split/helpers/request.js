var http = require('http');
var aws4 = require('aws4');
var once = require('once');
var config = require('./config');

var MAX_RETRIES = 20;

var port = 10000 + Math.round(Math.random() * 10000);
var requestOpts = config.useRemoteDynamo ?
  { host: 'dynamodb.' + config.awsRegion + '.amazonaws.com', method: 'POST' } :
  { host: '127.0.0.1', port: port, method: 'POST' };

http.globalAgent.maxSockets = Infinity;

// Store port for setup module to access
exports.port = port;

function request (opts, cb) {
  if (typeof opts === 'function') { cb = opts; opts = {} }
  opts.retries = opts.retries || 0;
  cb = once(cb);
  for (var key in requestOpts) {
    if (opts[key] === undefined)
      opts[key] = requestOpts[key];
  }
  if (!opts.noSign) {
    aws4.sign(opts);
    opts.noSign = true; // don't sign twice if calling recursively
  }
  // console.log(opts)
  http.request(opts, function (res) {
    res.setEncoding('utf8');
    res.on('error', cb);
    res.rawBody = '';
    res.on('data', function (chunk) { res.rawBody += chunk });
    res.on('end', function () {
      try {
        res.body = JSON.parse(res.rawBody);
      }
      catch (e) {
        res.body = res.rawBody;
      }
      if (config.useRemoteDynamo && opts.retries <= MAX_RETRIES &&
          (res.body.__type == 'com.amazon.coral.availability#ThrottlingException' ||
          res.body.__type == 'com.amazonaws.dynamodb.v20120810#LimitExceededException')) {
        opts.retries++;
        return setTimeout(request, Math.floor(Math.random() * 1000), opts, cb);
      }
      cb(null, res);
    });
  }).on('error', function (err) {
    if (err && ~[ 'ECONNRESET', 'EMFILE', 'ENOTFOUND' ].indexOf(err.code) && opts.retries <= MAX_RETRIES) {
      opts.retries++;
      return setTimeout(request, Math.floor(Math.random() * 100), opts, cb);
    }
    cb(err);
  }).end(opts.body);
}

function opts (target, data) {
  return {
    headers: {
      'Content-Type': 'application/x-amz-json-1.0',
      'X-Amz-Target': config.version + '.' + target,
    },
    body: JSON.stringify(data),
  };
}

exports.request = request;
exports.opts = opts; 