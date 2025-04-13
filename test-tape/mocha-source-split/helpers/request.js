const http = require('http');
const aws4 = require('aws4');
const once = require('once');
const config = require('./config');

const MAX_RETRIES = 20;
let baseRequestOpts = {}; // Will be initialized by setup.js

function initRequest(opts) {
    baseRequestOpts = opts;
}

function request(callOpts, cb) {
    if (typeof callOpts === 'function') { cb = callOpts; callOpts = {}; }
    callOpts.retries = callOpts.retries || 0;
    cb = once(cb);

    // Merge base options (host, port) with call-specific options
    const finalOpts = { ...baseRequestOpts, ...callOpts };

    // Ensure headers exist
    finalOpts.headers = finalOpts.headers || {};

    if (!finalOpts.noSign) {
        // Clean up potential conflicting headers if we are signing
        // aws4.sign modifies the opts object directly
        delete finalOpts.headers['host'];
        delete finalOpts.headers['content-length'];
        delete finalOpts.headers['x-amz-date'];
        delete finalOpts.headers['authorization'];

        aws4.sign(finalOpts, {
            accessKeyId: process.env.AWS_ACCESS_KEY_ID,
            secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
            sessionToken: process.env.AWS_SESSION_TOKEN, // Include session token if present
        });
        finalOpts.noSign = true; // Don't sign twice if calling recursively
    }

    // console.log(finalOpts);
    const req = http.request(finalOpts, (res) => {
        res.setEncoding('utf8');
        res.on('error', cb);
        res.rawBody = '';
        res.on('data', (chunk) => { res.rawBody += chunk; });
        res.on('end', () => {
            try {
                res.body = JSON.parse(res.rawBody);
            }
            catch (e) {
                res.body = res.rawBody;
            }
            // Retry logic for throttling/limits when using remote DynamoDB
            if (config.useRemoteDynamo && finalOpts.retries <= MAX_RETRIES &&
                (res.body.__type === 'com.amazon.coral.availability#ThrottlingException' ||
                 res.body.__type === 'com.amazonaws.dynamodb.v20120810#LimitExceededException')) {
                finalOpts.retries++;
                // Use the original callOpts for retry, but keep the incremented retries count
                const retryOpts = { ...callOpts, retries: finalOpts.retries };
                return setTimeout(request, Math.floor(Math.random() * 1000), retryOpts, cb);
            }
            cb(null, res);
        });
    });

    req.on('error', (err) => {
        // Retry logic for common network errors
        if (err && ~['ECONNRESET', 'EMFILE', 'ENOTFOUND'].indexOf(err.code) && finalOpts.retries <= MAX_RETRIES) {
            finalOpts.retries++;
            // Use the original callOpts for retry, but keep the incremented retries count
            const retryOpts = { ...callOpts, retries: finalOpts.retries };
            return setTimeout(request, Math.floor(Math.random() * 100), retryOpts, cb);
        }
        cb(err);
    });

    // Write body if it exists
    if (finalOpts.body) {
        req.end(finalOpts.body);
    } else {
        req.end();
    }
}

function opts(target, data) {
    return {
        headers: {
            'Content-Type': 'application/x-amz-json-1.0',
            'X-Amz-Target': config.version + '.' + target,
        },
        body: JSON.stringify(data),
        // Add method here as it's consistent for these opts
        method: 'POST'
    };
}

module.exports = {
    initRequest,
    request,
    opts,
};
