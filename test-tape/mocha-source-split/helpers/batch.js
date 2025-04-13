var async = require('async');
var reqHelpers = require('./request');

function batchBulkPut (name, items, segments, done) {
  if (!done) { done = segments; segments = 2 };

  var itemChunks = [], i;
  for (i = 0; i < items.length; i += 25)
    itemChunks.push(items.slice(i, i + 25));

  async.eachLimit(itemChunks, segments, function (items, cb) { batchWriteUntilDone(name, { puts: items }, cb) }, done);
}

function batchWriteUntilDone (name, actions, cb) {
  var batchReq = { RequestItems: {} }, batchRes = {};
  batchReq.RequestItems[name] = (actions.puts || []).map(function (item) { return { PutRequest: { Item: item } } })
    .concat((actions.deletes || []).map(function (key) { return { DeleteRequest: { Key: key } } }));

  async.doWhilst(
    function (cb) {
      reqHelpers.request(reqHelpers.opts('BatchWriteItem', batchReq), function (err, res) {
        if (err) return cb(err);
        batchRes = res;
        if (res.body.UnprocessedItems && Object.keys(res.body.UnprocessedItems).length) {
          batchReq.RequestItems = res.body.UnprocessedItems;
        }
        else if (/ProvisionedThroughputExceededException/.test(res.body.__type)) {
          console.log('ProvisionedThroughputExceededException'); // eslint-disable-line no-console
          return setTimeout(cb, 2000);
        }
        else if (res.statusCode != 200) {
          return cb(new Error(res.statusCode + ': ' + JSON.stringify(res.body)));
        }
        cb();
      });
    },
    function (cb) {
      var result = (batchRes.body.UnprocessedItems && Object.keys(batchRes.body.UnprocessedItems).length) ||
      /ProvisionedThroughputExceededException/.test(batchRes.body.__type);
      cb(null, result);
    },
    cb
  );
}

exports.batchBulkPut = batchBulkPut;
exports.batchWriteUntilDone = batchWriteUntilDone; 