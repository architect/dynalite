var async = require('async');
var config = require('./config');
var reqHelpers = require('./request');
var batchHelpers = require('./batch'); // Will be created next

var CREATE_REMOTE_TABLES = true;
var DELETE_REMOTE_TABLES = true;

function createTestTables (done) {
  if (config.useRemoteDynamo && !CREATE_REMOTE_TABLES) return done();
  var readCapacity = config.readCapacity, writeCapacity = config.writeCapacity;
  var tables = [ {
    TableName: config.testHashTable,
    AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' } ],
    KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
    ProvisionedThroughput: { ReadCapacityUnits: readCapacity, WriteCapacityUnits: writeCapacity },
  }, {
    TableName: config.testHashNTable,
    AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'N' } ],
    KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
    BillingMode: 'PAY_PER_REQUEST',
  }, {
    TableName: config.testRangeTable,
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
    TableName: config.testRangeNTable,
    AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'N' } ],
    KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
    ProvisionedThroughput: { ReadCapacityUnits: readCapacity, WriteCapacityUnits: writeCapacity },
  }, {
    TableName: config.testRangeBTable,
    AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'B' } ],
    KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
    ProvisionedThroughput: { ReadCapacityUnits: readCapacity, WriteCapacityUnits: writeCapacity },
  } ];
  async.forEach(tables, createAndWait, done);
}

function getAccountId (done) {
  reqHelpers.request(reqHelpers.opts('DescribeTable', { TableName: config.testHashTable }), function (err, res) {
    if (err) return done(err);
    config.awsAccountId = res.body.Table.TableArn.split(':')[4];
    console.log('Fetched AWS Account ID:', config.awsAccountId); // Log for confirmation
    done();
  });
}

function deleteTestTables (done) {
  if (config.useRemoteDynamo && !DELETE_REMOTE_TABLES) return done();
  reqHelpers.request(reqHelpers.opts('ListTables', {}), function (err, res) {
    if (err) return done(err);
    var names = res.body.TableNames.filter(function (name) { return name.indexOf(config.prefix) === 0 });
    async.forEach(names, deleteAndWait, done);
  });
}

function createAndWait (table, done) {
  reqHelpers.request(reqHelpers.opts('CreateTable', table), function (err, res) {
    if (err) return done(err);
    if (res.statusCode != 200) return done(new Error(res.statusCode + ': ' + JSON.stringify(res.body)));
    setTimeout(waitUntilActive, 1000, table.TableName, done);
  });
}

function deleteAndWait (name, done) {
  reqHelpers.request(reqHelpers.opts('DeleteTable', { TableName: name }), function (err, res) {
    if (err) return done(err);
    if (res.body && res.body.__type == 'com.amazonaws.dynamodb.v20120810#ResourceInUseException')
      return setTimeout(deleteAndWait, 1000, name, done);
    else if (res.statusCode != 200)
      return done(new Error(res.statusCode + ': ' + JSON.stringify(res.body)));
    setTimeout(waitUntilDeleted, 1000, name, done);
  });
}

function waitUntilActive (name, done) {
  reqHelpers.request(reqHelpers.opts('DescribeTable', { TableName: name }), function (err, res) {
    if (err) return done(err);
    if (res.statusCode != 200) return done(new Error(res.statusCode + ': ' + JSON.stringify(res.body)));
    if (res.body.Table.TableStatus == 'ACTIVE' &&
        (!res.body.Table.GlobalSecondaryIndexes ||
          res.body.Table.GlobalSecondaryIndexes.every(function (index) { return index.IndexStatus == 'ACTIVE' }))) {
      return done(null, res);
    }
    setTimeout(waitUntilActive, 1000, name, done);
  });
}

function waitUntilDeleted (name, done) {
  reqHelpers.request(reqHelpers.opts('DescribeTable', { TableName: name }), function (err, res) {
    if (err) return done(err);
    if (res.body && res.body.__type == 'com.amazonaws.dynamodb.v20120810#ResourceNotFoundException')
      return done(null, res);
    else if (res.statusCode != 200)
      return done(new Error(res.statusCode + ': ' + JSON.stringify(res.body)));
    setTimeout(waitUntilDeleted, 1000, name, done);
  });
}

function waitUntilIndexesActive (name, done) {
  reqHelpers.request(reqHelpers.opts('DescribeTable', { TableName: name }), function (err, res) {
    if (err) return done(err);
    if (res.statusCode != 200)
      return done(new Error(res.statusCode + ': ' + JSON.stringify(res.body)));
    else if (res.body.Table.GlobalSecondaryIndexes.every(function (index) { return index.IndexStatus == 'ACTIVE' }))
      return done(null, res);
    setTimeout(waitUntilIndexesActive, 1000, name, done);
  });
}

function deleteWhenActive (name, done) {
  if (!done) done = function () { };
  waitUntilActive(name, function (err) {
    if (err) return done(err);
    reqHelpers.request(reqHelpers.opts('DeleteTable', { TableName: name }), done);
  });
}

function clearTable (name, keyNames, segments, done) {
  if (!done) { done = segments; segments = 2 }
  if (!Array.isArray(keyNames)) keyNames = [ keyNames ];

  scanAndDelete(done);

  function scanAndDelete (cb) {
    async.times(segments, scanSegmentAndDelete, function (err, segmentsHadKeys) {
      if (err) return cb(err);
      if (segmentsHadKeys.some(Boolean)) return scanAndDelete(cb);
      cb();
    });
  }

  function scanSegmentAndDelete (n, cb) {
    reqHelpers.request(reqHelpers.opts('Scan', { TableName: name, AttributesToGet: keyNames, Segment: n, TotalSegments: segments }), function (err, res) {
      if (err) return cb(err);
      if (/ProvisionedThroughputExceededException/.test(res.body.__type)) {
        console.log('ProvisionedThroughputExceededException'); // eslint-disable-line no-console
        return setTimeout(scanSegmentAndDelete, 2000, n, cb);
      }
      else if (res.statusCode != 200) {
        return cb(new Error(res.statusCode + ': ' + JSON.stringify(res.body)));
      }
      if (!res.body.ScannedCount) return cb(null, false);

      var keys = res.body.Items, batchDeletes;

      for (batchDeletes = []; keys.length; keys = keys.slice(25))
        batchDeletes.push(batchHelpers.batchWriteUntilDone.bind(null, name, { deletes: keys.slice(0, 25) }));

      async.parallel(batchDeletes, function (err) {
        if (err) return cb(err);
        cb(null, true);
      });
    });
  }
}

function replaceTable (name, keyNames, items, segments, done) {
  if (!done) { done = segments; segments = 2 };

  clearTable(name, keyNames, segments, function (err) {
    if (err) return done(err);
    batchHelpers.batchBulkPut(name, items, segments, done);
  });
}

exports.createTestTables = createTestTables;
exports.getAccountId = getAccountId;
exports.deleteTestTables = deleteTestTables;
exports.createAndWait = createAndWait;
exports.deleteAndWait = deleteAndWait;
exports.waitUntilActive = waitUntilActive;
exports.waitUntilDeleted = waitUntilDeleted;
exports.waitUntilIndexesActive = waitUntilIndexesActive;
exports.deleteWhenActive = deleteWhenActive;
exports.clearTable = clearTable;
exports.replaceTable = replaceTable; 