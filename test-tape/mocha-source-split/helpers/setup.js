var dynalite = require('../../..'); // Path relative to helpers dir
var tables = require('./tables');
var reqHelpers = require('./request');

// This holds the dynalite server instance
var dynaliteServer;

// Mocha hooks - these will be replaced/adapted for Tape later

function beforeAllSetup(done) {
  // `this` context from Mocha is not used here
  console.log('Starting Dynalite server for Mocha tests...');
  dynaliteServer = dynalite({ path: process.env.DYNALITE_PATH });
  dynaliteServer.listen(reqHelpers.port, function (err) {
    if (err) return done(err);
    console.log('Dynalite server started on port:', reqHelpers.port);
    tables.createTestTables(function (err) {
      if (err) return done(err);
      tables.getAccountId(done); // Note: Side effect removed, just calls done
    });
  });
}

function afterAllCleanup(done) {
  // `this` context from Mocha is not used here
  console.log('Stopping Dynalite server for Mocha tests...');
  tables.deleteTestTables(function (err) {
    // Log error but continue
    if (err) console.error('Error deleting test tables:', err);
    if (dynaliteServer) {
        dynaliteServer.close(function(closeErr) {
            if (closeErr) console.error('Error closing dynalite server:', closeErr);
            else console.log('Dynalite server stopped.');
            done(err || closeErr); // Pass first error if any
        });
    } else {
        done(err); // Pass table deletion error if any
    }
  });
}

exports.beforeAllSetup = beforeAllSetup;
exports.afterAllCleanup = afterAllCleanup;
exports.getDynaliteServer = () => dynaliteServer; // Allow access if needed 