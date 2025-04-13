// test-tape/mocha-source-split/mocha-hooks.js

const { beforeAllSetup, afterAllCleanup } = require('./helpers/setup');

exports.mochaHooks = {
  beforeAll: function(done) {
    // Increase timeout significantly for setup
    this.timeout(210000); // Adjust as needed
    console.log("Running root beforeAll hook...");
    beforeAllSetup(done);
  },
  afterAll: function(done) {
    // Increase timeout significantly for cleanup
    this.timeout(510000); // Adjust as needed
    console.log("Running root afterAll hook...");
    afterAllCleanup(done);
  }
}; 