// test-tape/mocha-source-split/helpers/setup.js
// Note: This file primarily initializes request options and exports helpers.
// The original Mocha before/after hooks have been moved to 00-setup.js and 99-teardown.js.

// Core Node modules
const http = require('http')

// Dependencies
require('should') // Extends Object.prototype, needed globally
// const dynalite = require('../../../') // Moved to 00-setup.js

// Our helper modules
// const config = require('./config') // Moved to 00-setup.js
// const requestHelpers = require('./request') // Moved to 00-setup.js
// const tableLifecycle = require('./table-lifecycle') // Moved to 00-setup.js / 99-teardown.js
const allHelpers = require('./index') // Get all aggregated helpers

// --- Global Setup & Teardown ---

// Configure global agent
http.globalAgent.maxSockets = Infinity

// Dynalite server instance creation and listening moved to 00-setup.js
// const dynaliteServer = dynalite({ path: process.env.DYNALITE_PATH })
// const port = 10000 + Math.round(Math.random() * 10000); // Port generation might still be needed if referenced elsewhere?
// *** TODO: Check if this random port generation is still needed or if port should be obtained from setup module ***

// Determine base request options based on environment
/* // Base request options are now determined and set in 00-setup.js
const baseRequestOpts = config.useRemoteDynamo
  ? { host: `dynamodb.${config.awsRegion}.amazonaws.com`, method: 'POST' }
  // Port for local is now set dynamically in 00-setup.js and passed to initRequest there.
  // We might not need to set it here, but keeping for potential reference.
  : { host: '127.0.0.1', method: 'POST' };
*/

// Initialize the request helper with base options
// requestHelpers.initRequest(baseRequestOpts) // This is now done in 00-setup.js with the *actual* port

// NOTE: Mocha hooks `before` and `after` were removed here during Tape migration.
// Original before logic (for reference - TO BE MOVED/REPLACED):
/*
function setupServerAndTables(done) {
  // this.timeout(200000) // Increase timeout for setup
  console.log(`Starting Dynalite server on port ${port}...`)
  dynaliteServer.listen(port, (err) => {
    if (err) return done(err)
    console.log('Dynalite server started. Creating test tables...')
    tableLifecycle.createTestTables((err) => {
      if (err) {
        console.error('Error creating test tables:', err)
        // Attempt to close server even if table creation failed
        return dynaliteServer.close(() => done(err))
      }
      console.log('Test tables created. Fetching Account ID...')
      // Only get account ID if using remote, otherwise it's not needed/available
      if (config.useRemoteDynamo) {
        tableLifecycle.getAccountId((err) => {
          if (err) {
            console.error('Error fetching AWS Account ID:', err)
            return dynaliteServer.close(() => done(err))
          }
          console.log(`AWS Account ID: ${config.getAwsAccountId()}`)
          console.log('Setup complete.')
          done()
        })
      }
      else {
        console.log('Using local Dynalite, skipping Account ID fetch.')
        console.log('Setup complete.')
        done()
      }
    })
  })
}
*/

// Original after logic (for reference - TO BE MOVED/REPLACED):
/*
function teardownServerAndTables(done) {
  // this.timeout(500000) // Increase timeout for teardown
  console.log('Deleting test tables...')
  tableLifecycle.deleteTestTables((err) => {
    if (err) {
      console.error('Error deleting test tables:', err)
      // Still try to close the server
    }
    else {
      console.log('Test tables deleted.')
    }
    console.log('Stopping Dynalite server...')
    dynaliteServer.close((closeErr) => {
      if (closeErr) {
        console.error('Error stopping Dynalite server:', closeErr)
        return done(err || closeErr) // Report original error if it exists, else close error
      }
      console.log('Dynalite server stopped. Teardown complete.')
      done(err) // Report potential table deletion error
    })
  })
}
*/

// --- Exports ---
// Export all helpers for test files to use
module.exports = allHelpers
