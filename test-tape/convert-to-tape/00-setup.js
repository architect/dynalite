// test-tape/tape-tests/00-setup.js
const test = require('tape')
const dynalite = require('../../') // Adjust path to dynalite main index.js
const config = require('./helpers/config')
const tableLifecycle = require('./helpers/table-lifecycle')
const requestHelpers = require('./helpers/request') // Needed to set the port

let serverInstance
let serverPort

test('Setup: Start Dynalite Server and Create Tables', (t) => {
  // Use a long timeout for setup if needed, tape doesn't enforce default timeouts
  // const setupTimeout = setTimeout(() => { ... }, 210000);

  console.log('Running Tape setup...')

  // Only run setup if not using remote DynamoDB
  if (config.useRemoteDynamo) {
    console.log('REMOTE environment variable set. Skipping local Dynalite setup.')
    // Potentially still fetch Account ID if needed
    // tableLifecycle.getAccountId(t.end); // Example if needed
    t.end()
    return
  }

  serverInstance = dynalite({ path: process.env.DYNALITE_PATH /* create: true, delete: true */ }) // Add options if needed
  serverPort = 10000 + Math.round(Math.random() * 10000)

  // IMPORTANT: Update the request helper config to use the correct port
  const baseRequestOpts = { host: '127.0.0.1', port: serverPort, method: 'POST' }
  requestHelpers.initRequest(baseRequestOpts) // Re-initialize with the chosen port
  console.log(`Attempting to start Dynalite server on port ${serverPort}...`)

  serverInstance.listen(serverPort, (err) => {
    if (err) {
      t.fail(`Dynalite server failed to start on port ${serverPort}: ${err.message}`)
      // clearTimeout(setupTimeout);
      t.end()
      process.exit(1) // Exit forcefully if server fails
      return
    }
    console.log(`Dynalite server started successfully on port ${serverPort}.`)
    console.log('Creating test tables...')

    tableLifecycle.createTestTables((tableErr) => {
      // clearTimeout(setupTimeout);
      if (tableErr) {
        t.error(tableErr, 'Error creating test tables')
        // Attempt to close server even if table creation failed
        return serverInstance.close(() => t.end())
      }
      console.log('Test tables created successfully.')
      console.log('Tape setup finished.')
      t.end()
    })
  })
})

// Export for teardown or direct use (though helpers should use config/request)
module.exports = {
  getServerInstance: () => serverInstance,
  getServerPort: () => serverPort,
}
