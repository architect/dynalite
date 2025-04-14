// test-tape/tape-tests/99-teardown.js
const test = require('tape')
const config = require('./helpers/config')
const tableLifecycle = require('./helpers/table-lifecycle')
const setup = require('./00-setup.js') // Get access to the server instance

test('Teardown: Delete Tables and Stop Dynalite Server', (t) => {
  // const teardownTimeout = setTimeout(() => { ... }, 510000);

  console.log('Running Tape teardown...')

  const serverInstance = setup.getServerInstance()

  // Only run teardown if not using remote and if server instance exists
  if (config.useRemoteDynamo || !serverInstance) {
    console.log('REMOTE environment variable set or no local server instance found. Skipping local Dynalite teardown.')
    // clearTimeout(teardownTimeout)
    t.end()
    return
  }

  console.log('Deleting test tables...')
  tableLifecycle.deleteTestTables((deleteErr) => {
    if (deleteErr) {
      // Log error but don't fail the test, proceed to close server
      console.error('Error deleting test tables during teardown:', deleteErr)
      t.comment(`Error deleting test tables: ${deleteErr.message}`)
    }
    else {
      console.log('Test tables deleted successfully.')
    }

    console.log('Stopping Dynalite server...')
    serverInstance.close((closeErr) => {
      // clearTimeout(teardownTimeout)
      if (closeErr) {
        console.error('Error stopping Dynalite server during teardown:', closeErr)
        t.error(closeErr, 'Server should close cleanly') // Report server close error
      }
      else {
        console.log('Dynalite server stopped successfully.')
      }
      console.log('Tape teardown finished.')
      // Pass table deletion error if it occurred, otherwise pass server close error
      t.error(deleteErr || closeErr, 'Teardown should complete without critical errors')
      t.end()
    })
  })
})
