// test-tape/tape-tests/00-setup.js
const test = require('tape');
const { beforeAllSetup } = require('../mocha-source-split/helpers/setup');

test('Setup: Start Dynalite Server and Create Tables', function (t) {
    // Use a long timeout for setup
    const setupTimeout = setTimeout(() => {
        t.fail('Setup timed out after 210 seconds');
        t.end();
        process.exit(1); // Exit forcefully on timeout
    }, 210000);

    console.log("Running Tape setup...");
    beforeAllSetup((err) => {
        clearTimeout(setupTimeout);
        t.error(err, 'beforeAllSetup should complete without error');
        console.log("Tape setup finished.");
        t.end();
    });
}); 