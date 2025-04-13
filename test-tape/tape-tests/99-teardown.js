// test-tape/tape-tests/99-teardown.js
const test = require('tape');
const { afterAllCleanup } = require('../mocha-source-split/helpers/setup');

test('Teardown: Delete Tables and Stop Dynalite Server', function (t) {
    // Use a long timeout for cleanup
    const teardownTimeout = setTimeout(() => {
        t.fail('Teardown timed out after 510 seconds');
        // Don't exit here, allow other tests to potentially finish
        t.end();
    }, 510000);

    console.log("Running Tape teardown...");
    afterAllCleanup((err) => {
        clearTimeout(teardownTimeout);
        // Log the error but don't fail the teardown test itself
        // as failing here might obscure earlier test failures.
        if (err) {
            console.error("Error during Tape teardown:", err);
        }
        console.log("Tape teardown finished.");
        t.end();
    });
}); 