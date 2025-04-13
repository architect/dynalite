// test-tape/run-tape-suite.js

// This file ensures tests run in the correct order:
// 1. Setup
// 2. Converted tests
// 3. Teardown

console.log('Running Tape test suite via run-tape-suite.js...');

// Require setup first - this executes the setup test
require('./convert-to-tape/00-setup.js');

// Require the converted test files
// As we convert more, add them here or use dynamic loading (e.g., glob)
require('./convert-to-tape/bench.test.js');
require('./convert-to-tape/getItem.part1.test.js');

// Require teardown last - this executes the teardown test
require('./convert-to-tape/99-teardown.js');

console.log('Finished requiring tests in run-tape-suite.js.'); 