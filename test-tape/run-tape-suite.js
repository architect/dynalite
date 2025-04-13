// test-tape/run-tape-suite.js
const path = require('path');
const glob = require('glob');

// This file ensures tests run in the correct order:
// 1. Setup
// 2. Converted tests (dynamically loaded)
// 3. Teardown

console.log('Running Tape test suite via run-tape-suite.js...');

// Require setup first - this executes the setup test
require('./convert-to-tape/00-setup.js');

// Dynamically find and require all converted test files
const testDir = path.join(__dirname, 'convert-to-tape');
const testFiles = glob.sync('*.js', {
  cwd: testDir,
  absolute: true, // Get absolute paths for require
});

console.log(`Found ${testFiles.length} test files to run...`);

testFiles.forEach((file) => {
  // Ensure we don't re-require setup or teardown if they match the pattern
  if (!file.endsWith('00-setup.js') && !file.endsWith('99-teardown.js')) {
    console.log(`Requiring test file: ${path.relative(__dirname, file)}`);
    require(file);
  }
});

// Require teardown last - this executes the teardown test
require('./convert-to-tape/99-teardown.js');

console.log('Finished requiring tests in run-tape-suite.js.'); 