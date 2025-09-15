# Design Document

## Overview

This design outlines the technical approach for modernizing Dynalite's dependencies, with primary focus on updating the LevelDB ecosystem and replacing Mocha with Node.js built-in test runner. The modernization will be executed in phases to minimize risk and ensure compatibility.

## Architecture

### Current Dependency Architecture

```
Dynalite Core
├── LevelDB Ecosystem (CRITICAL - ALL DEPRECATED ⚠️)
│   ├── levelup@5.1.1 (DEPRECATED) → level@10.x
│   ├── leveldown@6.1.1 → classic-level (via level@10.x)
│   ├── memdown@6.1.1 (DEPRECATED) → memory-level@3.x
│   └── subleveldown@6.0.1 (DEPRECATED) → abstract-level sublevels
├── Test Framework (SECONDARY PRIORITY)
│   ├── mocha@10.2.0 → node:test (built-in)
│   └── should@13.2.3 → node:assert (built-in)
├── Built-in Replacements
│   ├── once@1.4.0 → custom wrapper
│   └── minimist@1.2.8 → util.parseArgs()
└── Other Dependencies
    ├── async@3.2.4 → async@3.x (latest)
    ├── big.js@6.2.1 → big.js@6.x (latest)
    └── others → latest versions
```

### Target Architecture

```
Modernized Dynalite
├── LevelDB Ecosystem (MODERN ABSTRACT-LEVEL)
│   ├── level@10.x (replaces levelup + leveldown)
│   ├── memory-level@3.x (replaces memdown)
│   ├── abstract-level sublevels (replaces subleveldown)
│   └── classic-level (native binding via level)
├── Node.js Built-ins
│   ├── node:test (replaces mocha)
│   ├── node:assert (replaces should)
│   ├── util.parseArgs() (replaces minimist)
│   └── custom once() wrapper
└── Updated Dependencies
    └── All other deps at latest stable versions
```

## Components and Interfaces

### 1. LevelDB Ecosystem Migration

#### Current Implementation (DEPRECATED PACKAGES)
```javascript
// db/index.js - Current (ALL DEPRECATED ⚠️)
var levelup = require('levelup'),        // DEPRECATED
    memdown = require('memdown'),        // DEPRECATED  
    sub = require('subleveldown')        // DEPRECATED

var db = levelup(options.path ? require('leveldown')(options.path) : memdown())
```

#### Target Implementation (MODERN ABSTRACT-LEVEL)
```javascript
// db/index.js - Target
var { Level } = require('level'),           // Modern replacement
    { MemoryLevel } = require('memory-level') // Modern replacement

var db = options.path ? 
  new Level(options.path) : 
  new MemoryLevel()

// Sublevel functionality now built into abstract-level
function getSubDb(name) {
  return db.sublevel(name, { valueEncoding: 'json' })
}
```

#### Migration Strategy (DEPRECATED → MODERN)
- **Phase 1**: Research abstract-level migration path and compatibility
- **Phase 2**: Replace deprecated packages with modern abstract-level ecosystem
  - levelup@5.1.1 (DEPRECATED) → level@10.x
  - memdown@6.1.1 (DEPRECATED) → memory-level@3.x  
  - subleveldown@6.0.1 (DEPRECATED) → built-in sublevel functionality
- **Phase 3**: Update all database access patterns to use new APIs
- **Phase 4**: Test data compatibility and performance extensively

### 2. Test Framework Migration

#### Current Test Structure
```javascript
// test/listTables.js - Current
var should = require('should'),
    async = require('async'),
    helpers = require('./helpers')

describe('listTables', function() {
  it('should return empty list', function(done) {
    // test implementation
  })
})
```

#### Target Test Structure
```javascript
// test/listTables.js - Target
import { test, describe } from 'node:test'
import assert from 'node:assert'
import helpers from './helpers.js'

describe('listTables', () => {
  test('should return empty list', async () => {
    // test implementation with assert
  })
})
```

#### Migration Strategy
- Convert 20 test files from Mocha to Node.js test runner
- Replace `should` assertions with `node:assert`
- Maintain existing test helper patterns
- Update npm scripts for new test runner

### 3. Built-in Replacements

#### Once Module Replacement
```javascript
// Current usage
var once = require('once')
cb = once(cb)

// Target replacement
function once(fn) {
  let called = false
  return function(...args) {
    if (called) return
    called = true
    return fn.apply(this, args)
  }
}
```

#### Minimist Replacement
```javascript
// cli.js - Current
var argv = require('minimist')(process.argv.slice(2), { 
  alias: { debug: ['d'], verbose: ['v'] } 
})

// cli.js - Target
import { parseArgs } from 'node:util'
const { values: argv } = parseArgs({
  args: process.argv.slice(2),
  options: {
    debug: { type: 'boolean', short: 'd' },
    verbose: { type: 'boolean', short: 'v' },
    help: { type: 'boolean', short: 'h' },
    port: { type: 'string' },
    host: { type: 'string' },
    path: { type: 'string' },
    ssl: { type: 'boolean' }
  }
})
```

## Data Models

### LevelDB Data Compatibility

The LevelDB ecosystem update must maintain compatibility with existing data formats:

```javascript
// Key encoding remains identical
function createKey(item, table, keySchema) {
  // Existing key creation logic preserved
  return keyStr
}

// Value encoding remains identical  
function itemSize(item, compress, addMetaSize, rangeKey) {
  // Existing size calculation preserved
  return size
}
```

### Test Data Migration

Test helpers and data structures remain unchanged:

```javascript
// test/helpers.js - Interface preserved
exports.testHashTable = 'test_table_name'
exports.request = request
exports.opts = opts
// All existing helper functions maintained
```

## Error Handling

### LevelDB Error Compatibility
- Ensure error types and messages remain consistent
- Map new LevelDB errors to existing error patterns
- Maintain existing error handling in actions/

### Test Framework Error Handling
- Convert Mocha error patterns to Node.js test patterns
- Preserve existing assertion error messages
- Maintain test timeout and async error handling

## Testing Strategy

### Phase 1: LevelDB Ecosystem Testing
1. **Compatibility Tests**: Verify existing data can be read/written
2. **Performance Tests**: Ensure no regression in operation speed
3. **Integration Tests**: Full DynamoDB API operation testing
4. **Migration Tests**: Test upgrade path from old to new versions

### Phase 2: Test Framework Migration Testing
1. **Conversion Verification**: Each test file converted and verified
2. **Coverage Maintenance**: Ensure test coverage remains identical
3. **CI/CD Integration**: Update GitHub Actions for new test runner
4. **Helper Function Testing**: Verify all test utilities work

### Phase 3: Built-in Replacement Testing
1. **CLI Testing**: Verify all command-line options work identically
2. **Callback Testing**: Ensure once() wrapper functions correctly
3. **Edge Case Testing**: Test error conditions and unusual inputs

## Implementation Phases

### Phase 1: LevelDB Ecosystem Migration (CRITICAL PRIORITY - DEPRECATED PACKAGES)
**Duration**: 3-4 days
**Risk**: High (deprecated packages, API changes, data compatibility)

1. Research abstract-level migration guide and breaking changes
2. Replace deprecated packages in package.json:
   - Remove: levelup, memdown, subleveldown (all DEPRECATED ⚠️)
   - Add: level@10.x, memory-level@3.x
3. Rewrite db/index.js for abstract-level API
4. Update all sublevel usage to use built-in sublevel functionality
5. Run comprehensive tests to verify data compatibility
6. Performance benchmarking to ensure no regression

### Phase 2: Test Framework Migration (MEDIUM PRIORITY)  
**Duration**: 3-4 days
**Risk**: Medium (test coverage)

1. Convert test/helpers.js to work with Node.js test runner
2. Convert individual test files (20 files) from Mocha to node:test
3. Replace should assertions with node:assert
4. Update npm scripts and package.json
5. Verify all tests pass with identical coverage

### Phase 3: Built-in Replacements (LOW PRIORITY)
**Duration**: 1-2 days  
**Risk**: Low (simple replacements)

1. Replace once module with custom implementation
2. Replace minimist with util.parseArgs()
3. Update Node.js version requirement to >=20
4. Test CLI functionality thoroughly

### Phase 4: Remaining Dependencies (LOW PRIORITY)
**Duration**: 1 day
**Risk**: Low (version updates)

1. Update all remaining dependencies to latest versions
2. Update ESLint and development tools
3. Replace PEG.js with @peggyjs/peggy if needed
4. Final integration testing

## Risk Mitigation

### LevelDB Data Loss Prevention
- Create backup/restore utilities for testing
- Implement rollback strategy for LevelDB changes
- Test with existing production-like data sets

### Test Coverage Preservation
- Automated test conversion verification
- Coverage reporting comparison (before/after)
- Manual verification of critical test paths

### Performance Regression Prevention
- Benchmark existing performance before changes
- Continuous performance monitoring during updates
- Rollback plan if performance degrades significantly

## Success Criteria

1. **LevelDB Ecosystem**: All dependencies updated, data compatibility maintained, performance preserved
2. **Test Framework**: All tests converted to Node.js test runner, coverage maintained
3. **Built-ins**: Successfully replaced once and minimist with Node.js built-ins
4. **Compatibility**: All existing APIs work identically, no breaking changes
5. **Security**: No new vulnerabilities introduced, dependency count reduced