# Technology Stack

## Core Technologies
- **Runtime**: Node.js (>=16)
- **Database**: LevelDB via LevelUP with memdown for in-memory storage
- **HTTP Server**: Node.js built-in http/https modules
- **Parsing**: PEG.js for expression parsing (condition, projection, update expressions)
- **Cryptography**: Node.js crypto module for hashing and SSL
- **Async Control**: async library for flow control

## Key Dependencies
- `levelup` + `leveldown`/`memdown` - Database layer
- `subleveldown` - Database partitioning
- `big.js` - Precise decimal arithmetic for DynamoDB numbers
- `buffer-crc32` - CRC32 checksums for response validation
- `lazy` - Stream processing utilities
- `pegjs` - Parser generator for expressions
- `minimist` - CLI argument parsing

## Build System
- **Build Command**: `npm run build` - Compiles PEG.js grammar files to JavaScript parsers
- **Test Command**: `npm test` - Runs linting and Mocha test suite
- **Lint Command**: `npm run lint` - ESLint with @architect/eslint-config
- **Coverage**: `npm run coverage` - Test coverage via nyc

## Development Commands
```bash
# Install dependencies
npm install

# Build parsers from grammar files
npm run build

# Run tests (includes linting)
npm test

# Run with coverage
npm run coverage

# Start server programmatically
node index.js

# Start CLI server
node cli.js --port 4567
```

## Parser Generation
The project uses PEG.js to generate parsers from grammar files in `/db/*.pegjs`:
- `conditionParser.pegjs` → `conditionParser.js`
- `projectionParser.pegjs` → `projectionParser.js` 
- `updateParser.pegjs` → `updateParser.js`