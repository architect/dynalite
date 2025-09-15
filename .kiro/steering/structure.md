# Project Structure

## Root Files
- `index.js` - Main server module and HTTP request handler
- `cli.js` - Command-line interface entry point
- `package.json` - Project configuration and dependencies

## Core Directories

### `/actions/`
Contains implementation modules for each DynamoDB operation:
- Each file corresponds to a DynamoDB API action (e.g., `listTables.js`, `putItem.js`)
- Functions accept `(store, data, callback)` parameters
- Return results via callback with `(err, data)` signature

### `/validations/`
Input validation and type checking for API operations:
- `index.js` - Core validation framework and utilities
- Individual validation files match action names (e.g., `listTables.js`)
- Each exports `types` object defining parameter validation rules
- May include `custom` validation functions

### `/db/`
Database layer and expression parsing:
- `index.js` - Core database operations and utilities
- `*.pegjs` - PEG.js grammar files for DynamoDB expressions
- `*Parser.js` - Generated parsers (built from .pegjs files)

### `/test/`
Comprehensive test suite:
- `helpers.js` - Test utilities and shared functions
- Individual test files match action names
- Uses Mocha framework with `should` assertions
- Supports both local and remote DynamoDB testing

### `/ssl/`
SSL certificate files for HTTPS support:
- Self-signed certificates for development
- Used when `--ssl` flag is enabled

## Architecture Patterns

### Action Pattern
```javascript
// actions/operationName.js
module.exports = function operationName(store, data, cb) {
  // Implementation
  cb(null, result)
}
```

### Validation Pattern
```javascript
// validations/operationName.js
exports.types = {
  ParameterName: {
    type: 'String',
    required: true,
    // additional constraints
  }
}
```

### Database Operations
- Use `store.tableDb` for table metadata
- Use `store.getItemDb(tableName)` for item storage
- Use `store.getIndexDb()` for secondary indexes
- All operations are asynchronous with callbacks

## Naming Conventions
- Files use camelCase matching DynamoDB operation names
- Action functions use camelCase (e.g., `listTables`, `putItem`)
- Database keys use specific encoding schemes for sorting
- Test files mirror the structure of implementation files