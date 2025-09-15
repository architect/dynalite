# ESLint Migration Tasks (8.57.1 → 9.35.0)

## Overview
ESLint 9.x introduces significant breaking changes including the new "flat config" system, removal of legacy configuration formats, and updated rule behaviors. This migration requires careful handling of configuration changes.

## Detailed Tasks

- [x] 1. Research ESLint 9.x breaking changes and migration requirements
  - Review ESLint 9.x migration guide and breaking changes documentation
  - Understand flat config system vs legacy .eslintrc system
  - Check @architect/eslint-config compatibility with ESLint 9.x
  - Document required configuration changes

- [x] 2. Check @architect/eslint-config compatibility
  - Verify if @architect/eslint-config@2.1.2 supports ESLint 9.x
  - Check for newer version of @architect/eslint-config that supports ESLint 9.x
  - If incompatible, plan migration strategy (custom config or alternative)

- [-] 3. Create new ESLint flat configuration
  - Create eslint.config.js file with flat config format
  - Migrate current configuration from package.json eslintConfig section:
    - extends: "@architect/eslint-config"
    - env: { mocha: true } (will need to change since we're removing Mocha)
    - rules: { "filenames/match-regex": ["error", "^[a-zA-Z0-9-_.]+$", true] }
  - Ensure .eslintignore patterns are preserved (coverage/**, db/*Parser.js)

- [-] 4. Update package.json for ESLint 9.x
  - Remove eslintConfig section from package.json (replaced by eslint.config.js)
  - Update eslint dependency from ^8.48.0 to ^9.35.0
  - Update or replace @architect/eslint-config if needed

- [ ] 5. Handle environment configuration changes
  - Remove "mocha: true" environment since we're migrating to Node.js test runner
  - Add appropriate Node.js test environment configuration if needed
  - Ensure all global variables are properly configured

- [-] 6. Test ESLint configuration
  - Run npm run lint to verify ESLint works with new configuration
  - Fix any linting errors that arise from rule changes in ESLint 9.x
  - Verify all files are being linted correctly
  - Ensure ignored files (.eslintignore) are still being ignored

- [ ] 7. Update npm scripts if needed
  - Verify "lint" script still works: "eslint . --fix"
  - Update script if flat config requires different CLI options
  - Test that linting integrates properly with npm test workflow

- [ ] 8. Handle any rule changes or deprecations
  - Review any deprecated rules that may have been removed in ESLint 9.x
  - Update custom rules if needed
  - Ensure filenames/match-regex rule still works (may need plugin update)

## Key Considerations

### ESLint 9.x Breaking Changes:
- **Flat Config System**: New eslint.config.js format replaces .eslintrc.*
- **Node.js 18.18.0+ Required**: Ensure Node.js version compatibility
- **Removed Legacy Features**: Some legacy configuration options removed
- **Plugin Loading Changes**: Different plugin loading mechanism

### Current Configuration Analysis:
```json
{
  "eslintConfig": {
    "extends": "@architect/eslint-config",
    "env": {
      "mocha": true  // ← Will remove (migrating to Node.js test)
    },
    "rules": {
      "filenames/match-regex": ["error", "^[a-zA-Z0-9-_.]+$", true]
    }
  }
}
```

### Target Flat Configuration:
```javascript
// eslint.config.js
import architectConfig from '@architect/eslint-config'

export default [
  ...architectConfig,
  {
    languageOptions: {
      globals: {
        // Node.js test globals instead of Mocha
      }
    },
    rules: {
      "filenames/match-regex": ["error", "^[a-zA-Z0-9-_.]+$", true]
    }
  }
]
```

## Risk Assessment
- **Medium Risk**: ESLint 9.x has significant breaking changes
- **Dependency Risk**: @architect/eslint-config may not support ESLint 9.x yet
- **Configuration Risk**: Flat config system is completely different from legacy
- **Integration Risk**: May affect npm test workflow if linting fails