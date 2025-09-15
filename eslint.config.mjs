import architectConfig from '@architect/eslint-config'

export default [
  ...architectConfig,
  {
    ignores: [
      'coverage/**',
      'db/*Parser.js',
    ],
  },
  {
    files: [ 'test/**/*.js' ],
    languageOptions: {
      globals: {
        describe: 'readonly',
        it: 'readonly',
        before: 'readonly',
        after: 'readonly',
        beforeEach: 'readonly',
        afterEach: 'readonly',
      },
    },
  },
  {
    // Override filename rule to allow camelCase (which this project uses extensively)
    rules: {
      'arc/match-regex': 'off',
    },
  },
]
