const config = require('./config')
const random = require('./random')
const utils = require('./utils')
const requestHelpers = require('./request')
const tableLifecycle = require('./table-lifecycle')
const tableData = require('./table-data')
const assertions = require('./assertions')

module.exports = {
  // Config exports (excluding internal setters/getters if not needed externally)
  useRemoteDynamo: config.useRemoteDynamo,
  runSlowTests: config.runSlowTests,
  MAX_SIZE: config.MAX_SIZE,
  awsRegion: config.awsRegion,
  getAwsAccountId: config.getAwsAccountId, // Expose getter
  version: config.version,
  prefix: config.prefix,
  readCapacity: config.readCapacity,
  writeCapacity: config.writeCapacity,

  // Random utils
  ...random,

  // General utils
  ...utils,

  // Request utils (only export request and opts, init is internal to setup)
  request: requestHelpers.request,
  opts: requestHelpers.opts,

  // Table lifecycle utils (includes table names)
  ...tableLifecycle,

  // Table data utils
  ...tableData,

  // Assertion utils
  ...assertions,
}
