var async = require('async'),
  helpers = require('./helpers')

var target = 'DeleteItem',
  request = helpers.request,
  opts = helpers.opts.bind(null, target),
  assertType = helpers.assertType.bind(null, target),
  assertValidation = helpers.assertValidation.bind(null, target),
  assertConditional = helpers.assertConditional.bind(null, target)