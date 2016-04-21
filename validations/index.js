var Big = require('big.js'),
    db = require('../db'),
    conditionParser = require('../db/conditionParser'),
    projectionParser = require('../db/projectionParser'),
    updateParser = require('../db/updateParser')

exports.checkTypes = checkTypes
exports.checkValidations = checkValidations
exports.toLowerFirst = toLowerFirst
exports.findDuplicate = findDuplicate
exports.validateAttributeValue = validateAttributeValue
exports.validateConditions = validateConditions
exports.validateAttributeConditions = validateAttributeConditions
exports.validateExpressionParams = validateExpressionParams
exports.validateExpressions = validateExpressions
exports.convertKeyCondition = convertKeyCondition

function checkTypes(data, types) {
  var key
  for (key in data) {
    // TODO: deal with nulls
    if (!types[key] || data[key] == null)
      delete data[key]
  }

  return Object.keys(types).reduce(function(newData, key) {
    var val = checkType(data[key], types[key])
    if (val != null) newData[key] = val
    return newData
  }, {})

  function typeError(msg) {
    var err = new Error(msg)
    err.statusCode = 400
    err.body = {
      __type: 'com.amazon.coral.service#SerializationException',
      Message: msg,
    }
    return err
  }

  function checkType(val, type) {
    if (val == null) return null
    var children = type.children
    if (typeof children == 'string') children = {type: children}
    if (type.type) type = type.type

    if (type == 'AttrStructure') {
      type = 'Structure'
      children = {
        S: 'String',
        B: 'Blob',
        N: 'String',
        BOOL: 'Boolean',
        NULL: 'Boolean',
        BS: {
          type: 'List',
          children: 'Blob',
        },
        NS: {
          type: 'List',
          children: 'String',
        },
        SS: {
          type: 'List',
          children: 'String',
        },
        L: {
          type: 'List',
          children: 'AttrStructure',
        },
        M: {
          type: 'Map',
          children: 'AttrStructure',
        },
      }
    }

    switch (type) {
      case 'Boolean':
        switch (typeof val) {
          case 'number':
            throw typeError('class com.amazon.coral.value.json.numbers.TruncatingBigNumber can not be converted to an Boolean')
          case 'string':
            // "\'HELLOWTF\' can not be converted to an Boolean"
            // seems to convert to uppercase
            // 'true'/'false'/'1'/'0'/'no'/'yes' seem to convert fine
            val = val.toUpperCase()
            if (~['TRUE', '1', 'YES'].indexOf(val)) {
              val = true
            } else if (~['FALSE', '0', 'NO'].indexOf(val)) {
              val = false
            } else {
              throw typeError('\'' + val + '\' can not be converted to an Boolean')
            }
            break
          case 'object':
            if (Array.isArray(val)) throw typeError('Start of list found where not expected')
            throw typeError('Start of structure or map found where not expected.')
        }
        return val
      case 'Short':
      case 'Integer':
      case 'Long':
      case 'Double':
        switch (typeof val) {
          case 'boolean':
            throw typeError('class java.lang.Boolean can not be converted to an ' + type)
          case 'number':
            if (type != 'Double') val = Math.floor(val)
            break
          case 'string':
            throw typeError('class java.lang.String can not be converted to an ' + type)
          case 'object':
            if (Array.isArray(val)) throw typeError('Start of list found where not expected')
            throw typeError('Start of structure or map found where not expected.')
        }
        return val
      case 'String':
        switch (typeof val) {
          case 'boolean':
            throw typeError('class java.lang.Boolean can not be converted to an String')
          case 'number':
            throw typeError('class com.amazon.coral.value.json.numbers.TruncatingBigNumber can not be converted to an String')
          case 'object':
            if (Array.isArray(val)) throw typeError('Start of list found where not expected')
            throw typeError('Start of structure or map found where not expected.')
        }
        return val
      case 'Blob':
        switch (typeof val) {
          case 'boolean':
            throw typeError('class java.lang.Boolean can not be converted to a Blob')
          case 'number':
            throw typeError('class com.amazon.coral.value.json.numbers.TruncatingBigNumber can not be converted to a Blob')
          case 'object':
            if (Array.isArray(val)) throw typeError('Start of list found where not expected')
            throw typeError('Start of structure or map found where not expected.')
        }
        if (val.length % 4)
          throw typeError('\'' + val + '\' can not be converted to a Blob: ' +
            'Base64 encoded length is expected a multiple of 4 bytes but found: ' + val.length)
        var match = val.match(/[^a-zA-Z0-9+/=]|\=[^=]/)
        if (match)
          throw typeError('\'' + val + '\' can not be converted to a Blob: ' +
            'Invalid Base64 character: \'' + match[0][0] + '\'')
        // TODO: need a better check than this...
        if (new Buffer(val, 'base64').toString('base64') != val)
          throw typeError('\'' + val + '\' can not be converted to a Blob: ' +
            'Invalid last non-pad Base64 character dectected')
        return val
      case 'List':
        switch (typeof val) {
          case 'boolean':
          case 'number':
          case 'string':
            throw typeError('Expected list or null')
          case 'object':
            if (!Array.isArray(val)) throw typeError('Start of structure or map found where not expected.')
        }
        return val.map(function(child) { return checkType(child, children) })
      case 'Map':
        switch (typeof val) {
          case 'boolean':
          case 'number':
          case 'string':
            throw typeError('Expected map or null')
          case 'object':
            if (Array.isArray(val)) throw typeError('Start of list found where not expected')
        }
        return Object.keys(val).reduce(function(newVal, key) {
          newVal[key] = checkType(val[key], children)
          return newVal
        }, {})
      case 'Structure':
        switch (typeof val) {
          case 'boolean':
          case 'number':
          case 'string':
            throw typeError('Expected null')
          case 'object':
            if (Array.isArray(val)) throw typeError('Start of list found where not expected')
        }
        return checkTypes(val, children)
      default:
        throw new Error('Unknown type: ' + type)
    }
  }
}

var validateFns = {}

function checkValidations(data, validations, custom, store) {
  var attr, msg, errors = []
  function validationError(msg) {
    var err = new Error(msg)
    err.statusCode = 400
    err.body = {
      __type: 'com.amazon.coral.validate#ValidationException',
      message: msg,
    }
    return err
  }

  for (attr in validations) {
    if (validations[attr].required && data[attr] == null) {
      throw validationError('The parameter \'' + attr + '\' is required but was not present in the request')
    }
    if (validations[attr].tableName) {
      msg = validateTableName(attr, data[attr])
      if (msg) throw validationError(msg)
    }
  }

  function checkNonRequireds(data, types, parent) {
    for (attr in types) {
      checkNonRequired(attr, data[attr], types[attr], parent)
    }
  }

  checkNonRequireds(data, validations)

  function checkNonRequired(attr, data, validations, parent) {
    if (validations == null || typeof validations != 'object') return
    for (var validation in validations) {
      if (errors.length >= 10) return
      if (~['type', 'required', 'tableName'].indexOf(validation)) continue
      if (validation != 'notNull' && data == null) continue
      if (validation == 'children') {
        if (validations.type == 'List') {
          for (var i = 0; i < data.length; i++) {
            checkNonRequired('member', data[i], validations.children,
              (parent ? parent + '.' : '') + toLowerFirst(attr) + '.' + (i + 1))
          }
          continue
        } else if (validations.type == 'Map') {
          // TODO: Always reverse?
          Object.keys(data).reverse().forEach(function(key) { // eslint-disable-line no-loop-func
            checkNonRequired('member', data[key], validations.children,
              (parent ? parent + '.' : '') + toLowerFirst(attr) + '.' + key)
          })
          continue
        }
        checkNonRequireds(data, validations.children, (parent ? parent + '.' : '') + toLowerFirst(attr))
        continue
      }
      validateFns[validation](parent, attr, validations[validation], data, errors)
    }
  }

  if (errors.length)
    throw validationError(errors.length + ' validation error' + (errors.length > 1 ? 's' : '') + ' detected: ' + errors.join('; '))

  if (custom) {
    msg = custom(data, store)
    if (msg) throw validationError(msg)
  }
}

validateFns.notNull = function(parent, key, val, data, errors) {
  validate(data != null, 'Member must not be null', data, parent, key, errors)
}
validateFns.greaterThanOrEqual = function(parent, key, val, data, errors) {
  validate(data >= val, 'Member must have value greater than or equal to ' + val, data, parent, key, errors)
}
validateFns.lessThanOrEqual = function(parent, key, val, data, errors) {
  validate(data <= val, 'Member must have value less than or equal to ' + val, data, parent, key, errors)
}
validateFns.regex = function(parent, key, pattern, data, errors) {
  validate(RegExp('^' + pattern + '$').test(data), 'Member must satisfy regular expression pattern: ' + pattern, data, parent, key, errors)
}
validateFns.lengthGreaterThanOrEqual = function(parent, key, val, data, errors) {
  var length = (typeof data == 'object' && !Array.isArray(data)) ? Object.keys(data).length : data.length
  validate(length >= val, 'Member must have length greater than or equal to ' + val, data, parent, key, errors)
}
validateFns.lengthLessThanOrEqual = function(parent, key, val, data, errors) {
  var length = (typeof data == 'object' && !Array.isArray(data)) ? Object.keys(data).length : data.length
  validate(length <= val, 'Member must have length less than or equal to ' + val, data, parent, key, errors)
}
validateFns.enum = function(parent, key, val, data, errors) {
  validate(~val.indexOf(data), 'Member must satisfy enum value set: [' + val.join(', ') + ']', data, parent, key, errors)
}
validateFns.keys = function(parent, key, val, data, errors) {
  Object.keys(data).forEach(function(mapKey) {
    try {
      Object.keys(val).forEach(function(validation) {
        validateFns[validation]('', '', val[validation], mapKey, [])
      })
    } catch (e) {
      var msgs = Object.keys(val).map(function(validation) {
        if (validation == 'lengthGreaterThanOrEqual')
          return 'Member must have length greater than or equal to ' + val[validation]
        if (validation == 'lengthLessThanOrEqual')
          return 'Member must have length less than or equal to ' + val[validation]
        if (validation == 'regex')
          return 'Member must satisfy regular expression pattern: ' + val[validation]
      })
      validate(false, 'Map keys must satisfy constraint: [' + msgs.join(', ') + ']', data, parent, key, errors)
    }
  })
}
validateFns.values = function(parent, key, val, data, errors) {
  Object.keys(data).forEach(function(mapKey) {
    try {
      Object.keys(val).forEach(function(validation) {
        validateFns[validation]('', '', val[validation], data[mapKey], [])
      })
    } catch (e) {
      var msgs = Object.keys(val).map(function(validation) {
        if (validation == 'lengthGreaterThanOrEqual')
          return 'Member must have length greater than or equal to ' + val[validation]
        if (validation == 'lengthLessThanOrEqual')
          return 'Member must have length less than or equal to ' + val[validation]
      })
      validate(false, 'Map value must satisfy constraint: [' + msgs.join(', ') + ']', data, parent, key, errors)
    }
  })
}

function validate(predicate, msg, data, parent, key, errors) {
  if (predicate) return
  var value = valueStr(data)
  if (value != 'null') value = '\'' + value + '\''
  parent = parent ? parent + '.' : ''
  errors.push('Value ' + value + ' at \'' + parent + toLowerFirst(key) + '\' failed to satisfy constraint: ' + msg)
}

function validateTableName(key, val) {
  if (val == null) return null
  if (val.length < 3 || val.length > 255)
    return key + ' must be at least 3 characters long and at most 255 characters long'
}

function toLowerFirst(str) {
  return str[0].toLowerCase() + str.slice(1)
}

function validateAttributeValue(value) {
  var types = Object.keys(value), msg, i, attr
  if (!types.length)
    return 'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes'

  for (var type in value) {
    if (type == 'N') {
      msg = checkNum(type, value)
      if (msg) return msg
    }

    if (type == 'B' && !value[type])
      return 'One or more parameter values were invalid: An AttributeValue may not contain a null or empty binary type.'

    if (type == 'S' && !value[type])
      return 'One or more parameter values were invalid: An AttributeValue may not contain an empty string'

    if (type == 'NULL' && !value[type])
      return 'One or more parameter values were invalid: Null attribute value types must have the value of true'

    if (type == 'SS' && !value[type].length)
      return 'One or more parameter values were invalid: An string set  may not be empty'

    if (type == 'NS' && !value[type].length)
      return 'One or more parameter values were invalid: An number set  may not be empty'

    if (type == 'BS' && !value[type].length)
      return 'One or more parameter values were invalid: Binary sets should not be empty'

    if (type == 'SS' && value[type].some(function(x) { return !x })) // eslint-disable-line no-loop-func
      return 'One or more parameter values were invalid: An string set may not have a empty string as a member'

    if (type == 'BS' && value[type].some(function(x) { return !x })) // eslint-disable-line no-loop-func
      return 'One or more parameter values were invalid: Binary sets may not contain null or empty values'

    if (type == 'NS') {
      for (i = 0; i < value[type].length; i++) {
        msg = checkNum(i, value[type])
        if (msg) return msg
      }
    }

    if (type == 'SS' && findDuplicate(value[type]))
      return 'One or more parameter values were invalid: Input collection ' + valueStr(value[type]) + ' contains duplicates.'

    if (type == 'NS' && findDuplicate(value[type]))
      return 'Input collection contains duplicates'

    if (type == 'BS' && findDuplicate(value[type]))
      return 'One or more parameter values were invalid: Input collection ' + valueStr(value[type]) + 'of type BS contains duplicates.'

    if (type == 'M') {
      for (attr in value[type]) {
        msg = validateAttributeValue(value[type][attr])
        if (msg) return msg
      }
    }

    if (type == 'L') {
      for (i = 0; i < value[type].length; i++) {
        msg = validateAttributeValue(value[type][i])
        if (msg) return msg
      }
    }
  }

  if (types.length > 1)
    return 'Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes'
}

function checkNum(attr, obj) {
  if (!obj[attr])
    return 'The parameter cannot be converted to a numeric value'

  var bigNum
  try {
    bigNum = new Big(obj[attr])
  } catch (e) {
    return 'The parameter cannot be converted to a numeric value: ' + obj[attr]
  }
  if (bigNum.e > 125)
    return 'Number overflow. Attempting to store a number with magnitude larger than supported range'
  else if (bigNum.e < -130)
    return 'Number underflow. Attempting to store a number with magnitude smaller than supported range'
  else if (bigNum.c.length > 38)
    return 'Attempting to store more than 38 significant digits in a Number'

  obj[attr] = bigNum.toFixed()
}

function valueStr(data) {
  return data == null ? 'null' : Array.isArray(data) ? '[' + data.map(valueStr).join(', ') + ']' :
    typeof data == 'object' ? JSON.stringify(data) : data
}

function findDuplicate(arr) {
  if (!arr) return null
  var vals = Object.create(null)
  for (var i = 0; i < arr.length; i++) {
    if (vals[arr[i]]) return arr[i]
    vals[arr[i]] = true
  }
}

function validateAttributeConditions(data) {
  for (var key in data.Expected) {
    var condition = data.Expected[key]

    if ('AttributeValueList' in condition && 'Value' in condition)
      return 'One or more parameter values were invalid: ' +
        'Value and AttributeValueList cannot be used together for Attribute: ' + key

    if ('ComparisonOperator' in condition) {
      if ('Exists' in condition)
        return 'One or more parameter values were invalid: ' +
          'Exists and ComparisonOperator cannot be used together for Attribute: ' + key

      if (condition.ComparisonOperator != 'NULL' && condition.ComparisonOperator != 'NOT_NULL' &&
          !('AttributeValueList' in condition) && !('Value' in condition))
        return 'One or more parameter values were invalid: ' +
          'Value or AttributeValueList must be used with ComparisonOperator: ' + condition.ComparisonOperator +
          ' for Attribute: ' + key

      var values = condition.AttributeValueList ?
        condition.AttributeValueList.length : condition.Value ? 1 : 0
      var validAttrCount = false

      switch (condition.ComparisonOperator) {
        case 'EQ':
        case 'NE':
        case 'LE':
        case 'LT':
        case 'GE':
        case 'GT':
        case 'CONTAINS':
        case 'NOT_CONTAINS':
        case 'BEGINS_WITH':
          if (values === 1) validAttrCount = true
          break
        case 'NOT_NULL':
        case 'NULL':
          if (values === 0) validAttrCount = true
          break
        case 'IN':
          if (values > 0) validAttrCount = true
          break
        case 'BETWEEN':
          if (values === 2) validAttrCount = true
          break
      }
      if (!validAttrCount)
        return 'One or more parameter values were invalid: ' +
          'Invalid number of argument(s) for the ' + condition.ComparisonOperator + ' ComparisonOperator'

      if (condition.AttributeValueList && condition.AttributeValueList.length) {
        var type = Object.keys(condition.AttributeValueList[0])[0]
        if (condition.AttributeValueList.some(function(attr) { return Object.keys(attr)[0] != type })) {
          return 'One or more parameter values were invalid: AttributeValues inside AttributeValueList must be of same type'
        }
        if (condition.ComparisonOperator == 'BETWEEN' && db.compare('GT', condition.AttributeValueList[0], condition.AttributeValueList[1])) {
          return 'The BETWEEN condition was provided a range where the lower bound is greater than the upper bound'
        }
      }
    } else if ('AttributeValueList' in condition) {
      return 'One or more parameter values were invalid: ' +
        'AttributeValueList can only be used with a ComparisonOperator for Attribute: ' + key
    } else {
      var exists = condition.Exists == null || condition.Exists
      if (exists && condition.Value == null)
        return 'One or more parameter values were invalid: ' +
          'Value must be provided when Exists is ' +
          (condition.Exists == null ? 'null' : condition.Exists) +
          ' for Attribute: ' + key
      else if (!exists && condition.Value != null)
        return 'One or more parameter values were invalid: ' +
          'Value cannot be used when Exists is false for Attribute: ' + key
      if (condition.Value != null) {
        var msg = validateAttributeValue(condition.Value)
        if (msg) return msg
      }
    }
  }
}

function validateConditions(conditions) {
  var lengths = {
    NULL: 0,
    NOT_NULL: 0,
    EQ: 1,
    NE: 1,
    LE: 1,
    LT: 1,
    GE: 1,
    GT: 1,
    CONTAINS: 1,
    NOT_CONTAINS: 1,
    BEGINS_WITH: 1,
    IN: [1],
    BETWEEN: 2,
  }
  var types = {
    EQ: ['S', 'N', 'B', 'SS', 'NS', 'BS'],
    NE: ['S', 'N', 'B', 'SS', 'NS', 'BS'],
    LE: ['S', 'N', 'B'],
    LT: ['S', 'N', 'B'],
    GE: ['S', 'N', 'B'],
    GT: ['S', 'N', 'B'],
    CONTAINS: ['S', 'N', 'B'],
    NOT_CONTAINS: ['S', 'N', 'B'],
    BEGINS_WITH: ['S', 'B'],
    IN: ['S', 'N', 'B'],
    BETWEEN: ['S', 'N', 'B'],
  }
  for (var key in conditions) {
    var comparisonOperator = conditions[key].ComparisonOperator
    var attrValList = conditions[key].AttributeValueList || []
    for (var i = 0; i < attrValList.length; i++) {
      var msg = validateAttributeValue(attrValList[i])
      if (msg) return msg
    }

    if ((typeof lengths[comparisonOperator] == 'number' && attrValList.length != lengths[comparisonOperator]) ||
        (attrValList.length < lengths[comparisonOperator][0] || attrValList.length > lengths[comparisonOperator][1]))
      return 'One or more parameter values were invalid: Invalid number of argument(s) for the ' +
        comparisonOperator + ' ComparisonOperator'

    if (attrValList.length) {
      var type = Object.keys(attrValList[0])[0]
      if (attrValList.some(function(attr) { return Object.keys(attr)[0] != type })) {
        return 'One or more parameter values were invalid: AttributeValues inside AttributeValueList must be of same type'
      }
    }

    if (types[comparisonOperator]) {
      for (i = 0; i < attrValList.length; i++) {
        if (!~types[comparisonOperator].indexOf(Object.keys(attrValList[i])[0]))
          return 'One or more parameter values were invalid: ComparisonOperator ' + comparisonOperator +
            ' is not valid for ' + Object.keys(attrValList[i])[0] + ' AttributeValue type'
      }
    }

    if (comparisonOperator == 'BETWEEN' && db.compare('GT', attrValList[0], attrValList[1])) {
      return 'The BETWEEN condition was provided a range where the lower bound is greater than the upper bound'
    }
  }
}

function validateExpressionParams(data, expressions, nonExpressions) {
  var exprParams = expressions.filter(function(expr) { return data[expr] != null })

  if (exprParams.length) {
    // Special case for KeyConditions and KeyConditionExpression
    if (data.KeyConditions != null && data.KeyConditionExpression == null) {
      nonExpressions.splice(nonExpressions.indexOf('KeyConditions'), 1)
    }
    var nonExprParams = nonExpressions.filter(function(expr) { return data[expr] != null })
    if (nonExprParams.length) {
      return 'Can not use both expression and non-expression parameters in the same request: ' +
        'Non-expression parameters: {' + nonExprParams.join(', ') + '} ' +
        'Expression parameters: {' + exprParams.join(', ') + '}'
    }
  }

  if (data.ExpressionAttributeNames != null && !exprParams.length) {
    return 'ExpressionAttributeNames can only be specified when using expressions'
  }

  var valExprs = expressions.filter(function(expr) { return expr != 'ProjectionExpression' })
  if (valExprs.length && data.ExpressionAttributeValues != null &&
      valExprs.every(function(expr) { return data[expr] == null })) {
    return 'ExpressionAttributeValues can only be specified when using expressions: ' +
      valExprs.join(' and ') + ' ' + (valExprs.length > 1 ? 'are' : 'is') + ' null'
  }
}

function validateExpressions(data) {
  var key, msg, result, context = {
    attrNames: data.ExpressionAttributeNames,
    attrVals: data.ExpressionAttributeValues,
    unusedAttrNames: {},
    unusedAttrVals: {},
  }

  if (data.ExpressionAttributeNames != null) {
    if (!Object.keys(data.ExpressionAttributeNames).length)
      return 'ExpressionAttributeNames must not be empty'
    for (key in data.ExpressionAttributeNames) {
      if (!/^#[0-9a-zA-Z_]+$/.test(key)) {
        return 'ExpressionAttributeNames contains invalid key: Syntax error; key: "' + key + '"'
      }
      context.unusedAttrNames[key] = true
    }
  }

  if (data.ExpressionAttributeValues != null) {
    if (!Object.keys(data.ExpressionAttributeValues).length)
      return 'ExpressionAttributeValues must not be empty'
    for (key in data.ExpressionAttributeValues) {
      if (!/^:[0-9a-zA-Z_]+$/.test(key)) {
        return 'ExpressionAttributeValues contains invalid key: Syntax error; key: "' + key + '"'
      }
      context.unusedAttrVals[key] = true
    }
    for (key in data.ExpressionAttributeValues) {
      msg = validateAttributeValue(data.ExpressionAttributeValues[key])
      if (msg) {
        msg = 'ExpressionAttributeValues contains invalid value: ' + msg + ' for key ' + key
        return msg
      }
    }
  }

  if (data.UpdateExpression != null) {
    result = parse(data.UpdateExpression, updateParser, context)
    if (typeof result == 'string') {
      return 'Invalid UpdateExpression: ' + result
    }
    data._updates = result
  }

  if (data.ConditionExpression != null) {
    result = parse(data.ConditionExpression, conditionParser, context)
    if (typeof result == 'string') {
      return 'Invalid ConditionExpression: ' + result
    }
    data._condition = result
  }

  if (data.KeyConditionExpression != null) {
    context.isKeyCondition = true
    result = parse(data.KeyConditionExpression, conditionParser, context)
    if (typeof result == 'string') {
      return 'Invalid KeyConditionExpression: ' + result
    }
    data._keyCondition = result
  }

  if (data.FilterExpression != null) {
    result = parse(data.FilterExpression, conditionParser, context)
    if (typeof result == 'string') {
      return 'Invalid FilterExpression: ' + result
    }
    data._filter = result
  }

  if (data.ProjectionExpression != null) {
    result = parse(data.ProjectionExpression, projectionParser, context)
    if (typeof result == 'string') {
      return 'Invalid ProjectionExpression: ' + result
    }
    data._projection = result
  }

  if (Object.keys(context.unusedAttrNames).length) {
    return 'Value provided in ExpressionAttributeNames unused in expressions: ' +
      'keys: {' + Object.keys(context.unusedAttrNames).join(', ') + '}'
  }

  if (Object.keys(context.unusedAttrVals).length) {
    return 'Value provided in ExpressionAttributeValues unused in expressions: ' +
      'keys: {' + Object.keys(context.unusedAttrVals).join(', ') + '}'
  }
}

function parse(str, parser, context) {
  if (str == '') return 'The expression can not be empty;'
  context.isReserved = isReserved
  context.compare = db.compare
  try {
    return parser.parse(str, {context: context})
  } catch (e) {
    return e.name == 'SyntaxError' ? 'Syntax error; ' + e.message : e.message
  }
}

function convertKeyCondition(expression) {
  var keyConds = Object.create(null)
  return checkExpr(expression, keyConds) || keyConds
}

function checkExpr(expr, keyConds) {
  if (!expr || !expr.type) return
  if (~['or', 'not', 'in', '<>'].indexOf(expr.type)) {
    return 'Invalid operator used in KeyConditionExpression: ' + expr.type.toUpperCase()
  }
  if (expr.type == 'function' && ~['attribute_exists', 'attribute_not_exists', 'attribute_type', 'contains'].indexOf(expr.name)) {
    return 'Invalid operator used in KeyConditionExpression: ' + expr.name
  }
  if (expr.type == 'function' && expr.name == 'size') {
    return 'KeyConditionExpressions cannot contain nested operations'
  }
  if (expr.type == 'between' && !Array.isArray(expr.args[0])) {
    return 'Invalid condition in KeyConditionExpression: ' + expr.type.toUpperCase() + ' operator must have the key attribute as its first operand'
  }
  if (expr.type == 'function' && expr.name == 'begins_with' && !Array.isArray(expr.args[0])) {
    return 'Invalid condition in KeyConditionExpression: ' + expr.name + ' operator must have the key attribute as its first operand'
  }
  if (expr.args) {
    var attrName = '', attrIx = 0
    for (var i = 0; i < expr.args.length; i++) {
      if (Array.isArray(expr.args[i])) {
        if (attrName) {
          return 'Invalid condition in KeyConditionExpression: Multiple attribute names used in one condition'
        }
        if (expr.args[i].length > 1) {
          return 'KeyConditionExpressions cannot have conditions on nested attributes'
        }
        attrName = expr.args[i][0]
        attrIx = i
      } else if (expr.args[i].type) {
        var result = checkExpr(expr.args[i], keyConds)
        if (result) return result
      }
    }
    if (expr.type != 'and') {
      if (!attrName) {
        return 'Invalid condition in KeyConditionExpression: No key attribute specified'
      }
      if (keyConds[attrName]) {
        return 'KeyConditionExpressions must only contain one condition per key'
      }
      if (attrIx != 0) {
        expr.type = {
          '=': '=',
          '<': '>',
          '<=': '>=',
          '>': '<',
          '>=': '<=',
        }[expr.type]
        expr.args[1] = expr.args[0]
      }
      keyConds[attrName] = {
        ComparisonOperator: {
          '=': 'EQ',
          '<': 'LT',
          '<=': 'LE',
          '>': 'GT',
          '>=': 'GE',
          'between': 'BETWEEN',
          'function': 'BEGINS_WITH',
        }[expr.type],
        AttributeValueList: expr.args.slice(1),
      }
    }
  }
}

// http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html
var RESERVED_WORDS = {
  ABORT: true,
  ABSOLUTE: true,
  ACTION: true,
  ADD: true,
  AFTER: true,
  AGENT: true,
  AGGREGATE: true,
  ALL: true,
  ALLOCATE: true,
  ALTER: true,
  ANALYZE: true,
  AND: true,
  ANY: true,
  ARCHIVE: true,
  ARE: true,
  ARRAY: true,
  AS: true,
  ASC: true,
  ASCII: true,
  ASENSITIVE: true,
  ASSERTION: true,
  ASYMMETRIC: true,
  AT: true,
  ATOMIC: true,
  ATTACH: true,
  ATTRIBUTE: true,
  AUTH: true,
  AUTHORIZATION: true,
  AUTHORIZE: true,
  AUTO: true,
  AVG: true,
  BACK: true,
  BACKUP: true,
  BASE: true,
  BATCH: true,
  BEFORE: true,
  BEGIN: true,
  BETWEEN: true,
  BIGINT: true,
  BINARY: true,
  BIT: true,
  BLOB: true,
  BLOCK: true,
  BOOLEAN: true,
  BOTH: true,
  BREADTH: true,
  BUCKET: true,
  BULK: true,
  BY: true,
  BYTE: true,
  CALL: true,
  CALLED: true,
  CALLING: true,
  CAPACITY: true,
  CASCADE: true,
  CASCADED: true,
  CASE: true,
  CAST: true,
  CATALOG: true,
  CHAR: true,
  CHARACTER: true,
  CHECK: true,
  CLASS: true,
  CLOB: true,
  CLOSE: true,
  CLUSTER: true,
  CLUSTERED: true,
  CLUSTERING: true,
  CLUSTERS: true,
  COALESCE: true,
  COLLATE: true,
  COLLATION: true,
  COLLECTION: true,
  COLUMN: true,
  COLUMNS: true,
  COMBINE: true,
  COMMENT: true,
  COMMIT: true,
  COMPACT: true,
  COMPILE: true,
  COMPRESS: true,
  CONDITION: true,
  CONFLICT: true,
  CONNECT: true,
  CONNECTION: true,
  CONSISTENCY: true,
  CONSISTENT: true,
  CONSTRAINT: true,
  CONSTRAINTS: true,
  CONSTRUCTOR: true,
  CONSUMED: true,
  CONTINUE: true,
  CONVERT: true,
  COPY: true,
  CORRESPONDING: true,
  COUNT: true,
  COUNTER: true,
  CREATE: true,
  CROSS: true,
  CUBE: true,
  CURRENT: true,
  CURSOR: true,
  CYCLE: true,
  DATA: true,
  DATABASE: true,
  DATE: true,
  DATETIME: true,
  DAY: true,
  DEALLOCATE: true,
  DEC: true,
  DECIMAL: true,
  DECLARE: true,
  DEFAULT: true,
  DEFERRABLE: true,
  DEFERRED: true,
  DEFINE: true,
  DEFINED: true,
  DEFINITION: true,
  DELETE: true,
  DELIMITED: true,
  DEPTH: true,
  DEREF: true,
  DESC: true,
  DESCRIBE: true,
  DESCRIPTOR: true,
  DETACH: true,
  DETERMINISTIC: true,
  DIAGNOSTICS: true,
  DIRECTORIES: true,
  DISABLE: true,
  DISCONNECT: true,
  DISTINCT: true,
  DISTRIBUTE: true,
  DO: true,
  DOMAIN: true,
  DOUBLE: true,
  DROP: true,
  DUMP: true,
  DURATION: true,
  DYNAMIC: true,
  EACH: true,
  ELEMENT: true,
  ELSE: true,
  ELSEIF: true,
  EMPTY: true,
  ENABLE: true,
  END: true,
  EQUAL: true,
  EQUALS: true,
  ERROR: true,
  ESCAPE: true,
  ESCAPED: true,
  EVAL: true,
  EVALUATE: true,
  EXCEEDED: true,
  EXCEPT: true,
  EXCEPTION: true,
  EXCEPTIONS: true,
  EXCLUSIVE: true,
  EXEC: true,
  EXECUTE: true,
  EXISTS: true,
  EXIT: true,
  EXPLAIN: true,
  EXPLODE: true,
  EXPORT: true,
  EXPRESSION: true,
  EXTENDED: true,
  EXTERNAL: true,
  EXTRACT: true,
  FAIL: true,
  FALSE: true,
  FAMILY: true,
  FETCH: true,
  FIELDS: true,
  FILE: true,
  FILTER: true,
  FILTERING: true,
  FINAL: true,
  FINISH: true,
  FIRST: true,
  FIXED: true,
  FLATTERN: true,
  FLOAT: true,
  FOR: true,
  FORCE: true,
  FOREIGN: true,
  FORMAT: true,
  FORWARD: true,
  FOUND: true,
  FREE: true,
  FROM: true,
  FULL: true,
  FUNCTION: true,
  FUNCTIONS: true,
  GENERAL: true,
  GENERATE: true,
  GET: true,
  GLOB: true,
  GLOBAL: true,
  GO: true,
  GOTO: true,
  GRANT: true,
  GREATER: true,
  GROUP: true,
  GROUPING: true,
  HANDLER: true,
  HASH: true,
  HAVE: true,
  HAVING: true,
  HEAP: true,
  HIDDEN: true,
  HOLD: true,
  HOUR: true,
  IDENTIFIED: true,
  IDENTITY: true,
  IF: true,
  IGNORE: true,
  IMMEDIATE: true,
  IMPORT: true,
  IN: true,
  INCLUDING: true,
  INCLUSIVE: true,
  INCREMENT: true,
  INCREMENTAL: true,
  INDEX: true,
  INDEXED: true,
  INDEXES: true,
  INDICATOR: true,
  INFINITE: true,
  INITIALLY: true,
  INLINE: true,
  INNER: true,
  INNTER: true,
  INOUT: true,
  INPUT: true,
  INSENSITIVE: true,
  INSERT: true,
  INSTEAD: true,
  INT: true,
  INTEGER: true,
  INTERSECT: true,
  INTERVAL: true,
  INTO: true,
  INVALIDATE: true,
  IS: true,
  ISOLATION: true,
  ITEM: true,
  ITEMS: true,
  ITERATE: true,
  JOIN: true,
  KEY: true,
  KEYS: true,
  LAG: true,
  LANGUAGE: true,
  LARGE: true,
  LAST: true,
  LATERAL: true,
  LEAD: true,
  LEADING: true,
  LEAVE: true,
  LEFT: true,
  LENGTH: true,
  LESS: true,
  LEVEL: true,
  LIKE: true,
  LIMIT: true,
  LIMITED: true,
  LINES: true,
  LIST: true,
  LOAD: true,
  LOCAL: true,
  LOCALTIME: true,
  LOCALTIMESTAMP: true,
  LOCATION: true,
  LOCATOR: true,
  LOCK: true,
  LOCKS: true,
  LOG: true,
  LOGED: true,
  LONG: true,
  LOOP: true,
  LOWER: true,
  MAP: true,
  MATCH: true,
  MATERIALIZED: true,
  MAX: true,
  MAXLEN: true,
  MEMBER: true,
  MERGE: true,
  METHOD: true,
  METRICS: true,
  MIN: true,
  MINUS: true,
  MINUTE: true,
  MISSING: true,
  MOD: true,
  MODE: true,
  MODIFIES: true,
  MODIFY: true,
  MODULE: true,
  MONTH: true,
  MULTI: true,
  MULTISET: true,
  NAME: true,
  NAMES: true,
  NATIONAL: true,
  NATURAL: true,
  NCHAR: true,
  NCLOB: true,
  NEW: true,
  NEXT: true,
  NO: true,
  NONE: true,
  NOT: true,
  NULL: true,
  NULLIF: true,
  NUMBER: true,
  NUMERIC: true,
  OBJECT: true,
  OF: true,
  OFFLINE: true,
  OFFSET: true,
  OLD: true,
  ON: true,
  ONLINE: true,
  ONLY: true,
  OPAQUE: true,
  OPEN: true,
  OPERATOR: true,
  OPTION: true,
  OR: true,
  ORDER: true,
  ORDINALITY: true,
  OTHER: true,
  OTHERS: true,
  OUT: true,
  OUTER: true,
  OUTPUT: true,
  OVER: true,
  OVERLAPS: true,
  OVERRIDE: true,
  OWNER: true,
  PAD: true,
  PARALLEL: true,
  PARAMETER: true,
  PARAMETERS: true,
  PARTIAL: true,
  PARTITION: true,
  PARTITIONED: true,
  PARTITIONS: true,
  PATH: true,
  PERCENT: true,
  PERCENTILE: true,
  PERMISSION: true,
  PERMISSIONS: true,
  PIPE: true,
  PIPELINED: true,
  PLAN: true,
  POOL: true,
  POSITION: true,
  PRECISION: true,
  PREPARE: true,
  PRESERVE: true,
  PRIMARY: true,
  PRIOR: true,
  PRIVATE: true,
  PRIVILEGES: true,
  PROCEDURE: true,
  PROCESSED: true,
  PROJECT: true,
  PROJECTION: true,
  PROPERTY: true,
  PROVISIONING: true,
  PUBLIC: true,
  PUT: true,
  QUERY: true,
  QUIT: true,
  QUORUM: true,
  RAISE: true,
  RANDOM: true,
  RANGE: true,
  RANK: true,
  RAW: true,
  READ: true,
  READS: true,
  REAL: true,
  REBUILD: true,
  RECORD: true,
  RECURSIVE: true,
  REDUCE: true,
  REF: true,
  REFERENCE: true,
  REFERENCES: true,
  REFERENCING: true,
  REGEXP: true,
  REGION: true,
  REINDEX: true,
  RELATIVE: true,
  RELEASE: true,
  REMAINDER: true,
  RENAME: true,
  REPEAT: true,
  REPLACE: true,
  REQUEST: true,
  RESET: true,
  RESIGNAL: true,
  RESOURCE: true,
  RESPONSE: true,
  RESTORE: true,
  RESTRICT: true,
  RESULT: true,
  RETURN: true,
  RETURNING: true,
  RETURNS: true,
  REVERSE: true,
  REVOKE: true,
  RIGHT: true,
  ROLE: true,
  ROLES: true,
  ROLLBACK: true,
  ROLLUP: true,
  ROUTINE: true,
  ROW: true,
  ROWS: true,
  RULE: true,
  RULES: true,
  SAMPLE: true,
  SATISFIES: true,
  SAVE: true,
  SAVEPOINT: true,
  SCAN: true,
  SCHEMA: true,
  SCOPE: true,
  SCROLL: true,
  SEARCH: true,
  SECOND: true,
  SECTION: true,
  SEGMENT: true,
  SEGMENTS: true,
  SELECT: true,
  SELF: true,
  SEMI: true,
  SENSITIVE: true,
  SEPARATE: true,
  SEQUENCE: true,
  SERIALIZABLE: true,
  SESSION: true,
  SET: true,
  SETS: true,
  SHARD: true,
  SHARE: true,
  SHARED: true,
  SHORT: true,
  SHOW: true,
  SIGNAL: true,
  SIMILAR: true,
  SIZE: true,
  SKEWED: true,
  SMALLINT: true,
  SNAPSHOT: true,
  SOME: true,
  SOURCE: true,
  SPACE: true,
  SPACES: true,
  SPARSE: true,
  SPECIFIC: true,
  SPECIFICTYPE: true,
  SPLIT: true,
  SQL: true,
  SQLCODE: true,
  SQLERROR: true,
  SQLEXCEPTION: true,
  SQLSTATE: true,
  SQLWARNING: true,
  START: true,
  STATE: true,
  STATIC: true,
  STATUS: true,
  STORAGE: true,
  STORE: true,
  STORED: true,
  STREAM: true,
  STRING: true,
  STRUCT: true,
  STYLE: true,
  SUB: true,
  SUBMULTISET: true,
  SUBPARTITION: true,
  SUBSTRING: true,
  SUBTYPE: true,
  SUM: true,
  SUPER: true,
  SYMMETRIC: true,
  SYNONYM: true,
  SYSTEM: true,
  TABLE: true,
  TABLESAMPLE: true,
  TEMP: true,
  TEMPORARY: true,
  TERMINATED: true,
  TEXT: true,
  THAN: true,
  THEN: true,
  THROUGHPUT: true,
  TIME: true,
  TIMESTAMP: true,
  TIMEZONE: true,
  TINYINT: true,
  TO: true,
  TOKEN: true,
  TOTAL: true,
  TOUCH: true,
  TRAILING: true,
  TRANSACTION: true,
  TRANSFORM: true,
  TRANSLATE: true,
  TRANSLATION: true,
  TREAT: true,
  TRIGGER: true,
  TRIM: true,
  TRUE: true,
  TRUNCATE: true,
  TTL: true,
  TUPLE: true,
  TYPE: true,
  UNDER: true,
  UNDO: true,
  UNION: true,
  UNIQUE: true,
  UNIT: true,
  UNKNOWN: true,
  UNLOGGED: true,
  UNNEST: true,
  UNPROCESSED: true,
  UNSIGNED: true,
  UNTIL: true,
  UPDATE: true,
  UPPER: true,
  URL: true,
  USAGE: true,
  USE: true,
  USER: true,
  USERS: true,
  USING: true,
  UUID: true,
  VACUUM: true,
  VALUE: true,
  VALUED: true,
  VALUES: true,
  VARCHAR: true,
  VARIABLE: true,
  VARIANCE: true,
  VARINT: true,
  VARYING: true,
  VIEW: true,
  VIEWS: true,
  VIRTUAL: true,
  VOID: true,
  WAIT: true,
  WHEN: true,
  WHENEVER: true,
  WHERE: true,
  WHILE: true,
  WINDOW: true,
  WITH: true,
  WITHIN: true,
  WITHOUT: true,
  WORK: true,
  WRAPPED: true,
  WRITE: true,
  YEAR: true,
  ZONE: true,
}

function isReserved(name) {
  return RESERVED_WORDS[name.toUpperCase()] != null
}
