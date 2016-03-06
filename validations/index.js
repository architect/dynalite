var Big = require('big.js')

exports.checkTypes = checkTypes
exports.checkValidations = checkValidations
exports.toLowerFirst = toLowerFirst
exports.validateAttributeValue = validateAttributeValue
exports.validateAttributeConditions = validateAttributeConditions

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
            throw typeError('\'' + val + '\' can not be converted to an Boolean')
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

function checkValidations(data, validations, custom) {
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
    msg = custom(data)
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
  var types = Object.keys(value), msg
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

    if ((type == 'NS' || type == 'BS') && !value[type].length)
      return 'One or more parameter values were invalid: An AttributeValue may not contain an empty set.'

    if (type == 'SS' && value[type].some(function(x) { return !x })) // eslint-disable-line no-loop-func
      return 'One or more parameter values were invalid: An string set may not have a empty string as a member'

    if (type == 'BS' && value[type].some(function(x) { return !x })) // eslint-disable-line no-loop-func
      return 'One or more parameter values were invalid: Binary sets may not contain null or empty values'

    if (type == 'NS' && value[type].some(function(x) { return !x })) // eslint-disable-line no-loop-func
      return 'The parameter cannot be converted to a numeric value'

    if (type == 'NS') {
      for (var i = 0; i < value[type].length; i++) {
        msg = checkNum(i, value[type])
        if (msg) return msg
      }
    }

    if (type == 'SS' && hasDuplicates(value[type]))
      return 'One or more parameter values were invalid: Input collection ' + valueStr(value[type]) + ' contains duplicates.'

    if (type == 'NS' && hasDuplicates(value[type]))
      return 'Input collection contains duplicates'

    if (type == 'BS' && hasDuplicates(value[type]))
      return 'One or more parameter values were invalid: Input collection ' + valueStr(value[type]) + 'of type BS contains duplicates.'

    if (type == 'M') {
      for (var attr in value[type]) {
        msg = validateAttributeValue(value[type][attr])
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

function hasDuplicates(array) {
  var setObj = {}
  return array.some(function(val) {
    if (setObj[val]) return true
    setObj[val] = true
    return false
  })
}

function validateAttributeConditions(data) {
  if (data.Expected) {
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
}
