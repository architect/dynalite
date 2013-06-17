var http = require('http'),
    crypto = require('crypto'),
    crc32 = require('buffer-crc32'),
    levelup = require('levelup'),
    MemDown = require('memdown'),
    sublevel = require('level-sublevel'),
    levelUpdate = require('level-update')

var MAX_REQUEST_BYTES = 1024 * 1024

var db = sublevel(levelup('./mydb', {db: function(location){ return new MemDown(location) }})),
    tableDb = db.sublevel('table', {valueEncoding: 'json'})

levelUpdate(tableDb, function(newValue, oldValue) {
  if (oldValue) throw new Error('Already exists')
})

function rand52CharId(cb) {
  // 39 bytes turns into 52 base64 characters
  crypto.randomBytes(39, function(err, bytes) {
    if (err) return cb(err)
    // Need to replace + and / so just choose 0, obvs won't be truly random, whatevs
    cb(null, bytes.toString('base64').toUpperCase().replace(/\+|\//g, '0'))
  })
}

function sendData(req, res, data, statusCode) {
  var body = JSON.stringify(data)
  req.removeAllListeners()
  res.statusCode = statusCode || 200
  res.setHeader('x-amz-crc32', crc32.unsigned(body))
  res.setHeader('Content-Type', res.contentType)
  res.setHeader('Content-Length', Buffer.byteLength(body, 'utf8'))
  // AWS doesn't send a 'Connection' header but seems to use keep-alive behaviour
  //res.setHeader('Connection', '')
  //res.shouldKeepAlive = false
  res.end(body)
}

var dynalite = module.exports = http.createServer(function(req, res) {
  var body
  req.on('error', function(err) { throw err })
  req.on('data', function(data) {
    var newLength = data.length + (body ? body.length : 0)
    if (newLength > MAX_REQUEST_BYTES) {
      req.removeAllListeners()
      res.statusCode = 413
      res.setHeader('Transfer-Encoding', 'chunked')
      return res.end()
    }
    body = body ? Buffer.concat([body, data], newLength) : data
  })
  req.on('end', function() {

    body = body ? body.toString() : ''

    // All responses after this point have a RequestId
    rand52CharId(function(err, id) {
      if (err) throw err

      res.setHeader('x-amzn-RequestId', id)

      var contentType = req.headers['content-type']

      if (req.method != 'POST' ||
          (body && contentType != 'application/json' && contentType != 'application/x-amz-json-1.0')) {
        req.removeAllListeners()
        res.statusCode = 404
        res.setHeader('x-amz-crc32', 3552371480)
        res.setHeader('Content-Length', 29)
        return res.end('<UnknownOperationException/>\n')
      }

      // TODO: Perhaps don't do this
      res.contentType = contentType != 'application/x-amz-json-1.0' ? 'application/json' : contentType

      // THEN check body, see if the JSON parses:

      var data
      if (body) {
        try {
          data = JSON.parse(body)
        } catch (e) {
          return sendData(req, res, {__type: 'com.amazon.coral.service#SerializationException'}, 400)
        }
      }

      // DynamoDB doesn't seem to care about the HTTP path, so no checking needed for that

      var validApis = ['DynamoDB_20111205', 'DynamoDB_20120810'],
          validOperations = ['BatchGetItem', 'BatchWriteItem', 'CreateTable', 'DeleteItem', 'DeleteTable',
            'DescribeTable', 'GetItem', 'ListTables', 'PutItem', 'Query', 'Scan', 'UpdateItem', 'UpdateTable']

      var target = (req.headers['x-amz-target'] || '').split('.')

      if (target.length != 2 || !~validApis.indexOf(target[0]) || !~validOperations.indexOf(target[1]))
        return sendData(req, res, {__type: 'com.amazon.coral.service#UnknownOperationException'}, 400)

      var auth = req.headers.authorization

      if (!auth || auth.trim().slice(0, 5) != 'AWS4-')
        return sendData(req, res, {
          __type: 'com.amazon.coral.service#MissingAuthenticationTokenException',
          message: 'Request is missing Authentication Token',
        }, 400)

      var authParams = auth.split(' ').slice(1).join('').split(',').reduce(function(obj, x) {
            var keyVal = x.trim().split('=')
            obj[keyVal[0]] = keyVal[1]
            return obj
          }, {}),
          date = req.headers['x-amz-date'] || req.headers.date

      var headers = ['Credential', 'Signature', 'SignedHeaders']
      var msg = ''
      // TODO: Go through key-vals first
      // "'Credential' not a valid key=value pair (missing equal-sign) in Authorization header: 'AWS4-HMAC-SHA256 \
      // Signature=b,    Credential,    SignedHeaders'."
      for (var i in headers) {
        if (!authParams[headers[i]])
          // TODO: SignedHeaders *is* allowed to be an empty string at this point
          msg += 'Authorization header requires \'' + headers[i] + '\' parameter. '
      }
      if (!date)
        msg += 'Authorization header requires existence of either a \'X-Amz-Date\' or a \'Date\' header. '
      if (msg) {
        return sendData(req, res, {
          __type: 'com.amazon.coral.service#IncompleteSignatureException',
          message: msg + 'Authorization=' + auth,
        }, 400)
      }
      // THEN check Date format and expiration
      // {"__type":"com.amazon.coral.service#IncompleteSignatureException","message":"Date must be in ISO-8601 'basic format'. \
      // Got '201'. See http://en.wikipedia.org/wiki/ISO_8601"}
      // {"__type":"com.amazon.coral.service#InvalidSignatureException","message":"Signature expired: 20130301T000000Z is \
      // now earlier than 20130609T094515Z (20130609T100015Z - 15 min.)"}
      // THEN check Host is in SignedHeaders (not case sensitive)
      // {"__type":"com.amazon.coral.service#InvalidSignatureException","message":"'Host' must be a 'SignedHeader' in the AWS Authorization."}
      // THEN check Algorithm
      // {"__type":"com.amazon.coral.service#IncompleteSignatureException","message":"Unsupported AWS 'algorithm': \
      // 'AWS4-HMAC-SHA25' (only AWS4-HMAC-SHA256 for now). "}
      // THEN check Credential (trailing slashes are ignored)
      // {"__type":"com.amazon.coral.service#IncompleteSignatureException","message":"Credential must have exactly 5 \
      // slash-delimited elements, e.g. keyid/date/region/service/term, got 'a/b/c/d'"}
      // THEN check Credential pieces, all must match exact case, keyid checking throws different error below
      // {"__type":"com.amazon.coral.service#InvalidSignatureException","message":\
      // "Credential should be scoped to a valid region, not 'c'. \
      // Credential should be scoped to correct service: 'dynamodb'. \
      // Credential should be scoped with a valid terminator: 'aws4_request', not 'e'. \
      // Date in Credential scope does not match YYYYMMDD from ISO-8601 version of date from HTTP: 'b' != '20130609', from '20130609T095204Z'."}
      // THEN check keyid
      // {"__type":"com.amazon.coral.service#UnrecognizedClientException","message":"The security token included in the request is invalid."}
      // THEN check signature (requires body - will need async)
      // {"__type":"com.amazon.coral.service#InvalidSignatureException","message":"The request signature we calculated \
      // does not match the signature you provided. Check your AWS Secret Access Key and signing method. \
      // Consult the service documentation for details.\n\nThe Canonical String for this request should have \
      // been\n'POST\n/\n\nhost:dynamodb.ap-southeast-2.amazonaws.com\n\nhost\ne3b0c44298fc1c149afbf4c8996fb92427ae41e46\
      // 49b934ca495991b7852b855'\n\nThe String-to-Sign should have been\n'AWS4-HMAC-SHA256\n20130609T\
      // 100759Z\n20130609/ap-southeast-2/dynamodb/aws4_request\n7b8b82a032afd6014771e3375813fc995dd167b7b3a133a0b86e5925cb000ec5'\n"}
      // THEN check X-Amz-Security-Token if it exists
      // {"__type":"com.amazon.coral.service#UnrecognizedClientException","message":"The security token included in the request is invalid"}

      // THEN check types (note different capitalization for Message and poor grammar for a/an):

      // THEN validation checks (note different service):
      // {"__type":"com.amazon.coral.validate#ValidationException","message":"3 validation errors detected: \
      // Value \'2147483647\' at \'limit\' failed to satisfy constraint: \
      // Member must have value less than or equal to 100; \
      // Value \'89hls;;f;d\' at \'exclusiveStartTableName\' failed to satisfy constraint: \
      // Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; \
      // Value \'89hls;;f;d\' at \'exclusiveStartTableName\' failed to satisfy constraint: \
      // Member must have length less than or equal to 255"}

      // For some reason, the serialization checks seem to be a bit out of sync
      if (!body)
        return sendData(req, res, {__type: 'com.amazon.coral.service#SerializationException'}, 400)

      targets[toLowerFirst(target[1])](req, res, data)
    })
  })
})

var targets = {}

targets.listTables = function listTables(req, res, data) {
  var types = {
    ExclusiveStartTableName: 'String',
    Limit: 'Integer',
  }, validations = {
    Limit: {
      greaterThanOrEqual: 1,
      lessThanOrEqual: 100,
    },
    ExclusiveStartTableName: {
      regex: '[a-zA-Z0-9_.-]+',
      lengthGreaterThanOrEqual: 3,
      lengthLessThanOrEqual: 255,
    },
  }

  try {
    data = checkTypes(data, types)
    checkValidations(data, validations)
  } catch (e) {
    if (e.statusCode) return sendData(req, res, e.body, e.statusCode)
    throw e
  }

  // needs to be anything > ExclusiveStartTableName

  sendData(req, res, {TableNames: []})
}

targets.createTable = function createTable(req, res, data) {
  var types = {
    TableName: 'String',
    AttributeDefinitions: {
      type: 'List',
      children: {
        type: 'Structure',
        children: {
          AttributeName: 'String',
          AttributeType: 'String',
        }
      },
    },
    KeySchema: {
      type: 'List',
      children: {
        type: 'Structure',
        children: {
          AttributeName: 'String',
          KeyType: 'String',
        }
      },
    },
    LocalSecondaryIndexes: {
      type: 'List',
      children: {
        type: 'Structure',
        children: {
          IndexName: 'String',
          KeySchema: {
            type: 'List',
            children: {
              type: 'Structure',
              children: {
                AttributeName: 'String',
                KeyType: 'String',
              }
            }
          },
          Projection: {
            type: 'Structure',
            children: {
              NonKeyAttributes: {
                type: 'List',
                children: 'String'
              },
              ProjectionType: 'String'
            }
          }
        }
      },
    },
    ProvisionedThroughput: {
      type: 'Structure',
      children: {
        WriteCapacityUnits: 'Long',
        ReadCapacityUnits: 'Long',
      },
    },
  }, validations = {
    AttributeDefinitions: {
      notNull: true,
      children: {
        AttributeName: {
          notNull: true,
        },
        AttributeType: {
          notNull: true,
          enum: ['B', 'N', 'S']
        },
      },
    },
    TableName: {
      required: true,
      custom: validateTable,
      regex: '[a-zA-Z0-9_.-]+',
      lengthGreaterThanOrEqual: 3,
      lengthLessThanOrEqual: 255,
    },
    ProvisionedThroughput: {
      notNull: true,
      children: {
        WriteCapacityUnits: {
          notNull: true,
          greaterThanOrEqual: 1,
        },
        ReadCapacityUnits: {
          notNull: true,
          greaterThanOrEqual: 1,
        },
      },
    },
    KeySchema: {
      notNull: true,
      lengthGreaterThanOrEqual: 1,
      lengthLessThanOrEqual: 2,
      children: {
        KeyType: {
          notNull: true,
          enum: ['HASH', 'RANGE'],
        },
        AttributeName: {
          notNull: true,
        },
      },
    },
    LocalSecondaryIndexes: {
      children: {
        Projection: {
          notNull: true,
          children: {
            ProjectionType: {
              enum: ['ALL', 'INCLUDE', 'KEYS_ONLY']
            },
            NonKeyAttributes: {
              lengthGreaterThanOrEqual: 1,
            },
          }
        },
        IndexName: {
          notNull: true,
          regex: '[a-zA-Z0-9_.-]+',
          lengthGreaterThanOrEqual: 3,
          lengthLessThanOrEqual: 255,
        },
        KeySchema: {
          notNull: true,
          lengthGreaterThanOrEqual: 1,
          children: {
            AttributeName: {
              notNull: true,
            },
            KeyType: {
              notNull: true,
            },
          }
        },
      },
    },
  }, extra = [
    function checkCapacity(data) {
      if (data.ProvisionedThroughput.ReadCapacityUnits > 1000000000000)
        return 'Given value ' + data.ProvisionedThroughput.ReadCapacityUnits + ' for ReadCapacityUnits is out of bounds'
      if (data.ProvisionedThroughput.WriteCapacityUnits > 1000000000000)
        return 'Given value ' + data.ProvisionedThroughput.WriteCapacityUnits + ' for WriteCapacityUnits is out of bounds'
    },
    function checkKeysDefinition(data) {
      if (data.KeySchema.length != 2) return
      var keys = data.KeySchema.map(function(key) { return key.AttributeName })
      var defns = data.AttributeDefinitions.map(function(key) { return key.AttributeName })
      if (keys.some(function(key) { return !~defns.indexOf(key) }) ||
          // bizarre case - not sure what the general form of it is
          keys[0] == keys[1] && defns.length == 1)
        return 'Invalid KeySchema: Some index key attribute have no definition'
    },
    function checkKeyDefinition(data) {
      if (data.KeySchema.length != 1) return
      var defns = data.AttributeDefinitions.map(function(key) { return key.AttributeName }).reverse()
      var keys = data.KeySchema.map(function(key) { return key.AttributeName }).reverse()
      if (keys.some(function(key) { return !~defns.indexOf(key) }))
        return 'One or more parameter values were invalid: Some index key attributes are not defined in ' +
          'AttributeDefinitions. Keys: [' + keys.join(', ') + '], AttributeDefinitions: [' + defns.join(', ') + ']'
    },
    function checkKeyNames(data) {
      if (data.KeySchema.length != 2) return
      if (data.KeySchema[0].AttributeName == data.KeySchema[1].AttributeName)
        return 'Both the Hash Key and the Range Key element in the KeySchema have the same name'
    },
    function checkKeyHash(data) {
      if (data.KeySchema[0].KeyType != 'HASH')
        return 'Invalid KeySchema: The first KeySchemaElement is not a HASH key type'
    },
    function checkKeyRange(data) {
      if (data.KeySchema.length != 2) return
      if (data.KeySchema[1].KeyType != 'RANGE')
        return 'Invalid KeySchema: The second KeySchemaElement is not a RANGE key type'
    },
    function checkKeyRange(data) {
      if (!data.LocalSecondaryIndexes && data.KeySchema.length != data.AttributeDefinitions.length)
        return 'One or more parameter values were invalid: Number of attributes in KeySchema does not ' +
          'exactly match number of attributes defined in AttributeDefinitions'
    },
    function checkKeyRange(data) {
      if (data.LocalSecondaryIndexes && !data.LocalSecondaryIndexes.length)
        return 'One or more parameter values were invalid: List of indexes is empty'
    },
    function checkKeyRange(data) {
      if (data.LocalSecondaryIndexes && data.KeySchema.length != 2)
        return 'One or more parameter values were invalid: Table KeySchema does not have a range key'
    },
    function checkIndexDefinition(data) {
      if (!data.LocalSecondaryIndexes) return
      var keys, defns = data.AttributeDefinitions.map(function(key) { return key.AttributeName }).reverse()
      for (var i = 0; i < data.LocalSecondaryIndexes.length; i++) {
        keys = data.LocalSecondaryIndexes[i].KeySchema.map(function(key) { return key.AttributeName }).reverse()
        if (keys.some(function(key) { return !~defns.indexOf(key) }))
          return 'One or more parameter values were invalid: ' +
            'Some index key attributes are not defined in AttributeDefinitions. ' +
            'Keys: [' + keys.join(', ') + '], AttributeDefinitions: [' + defns.join(', ') + ']'
      }
    },
    function checkIndexKeyNames(data) {
      if (!data.LocalSecondaryIndexes) return
      for (var i = 0; i < data.LocalSecondaryIndexes.length; i++) {
        if (data.LocalSecondaryIndexes[i].KeySchema[1] &&
            data.LocalSecondaryIndexes[i].KeySchema[0].AttributeName ==
            data.LocalSecondaryIndexes[i].KeySchema[1].AttributeName)
          return 'Both the Hash Key and the Range Key element in the KeySchema have the same name'
      }
    },
    function checkIndexTypes(data) {
      if (!data.LocalSecondaryIndexes) return
      for (var i = 0; i < data.LocalSecondaryIndexes.length; i++) {
        if (data.LocalSecondaryIndexes[i].KeySchema[0].KeyType != 'HASH')
          return 'Invalid KeySchema: The first KeySchemaElement is not a HASH key type'
        if (data.LocalSecondaryIndexes[i].KeySchema[1] &&
            data.LocalSecondaryIndexes[i].KeySchema[1].KeyType != 'RANGE')
          return 'Invalid KeySchema: The second KeySchemaElement is not a RANGE key type'
      }
    },
    function checkIndexRange(data) {
      if (!data.LocalSecondaryIndexes) return
      for (var i = 0; i < data.LocalSecondaryIndexes.length; i++) {
        if (data.LocalSecondaryIndexes[i].KeySchema.length != 2)
          return 'One or more parameter values were invalid: Index KeySchema does not have a range key for index: ' +
            data.LocalSecondaryIndexes[i].IndexName
      }
    },
    function checkIndexHashSameAsTable(data) {
      if (!data.LocalSecondaryIndexes) return
      var tableHash = data.KeySchema[0].AttributeName
      for (var i = 0; i < data.LocalSecondaryIndexes.length; i++) {
        var indexHash = data.LocalSecondaryIndexes[i].KeySchema[0].AttributeName
        if (indexHash != tableHash)
          return 'One or more parameter values were invalid: ' +
            'Index KeySchema does not have the same leading hash key as table KeySchema for index: ' +
            data.LocalSecondaryIndexes[i].IndexName + '. index hash key: ' + indexHash +
            ', table hash key: ' + tableHash
      }
    },
    function checkProjectionType(data) {
      if (!data.LocalSecondaryIndexes) return
      for (var i = 0; i < data.LocalSecondaryIndexes.length; i++) {
        if (data.LocalSecondaryIndexes[i].Projection.ProjectionType == null)
          return 'One or more parameter values were invalid: Unknown ProjectionType: null'
      }
    },
    function checkProjectionForNonKeyAttributes(data) {
      if (!data.LocalSecondaryIndexes) return
      for (var i = 0; i < data.LocalSecondaryIndexes.length; i++) {
        var projectionType = data.LocalSecondaryIndexes[i].Projection.ProjectionType
        if (data.LocalSecondaryIndexes[i].Projection.NonKeyAttributes && projectionType != 'INCLUDE')
          return 'One or more parameter values were invalid: ' +
            'ProjectionType is ' + projectionType + ', but NonKeyAttributes is specified'
      }
    },
    function checkUniqueIndexNames(data) {
      if (!data.LocalSecondaryIndexes) return
      var indexNames = Object.create(null)
      for (var i = 0; i < data.LocalSecondaryIndexes.length; i++) {
        var indexName = data.LocalSecondaryIndexes[i].IndexName
        if (indexNames[indexName])
          return 'One or more parameter values were invalid: Duplicate index name: ' + indexName
        indexNames[indexName] = true
      }
    },
    function checkIndexCount(data) {
      if (data.LocalSecondaryIndexes && data.LocalSecondaryIndexes.length > 5)
        return 'One or more parameter values were invalid: Number of indexes exceeds per-table limit of 5'
    },
  ]

  try {
    data = checkTypes(data, types)
    checkValidations(data, validations, extra)
  } catch (e) {
    if (e.statusCode) return sendData(req, res, e.body, e.statusCode)
    throw e
  }

  data.CreationDateTime = Date.now() / 1000
  data.ItemCount = 0
  data.ProvisionedThroughput.NumberOfDecreasesToday = 0
  data.TableSizeBytes = 0
  data.TableStatus = 'CREATING'
  if (data.LocalSecondaryIndexes) {
    data.LocalSecondaryIndexes.forEach(function(index) {
      index.IndexSizeBytes = 0
      index.ItemCount = 0
    })
  }

  tableDb.put(data.TableName, data, function(err) {
    if (err) throw err
    sendData(req, res, {TableDescription: data})
  })
}

targets.describeTable = function describeTable(req, res, data) {
  var types = {
    TableName: 'String',
  }, validations = {
    TableName: {
      required: true,
      custom: validateTable,
      regex: '[a-zA-Z0-9_.-]+',
      lengthGreaterThanOrEqual: 3,
      lengthLessThanOrEqual: 255,
    },
  }

  try {
    data = checkTypes(data, types)
    checkValidations(data, validations)
  } catch (e) {
    if (e.statusCode) return sendData(req, res, e.body, e.statusCode)
    throw e
  }

  tableDb.get(data.TableName, function(err, table) {
    if (err) {
      if (err.name == 'NotFoundError')
        return sendData(req, res, {
          __type: 'com.amazonaws.dynamodb.v20120810#ResourceNotFoundException',
          message: 'Requested resource not found: Table: ' + data.TableName + ' not found',
        }, 400)
      throw err
    }
    sendData(req, res, {Table: table})
  })
}

targets.getItem = function getItem(req, res, data) {
  var types = {
    TableName: 'String',
    Key: {
      type: 'Map',
      children: {
        type: 'Structure',
        children: {
          S: 'String',
          B: 'Blob',
          N: 'String',
        }
      }
    },
    AttributesToGet: 'List',
    ConsistentRead: 'Boolean',
    ReturnConsumedCapacity: 'String',
  }, validations = {
    ReturnConsumedCapacity: {
      enum: ['TOTAL', 'NONE']
    },
    AttributesToGet: {
      lengthGreaterThanOrEqual: 1,
      lengthLessThanOrEqual: 255,
    },
    TableName: {
      required: true,
      custom: validateTable,
      regex: '[a-zA-Z0-9_.-]+',
      lengthGreaterThanOrEqual: 3,
      lengthLessThanOrEqual: 255,
    },
    Key: {
      notNull: true,
    },
  }, extra = [
    function checkKeyNames(data) {
      for (var key in data.Key) {
        if (!Object.keys(data.Key[key]).length)
          return 'Supplied AttributeValue is empty, must contain exactly one of the supported datatypes'
      }
    },
    function checkKeyNotEmpty(data) {
      for (var key in data.Key) {
        for (var type in data.Key[key]) {
          if (!data.Key[key][type])
            return 'One or more parameter values were invalid: An AttributeValue may not contain an empty string.'
        }
      }
    },
    function checkKeyNotEmpty(data) {
      for (var key in data.Key) {
        for (var type in data.Key[key]) {
          if (type == 'N' && isNaN(Number(data.Key[key][type])))
            return 'The parameter cannot be converted to a numeric value: ' + data.Key[key][type]
        }
      }
    },
  ]

  try {
    data = checkTypes(data, types)
    checkValidations(data, validations, extra)
  } catch (e) {
    if (e.statusCode) return sendData(req, res, e.body, e.statusCode)
    throw e
  }

  sendData(req, res, {TableNames: []})
}


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

  function classForNumber(val) {
    return val % 1 !== 0 ? 'java.lang.Double' :
      val >= -32768 && val <= 32767 ? 'java.lang.Short' :
      val >= -2147483648 && val <= 2147483647 ? 'java.lang.Integer' : 'java.lang.Long'
  }

  function checkType(val, type) {
    // TODO: deal with nulls
    if (val == null) return
    switch (type.type || type) {
      case 'Boolean':
        switch (typeof val) {
          case 'number':
            // TODO: Strangely floats seem to be fine...?
            throw typeError('class ' + classForNumber(val) + ' can not be converted to an Boolean')
          case 'string':
            //"\'HELLOWTF\' can not be converted to an Boolean"
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
            // TODO: Should coersion happen here or elsewhere?
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
            throw typeError('class ' + classForNumber(val) + ' can not be converted to an String')
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
            throw typeError('class ' + classForNumber(val) + ' can not be converted to a Blob')
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
    //}
    //switch (type.type) {
      case 'List':
        switch (typeof val) {
          case 'boolean':
          case 'number':
          case 'string':
            throw typeError('Expected list or null')
          case 'object':
            if (!Array.isArray(val)) throw typeError('Start of structure or map found where not expected.')
        }
        return val.map(function(child) { return checkType(child, type.children) })
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
          newVal[key] = checkType(val[key], type.children)
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
        return checkTypes(val, type.children)
      default:
        throw new Error('Unknown type: ' + type)
    }
  }
}

function checkValidations(data, validations, extra) {
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
      throw validationError('The paramater \'' + toLowerFirst(attr) + '\' is required but was not present in the request')
    }
    if (validations[attr].custom) {
      msg = validations[attr].custom(attr, data)
      if (msg) throw validationError(msg)
    }
  }

  (function checkNonRequireds(data, validations, parent) {
    var attr, validation
    for (attr in validations) {
      for (validation in validations[attr]) {
        if (errors.length >= 10) return
        //if (validations[attr].required && data[attr] == null) {
          //throw validationError('The paramater \'' + toLowerFirst(attr) + '\' is required but was not present in the request')
        //}
        //if (validations[attr].custom) {
          //msg = validations[attr].custom(attr, data)
          //if (msg) throw validationError(msg)
        //}
        if (validation == 'required' || validation == 'custom') continue
        if (validation != 'notNull' && data[attr] == null) continue
        if (validation == 'children') {
          //if (data[attr] == null) continue
          if (Array.isArray(data[attr])) {
            for (var i = 0; i < data[attr].length; i++) {
              checkNonRequireds(data[attr][i], validations[attr].children, (parent ? parent + '.' : '') + toLowerFirst(attr) + '.' + (i + 1) + '.member')
            }
            continue
          }
          checkNonRequireds(data[attr], validations[attr].children, (parent ? parent + '.' : '') + toLowerFirst(attr))
          continue
        }
        validateFns[validation](parent, attr, validations[attr][validation], data, errors)
      }
    }
  })(data, validations)
  if (errors.length)
    throw validationError(errors.length + ' validation error' + (errors.length > 1 ? 's' : '') + ' detected: ' + errors.join('; '))

  if (!extra) return
  extra.forEach(function(extraValidation) {
    msg = extraValidation(data)
    if (msg) throw validationError(msg)
  })
}

var validateFns = {}
validateFns.required = function(parent, key, val, data, errors) {
  validate(data[key] != null, 'Member is required', data, parent, key, errors)
}
validateFns.notNull = function(parent, key, val, data, errors) {
  validate(data[key] != null, 'Member must not be null', data, parent, key, errors)
}
validateFns.greaterThanOrEqual = function(parent, key, val, data, errors) {
  validate(data[key] >= val, 'Member must have value greater than or equal to ' + val, data, parent, key, errors)
}
validateFns.lessThanOrEqual = function(parent, key, val, data, errors) {
  validate(data[key] <= val, 'Member must have value less than or equal to ' + val, data, parent, key, errors)
}
validateFns.regex = function(parent, key, pattern, data, errors) {
  validate(RegExp('^' + pattern + '$').test(data[key]), 'Member must satisfy regular expression pattern: ' + pattern, data, parent, key, errors)
}
validateFns.lengthGreaterThanOrEqual = function(parent, key, val, data, errors) {
  validate(data[key].length >= val, 'Member must have length greater than or equal to ' + val, data, parent, key, errors)
}
validateFns.lengthLessThanOrEqual = function(parent, key, val, data, errors) {
  validate(data[key].length <= val, 'Member must have length less than or equal to ' + val, data, parent, key, errors)
}
validateFns.enum = function(parent, key, val, data, errors) {
  validate(~val.indexOf(data[key]), 'Member must satisfy enum value set: [' + val.join(', ') + ']', data, parent, key, errors)
}

function validate(predicate, msg, data, parent, key, errors) {
  if (predicate) return
  var value = data[key] == null ? 'null' : Array.isArray(data[key]) ? '[' + data[key] + ']' : data[key]
  if (value != 'null') value = '\'' + value + '\''
  parent = parent ? parent + '.' : ''
  errors.push('Value ' + value + ' at \'' + parent + toLowerFirst(key) + '\' failed to satisfy constraint: ' + msg)
}

function validateTable(key, data) {
  if (data[key].length < 3 || data[key].length > 255)
    return key + ' must be at least 3 characters long and at most 255 characters long'
}

function toLowerFirst(str) {
  return str[0].toLowerCase() + str.slice(1)
}

if (require.main === module) dynalite.listen(4567)

