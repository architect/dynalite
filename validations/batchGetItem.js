var validations = require('./index'),
    db = require('../db')

exports.types = {
  ReturnConsumedCapacity: {
    type: 'String',
    enum: ['INDEXES', 'TOTAL', 'NONE'],
  },
  RequestItems: {
    type: 'Map',
    notNull: true,
    lengthGreaterThanOrEqual: 1,
    keys: {
      lengthLessThanOrEqual: 255,
      lengthGreaterThanOrEqual: 3,
      regex: '[a-zA-Z0-9_.-]+',
    },
    children: {
      type: 'Structure',
      children: {
        Keys: {
          type: 'List',
          notNull: true,
          lengthGreaterThanOrEqual: 1,
          lengthLessThanOrEqual: 100,
          children: {
            type: 'Map',
            children: 'AttrStructure',
          },
        },
        AttributesToGet: {
          type: 'List',
          lengthGreaterThanOrEqual: 1,
          lengthLessThanOrEqual: 255,
          children: 'String',
        },
        ConsistentRead: 'Boolean',
        ProjectionExpression: {
          type: 'String',
        },
        ExpressionAttributeNames: {
          type: 'Map',
          children: 'String',
        },
      },
    },
  },
}

exports.custom = function(data) {
  var numReqs = 0

  for (var table in data.RequestItems) {
    var tableData = data.RequestItems[table]

    var msg = validations.validateExpressionParams(tableData, ['ProjectionExpression'], ['AttributesToGet'])
    if (msg) return msg

    var seenKeys = Object.create(null)
    for (var i = 0; i < tableData.Keys.length; i++) {
      var key = tableData.Keys[i]

      for (var attr in key) {
        msg = validations.validateAttributeValue(key[attr])
        if (msg) return msg
      }

      // TODO: this is unnecessarily expensive
      var keyStr = Object.keys(key).sort().map(function(attr) { return db.toRangeStr(key[attr]) }).join('/')
      if (seenKeys[keyStr])
        return 'Provided list of item keys contains duplicates'
      seenKeys[keyStr] = true

      numReqs++
      if (numReqs > 100)
        return 'Too many items requested for the BatchGetItem call'
    }

    if (tableData.AttributesToGet) {
      msg = validations.findDuplicate(tableData.AttributesToGet)
      if (msg) return 'One or more parameter values were invalid: Duplicate value in attribute name: ' + msg
    }

    msg = validations.validateExpressions(tableData)
    if (msg) return msg
  }
}
