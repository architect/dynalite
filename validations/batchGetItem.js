var validateAttributeValue = require('./index').validateAttributeValue,
    validateExpressionParams = require('./index').validateExpressionParams,
    validateExpressions = require('./index').validateExpressions,
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
  var numReqs = 0, table, i, key, msg, attrs, tableData, seenKeys

  for (table in data.RequestItems) {
    tableData = data.RequestItems[table]

    msg = validateExpressionParams(tableData, ['ProjectionExpression'], ['AttributesToGet'])
    if (msg) return msg

    seenKeys = {}
    for (i = 0; i < tableData.Keys.length; i++) {
      for (key in tableData.Keys[i]) {
        msg = validateAttributeValue(tableData.Keys[i][key])
        if (msg) return msg
      }
      // TODO: this is unnecessarily expensive
      var keyStr = Object.keys(tableData.Keys[i]).sort().reduce(function(str, key) {
        var type = Object.keys(tableData.Keys[i][key])[0]
        return str + '~' + db.toLexiStr(tableData.Keys[i][key][type], type)
      }, '')
      if (seenKeys[keyStr])
        return 'Provided list of item keys contains duplicates'
      seenKeys[keyStr] = true

      numReqs++
      if (numReqs > 100)
        return 'Too many items requested for the BatchGetItem call'
    }

    if (tableData.AttributesToGet) {
      attrs = Object.create(null)
      for (i = 0; i < tableData.AttributesToGet.length; i++) {
        if (attrs[tableData.AttributesToGet[i]])
          return 'One or more parameter values were invalid: Duplicate value in attribute name: ' +
            tableData.AttributesToGet[i]
        attrs[tableData.AttributesToGet[i]] = true
      }
    }

    msg = validateExpressions(tableData)
    if (msg) return msg
  }
}
