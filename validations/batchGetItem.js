var validateAttributeValue = require('./index').validateAttributeValue

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
          }
        },
        AttributesToGet: {
          type: 'List',
          lengthGreaterThanOrEqual: 1,
          lengthLessThanOrEqual: 255,
          children: 'String',
        },
        ConsistentRead: 'Boolean',
      }
    }
  },
}

exports.custom = function(data) {
  var numReqs = 0, table, i, key, msg, attrs
  for (table in data.RequestItems) {
    for (i = 0; i < data.RequestItems[table].Keys.length; i++) {
      for (key in data.RequestItems[table].Keys[i]) {
        msg = validateAttributeValue(data.RequestItems[table].Keys[i][key])
        if (msg) return msg
      }
      numReqs++
      if (numReqs > 100)
        return 'Too many items requested for the BatchGetItem call'
    }
    if (data.RequestItems[table].AttributesToGet) {
      attrs = Object.create(null)
      for (i = 0; i < data.RequestItems[table].AttributesToGet.length; i++) {
        if (attrs[data.RequestItems[table].AttributesToGet[i]])
          return 'One or more parameter values were invalid: Duplicate value in attribute name: ' +
            data.RequestItems[table].AttributesToGet[i]
        attrs[data.RequestItems[table].AttributesToGet[i]] = true
      }
    }
  }
}

