var db = require('../db'),
    validateAttributeValue = require('./index').validateAttributeValue

exports.types = {
  ReturnConsumedCapacity: {
    type: 'String',
    enum: ['INDEXES', 'TOTAL', 'NONE']
  },
  ReturnItemCollectionMetrics: {
    type: 'String',
    enum: ['SIZE', 'NONE']
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
    values: {
      lengthLessThanOrEqual: 25,
      lengthGreaterThanOrEqual: 1,
    },
    children: {
      type: 'List',
      children: {
        type: 'Structure',
        children: {
          DeleteRequest: {
            type: 'Structure',
            children: {
              Key: {
                type: 'Map',
                notNull: true,
                children: {
                  type: 'Structure',
                  children: {
                    S: 'String',
                    B: 'Blob',
                    N: 'String',
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
                    }
                  }
                }
              }
            }
          },
          PutRequest: {
            type: 'Structure',
            children: {
              Item: {
                type: 'Map',
                notNull: true,
                children: {
                  type: 'Structure',
                  children: {
                    S: 'String',
                    B: 'Blob',
                    N: 'String',
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
                    }
                  }
                }
              },
            }
          }
        },
      }
    }
  },
}

exports.custom = function(data) {
  var table, i, request, key, msg
  for (table in data.RequestItems) {
    /* jshint -W083 */
    if (data.RequestItems[table].some(function(item) { return !Object.keys(item).length }))
      return 'Supplied AttributeValue has more than one datatypes set, ' +
        'must contain exactly one of the supported datatypes'
    for (i = 0; i < data.RequestItems[table].length; i++) {
      request = data.RequestItems[table][i]
      if (request.PutRequest) {
        for (key in request.PutRequest.Item) {
          msg = validateAttributeValue(request.PutRequest.Item[key])
          if (msg) return msg
        }
        if (db.itemSize(request.PutRequest.Item) > db.MAX_SIZE)
          return 'Item size has exceeded the maximum allowed size'
      } else if (request.DeleteRequest) {
        for (key in request.DeleteRequest.Key) {
          msg = validateAttributeValue(request.DeleteRequest.Key[key])
          if (msg) return msg
        }
      }
    }
  }
}

