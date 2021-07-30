var validations = require('./index'),
    db = require('../db')

exports.types = {
  ReturnConsumedCapacity: {
    type: 'String',
    enum: ['INDEXES', 'TOTAL', 'NONE'],
  },
  ReturnItemCollectionMetrics: {
    type: 'String',
    enum: ['SIZE', 'NONE'],
  },
  ClientRequestToken: {
    type: 'String'
  },
  TransactItems: {
    type: 'List',
    notNull: true,
    lengthGreaterThanOrEqual: 1,
    lengthLessThanOrEqual: 25,
    children: {
      type: 'ValueStruct<TransactWriteItem>',
      children: {
        Put: {
          type: 'FieldStruct<Put>',
          children: {
            TableName: {
              type: 'String',
            },
            ExpressionAttributeValues: {
              type: 'Map<AttributeValue>',
              children: 'AttrStruct<ValueStruct>',
            },
            ExpressionAttributeNames: {
              type: 'Map<java.lang.String>',
              children: 'String',
            },
            ReturnValuesOnConditionCheckFailure: {
              type: 'String',
              enum: ['ALL_OLD', 'NONE'],
            },
            ConditionExpression: {
              type: 'String',
            },
            Item: {
              type: 'Map<AttributeValue>',
              notNull: true,
              children: 'AttrStruct<ValueStruct>',
            },
          },
        },
        Update: {
          type: 'FieldStruct<Update>',
          children: {
            TableName: {
              type: 'String',
            },
            ExpressionAttributeValues: {
              type: 'Map<AttributeValue>',
              children: 'AttrStruct<ValueStruct>',
            },
            ExpressionAttributeNames: {
              type: 'Map<java.lang.String>',
              children: 'String',
            },
            ReturnValuesOnConditionCheckFailure: {
              type: 'String',
              enum: ['ALL_OLD', 'NONE'],
            },
            ConditionExpression: {
              type: 'String',
            },
            Key: {
              type: 'Map<AttributeValue>',
              notNull: true,
              children: 'AttrStruct<ValueStruct>',
            },
            UpdateExpression: {
              type: 'String',
            },
          },
        },
        Delete: {
          type: 'FieldStruct<Delete>',
          children: {
            TableName: {
              type: 'String',
            },
            ExpressionAttributeValues: {
              type: 'Map<AttributeValue>',
              children: 'AttrStruct<ValueStruct>',
            },
            ExpressionAttributeNames: {
              type: 'Map<java.lang.String>',
              children: 'String',
            },
            ReturnValuesOnConditionCheckFailure: {
              type: 'String',
              enum: ['ALL_OLD', 'NONE'],
            },
            ConditionExpression: {
              type: 'String',
            },
            Key: {
              type: 'Map<AttributeValue>',
              notNull: true,
              children: 'AttrStruct<ValueStruct>',
            },
          },
        },
        ConditionCheck: {
          type: 'FieldStruct<ConditionCheck>',
          children: {
            TableName: {
              type: 'String',
            },
            ExpressionAttributeValues: {
              type: 'Map<AttributeValue>',
              children: 'AttrStruct<ValueStruct>',
            },
            ExpressionAttributeNames: {
              type: 'Map<java.lang.String>',
              children: 'String',
            },
            ReturnValuesOnConditionCheckFailure: {
              type: 'String',
              enum: ['ALL_OLD', 'NONE'],
            },
            ConditionExpression: {
              type: 'String',
            },
            Key: {
              type: 'Map<AttributeValue>',
              notNull: true,
              children: 'AttrStruct<ValueStruct>',
            },
          },
        },
      },
    },
  },
}

exports.custom = function(data, store) {
  var i, request, msg, key
  for (i = 0; i < data.TransactItems.length; i++) {
    request = data.TransactItems[i]
    if (request.Put) {
      for (key in request.Put.Item) {
        msg = validations.validateAttributeValue(request.Put.Item[key])
        if (msg) return msg
      }
      if (db.itemSize(request.Put.Item) > store.options.maxItemSize)
        return 'Item size has exceeded the maximum allowed size'
    } else if (request.Delete) {
        for (key in request.Delete.Key) {
          msg = validations.validateAttributeValue(request.Delete.Key[key])
          if (msg) return msg
        }
    } else if (request.Update) {
        for (key in request.Update.Key) {
          msg = validations.validateAttributeValue(request.Update.Key[key])
          if (msg) return msg
        }
        msg = validations.validateExpressionParams(request.Update,
            ['UpdateExpression', 'ConditionExpression'],
            ['AttributeUpdates', 'Expected'])
        if (msg) return msg
        msg = validations.validateAttributeConditions(request.Update)
        if (msg) return msg
        msg = validations.validateExpressions(request.Update)
        if (msg) return msg
    } else if (request.ConditionCheck) {
        for (key in request.ConditionCheck.Key) {
          msg = validations.validateAttributeValue(request.ConditionCheck.Key[key])
          if (msg) return msg
        }
        msg = validations.validateExpressionParams(request.ConditionCheck, ['ConditionExpression'], [])
        if (msg) return msg
        msg = validations.validateExpressions(request.ConditionCheck)
        if (msg) return msg
    } else {
      return 'The action or operation requested is invalid. Verify that the action is typed correctly.'
    }
  }
}
