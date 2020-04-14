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
