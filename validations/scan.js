var validateAttributeValue = require('./index').validateAttributeValue,
    validateExpressionParams = require('./index').validateExpressionParams,
    validateExpressions = require('./index').validateExpressions

exports.types = {
  Limit: {
    type: 'Integer',
    greaterThanOrEqual: 1,
  },
  ReturnConsumedCapacity: {
    type: 'String',
    enum: ['INDEXES', 'TOTAL', 'NONE'],
  },
  AttributesToGet: {
    type: 'List',
    lengthGreaterThanOrEqual: 1,
    lengthLessThanOrEqual: 255,
    children: 'String',
  },
  Segment: {
    type: 'Integer',
    greaterThanOrEqual: 0,
  },
  Select: {
    type: 'String',
    enum: ['SPECIFIC_ATTRIBUTES', 'COUNT', 'ALL_ATTRIBUTES', 'ALL_PROJECTED_ATTRIBUTES'],
  },
  ScanFilter: {
    type: 'Map',
    children: {
      type: 'Structure',
      children: {
        AttributeValueList: {
          type: 'List',
          children: 'AttrStructure',
        },
        ComparisonOperator: {
          type: 'String',
          notNull: true,
          enum: ['IN', 'NULL', 'BETWEEN', 'LT', 'NOT_CONTAINS', 'EQ', 'GT', 'NOT_NULL', 'NE', 'LE', 'BEGINS_WITH', 'GE', 'CONTAINS'],
        },
      },
    },
  },
  ConditionalOperator: {
    type: 'String',
    enum: ['OR', 'AND'],
  },
  TotalSegments: {
    type: 'Integer',
    greaterThanOrEqual: 1,
  },
  TableName: {
    type: 'String',
    notNull: true,
    regex: '[a-zA-Z0-9_.-]+',
    lengthGreaterThanOrEqual: 3,
    lengthLessThanOrEqual: 255,
  },
  ExclusiveStartKey: {
    type: 'Map',
    children: 'AttrStructure',
  },
  IndexName: {
    type: 'String',
    regex: '[a-zA-Z0-9_.-]+',
    lengthGreaterThanOrEqual: 3,
    lengthLessThanOrEqual: 255,
  },
  FilterExpression: {
    type: 'String',
  },
  ProjectionExpression: {
    type: 'String',
  },
  ExpressionAttributeValues: {
    type: 'Map',
    children: 'AttrStructure',
  },
  ExpressionAttributeNames: {
    type: 'Map',
    children: 'String',
  },
}

exports.custom = function(data) {

  var msg = validateExpressionParams(data,
    ['ProjectionExpression', 'FilterExpression'],
    ['AttributesToGet', 'ScanFilter', 'ConditionalOperator'])
  if (msg) return msg

  if (data.AttributesToGet) {
    var attrs = Object.create(null)
    for (var i = 0; i < data.AttributesToGet.length; i++) {
      if (attrs[data.AttributesToGet[i]])
        return 'One or more parameter values were invalid: Duplicate value in attribute name: ' +
          data.AttributesToGet[i]
      attrs[data.AttributesToGet[i]] = true
    }
  }

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
  for (var key in data.ScanFilter) {
    var comparisonOperator = data.ScanFilter[key].ComparisonOperator
    var attrValList = data.ScanFilter[key].AttributeValueList || []
    for (i = 0; i < attrValList.length; i++) {
      msg = validateAttributeValue(attrValList[i])
      if (msg) return msg
    }

    if ((typeof lengths[comparisonOperator] == 'number' && attrValList.length != lengths[comparisonOperator]) ||
        (attrValList.length < lengths[comparisonOperator][0] || attrValList.length > lengths[comparisonOperator][1]))
      return 'One or more parameter values were invalid: Invalid number of argument(s) for the ' +
        comparisonOperator + ' ComparisonOperator'

    if (types[comparisonOperator]) {
      for (i = 0; i < attrValList.length; i++) {
        if (!~types[comparisonOperator].indexOf(Object.keys(attrValList[i])[0]))
          return 'One or more parameter values were invalid: ComparisonOperator ' + comparisonOperator +
            ' is not valid for ' + Object.keys(attrValList[i])[0] + ' AttributeValue type'
      }
    }
  }

  for (key in data.ExclusiveStartKey) {
    msg = validateAttributeValue(data.ExclusiveStartKey[key])
    if (msg) return 'The provided starting key is invalid: ' + msg
  }

  if (data.Segment && data.TotalSegments == null) {
    return 'The TotalSegments parameter is required but was not present in the request when Segment parameter is present'
  }

  if (data.TotalSegments) {
    if (data.Segment == null) {
      return 'The Segment parameter is required but was not present in the request when parameter TotalSegments is present'
    }
    if (data.Segment >= data.TotalSegments) {
      return 'The Segment parameter is zero-based and must be less than parameter TotalSegments: ' +
        'Segment: ' + data.Segment + ' is not less than TotalSegments: ' + data.TotalSegments
    }
  }

  msg = validateExpressions(data, ['ProjectionExpression', 'FilterExpression'])
  if (msg) return msg
}
