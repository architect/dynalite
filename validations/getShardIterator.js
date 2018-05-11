exports.types = {
  ShardId: {
    required: true,
    type: 'String',
    regex: '[a-zA-Z0-9\-]+',
    lengthGreaterThanOrEqual: 28,
    lengthLessThanOrEqual: 65,
  },
  ShardIteratorType: {
    required: true,
    type: 'String',
    enum: ['TRIM_HORIZON', 'LATEST', 'AT_SEQUENCE_NUMBER', 'AFTER_SEQUENCE_NUMBER'],
  },
  StreamArn: {
    required: true,
    type: 'String',
    regex: '[a-zA-Z0-9\-\.:/_]+',
    lengthGreaterThanOrEqual: 37,
    lengthLessThanOrEqual: 1024,
  },
  SequenceNumber: {
    type: 'String',
    regex: '\d+',
    lengthGreaterThanOrEqual: 21,
    lengthLessThanOrEqual: 40,
  },
}
