var async = require('async');
var reqHelpers = require('./request');

function assertSerialization (target, data, msg, done) {
  reqHelpers.request(reqHelpers.opts(target, data), function (err, res) {
    if (err) return done(err);
    res.statusCode.should.equal(400);
    res.body.should.eql({
      __type: 'com.amazon.coral.service#SerializationException',
      Message: msg,
    });
    done();
  });
}

function assertType (target, property, type, done) {
  var msgs = [], pieces = property.split('.'), subtypeMatch = type.match(/(.+?)<(.+)>$/), subtype;
  if (subtypeMatch != null) {
    type = subtypeMatch[1];
    subtype = subtypeMatch[2];
  }
  var castMsg = "class sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl cannot be cast to class java.lang.Class (sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl and java.lang.Class are in module java.base of loader 'bootstrap')";
  switch (type) {
  case 'Boolean':
    msgs = [
      [ '23', 'Unexpected token received from parser' ],
      [ 23, 'NUMBER_VALUE cannot be converted to Boolean' ],
      [ -2147483648, 'NUMBER_VALUE cannot be converted to Boolean' ],
      [ 2147483648, 'NUMBER_VALUE cannot be converted to Boolean' ],
      [ 34.56, 'DECIMAL_VALUE cannot be converted to Boolean' ],
      [ [], 'Unrecognized collection type class java.lang.Boolean' ],
      [ {}, 'Start of structure or map found where not expected' ],
    ];
    break;
  case 'String':
    msgs = [
      [ true, 'TRUE_VALUE cannot be converted to String' ],
      [ false, 'FALSE_VALUE cannot be converted to String' ],
      [ 23, 'NUMBER_VALUE cannot be converted to String' ],
      [ -2147483648, 'NUMBER_VALUE cannot be converted to String' ],
      [ 2147483648, 'NUMBER_VALUE cannot be converted to String' ],
      [ 34.56, 'DECIMAL_VALUE cannot be converted to String' ],
      [ [], 'Unrecognized collection type class java.lang.String' ],
      [ {}, 'Start of structure or map found where not expected' ],
    ];
    break;
  case 'Integer':
    msgs = [
      [ '23', 'STRING_VALUE cannot be converted to Integer' ],
      [ true, 'TRUE_VALUE cannot be converted to Integer' ],
      [ false, 'FALSE_VALUE cannot be converted to Integer' ],
      [ [], 'Unrecognized collection type class java.lang.Integer' ],
      [ {}, 'Start of structure or map found where not expected' ],
    ];
    break;
  case 'Long':
    msgs = [
      [ '23', 'STRING_VALUE cannot be converted to Long' ],
      [ true, 'TRUE_VALUE cannot be converted to Long' ],
      [ false, 'FALSE_VALUE cannot be converted to Long' ],
      [ [], 'Unrecognized collection type class java.lang.Long' ],
      [ {}, 'Start of structure or map found where not expected' ],
    ];
    break;
  case 'Blob':
    msgs = [
      [ true, 'only base-64-encoded strings are convertible to bytes' ],
      [ 23, 'only base-64-encoded strings are convertible to bytes' ],
      [ -2147483648, 'only base-64-encoded strings are convertible to bytes' ],
      [ 2147483648, 'only base-64-encoded strings are convertible to bytes' ],
      [ 34.56, 'only base-64-encoded strings are convertible to bytes' ],
      [ [], 'Unrecognized collection type class java.nio.ByteBuffer' ],
      [ {}, 'Start of structure or map found where not expected' ],
      [ '23456', 'Base64 encoded length is expected a multiple of 4 bytes but found: 5' ],
      [ '=+/=', 'Invalid last non-pad Base64 character dectected' ],
      [ '+/+=', 'Invalid last non-pad Base64 character dectected' ],
    ];
    break;
  case 'List':
    msgs = [
      [ '23', 'Unexpected field type' ],
      [ true, 'Unexpected field type' ],
      [ 23, 'Unexpected field type' ],
      [ -2147483648, 'Unexpected field type' ],
      [ 2147483648, 'Unexpected field type' ],
      [ 34.56, 'Unexpected field type' ],
      [ {}, 'Start of structure or map found where not expected' ],
    ];
    break;
  case 'ParameterizedList':
    msgs = [
      [ '23', castMsg ],
      [ true, castMsg ],
      [ 23, castMsg ],
      [ -2147483648, castMsg ],
      [ 2147483648, castMsg ],
      [ 34.56, castMsg ],
      [ {}, 'Start of structure or map found where not expected' ],
    ];
    break;
  case 'Map':
    msgs = [
      [ '23', 'Unexpected field type' ],
      [ true, 'Unexpected field type' ],
      [ 23, 'Unexpected field type' ],
      [ -2147483648, 'Unexpected field type' ],
      [ 2147483648, 'Unexpected field type' ],
      [ 34.56, 'Unexpected field type' ],
      [ [], 'Unrecognized collection type java.util.Map<java.lang.String, ' + (~subtype.indexOf('.') ? subtype : 'com.amazonaws.dynamodb.v20120810.' + subtype) + '>' ],
    ];
    break;
  case 'ParameterizedMap':
    msgs = [
      [ '23', castMsg ],
      [ true, castMsg ],
      [ 23, castMsg ],
      [ -2147483648, castMsg ],
      [ 2147483648, castMsg ],
      [ 34.56, castMsg ],
      [ [], 'Unrecognized collection type java.util.Map<java.lang.String, com.amazonaws.dynamodb.v20120810.AttributeValue>' ],
    ];
    break;
  case 'ValueStruct':
    msgs = [
      [ '23', 'Unexpected value type in payload' ],
      [ true, 'Unexpected value type in payload' ],
      [ 23, 'Unexpected value type in payload' ],
      [ -2147483648, 'Unexpected value type in payload' ],
      [ 2147483648, 'Unexpected value type in payload' ],
      [ 34.56, 'Unexpected value type in payload' ],
      [ [], 'Unrecognized collection type class com.amazonaws.dynamodb.v20120810.' + subtype ],
    ];
    break;
  case 'FieldStruct':
    msgs = [
      [ '23', 'Unexpected field type' ],
      [ true, 'Unexpected field type' ],
      [ 23, 'Unexpected field type' ],
      [ -2147483648, 'Unexpected field type' ],
      [ 2147483648, 'Unexpected field type' ],
      [ 34.56, 'Unexpected field type' ],
      [ [], 'Unrecognized collection type class com.amazonaws.dynamodb.v20120810.' + subtype ],
    ];
    break;
  case 'AttrStruct':
    async.forEach([
      [ property, subtype + '<AttributeValue>' ],
      [ property + '.S', 'String' ],
      [ property + '.N', 'String' ],
      [ property + '.B', 'Blob' ],
      [ property + '.BOOL', 'Boolean' ],
      [ property + '.NULL', 'Boolean' ],
      [ property + '.SS', 'List' ],
      [ property + '.SS.0', 'String' ],
      [ property + '.NS', 'List' ],
      [ property + '.NS.0', 'String' ],
      [ property + '.BS', 'List' ],
      [ property + '.BS.0', 'Blob' ],
      [ property + '.L', 'List' ],
      [ property + '.L.0', 'ValueStruct<AttributeValue>' ],
      [ property + '.L.0.BS', 'List' ],
      [ property + '.L.0.BS.0', 'Blob' ],
      [ property + '.M', 'Map<AttributeValue>' ],
      [ property + '.M.a', 'ValueStruct<AttributeValue>' ],
      [ property + '.M.a.BS', 'List' ],
      [ property + '.M.a.BS.0', 'Blob' ],
    ], function (test, cb) { assertType(target, test[0], test[1], cb) }, done);
    return;
  default:
    throw new Error('Unknown type: ' + type);
  }
  async.forEach(msgs, function (msg, cb) {
    var data = {}, child = data, i, ix;
    for (i = 0; i < pieces.length - 1; i++) {
      ix = Array.isArray(child) ? 0 : pieces[i];
      child = child[ix] = pieces[i + 1] === '0' ? [] : {};
    }
    ix = Array.isArray(child) ? 0 : pieces[pieces.length - 1];
    child[ix] = msg[0];
    assertSerialization(target, data, msg[1], cb);
  }, done);
}

function assertAccessDenied (target, data, msg, done) {
  reqHelpers.request(reqHelpers.opts(target, data), function (err, res) {
    if (err) return done(err);
    res.statusCode.should.equal(400);
    if (typeof res.body !== 'object') {
      return done(new Error('Not JSON: ' + res.body));
    }
    res.body.__type.should.equal('com.amazon.coral.service#AccessDeniedException');
    if (msg instanceof RegExp) {
      res.body.Message.should.match(msg);
    }
    else {
      res.body.Message.should.equal(msg);
    }
    done();
  });
}

function assertValidation (target, data, msg, done) {
  reqHelpers.request(reqHelpers.opts(target, data), function (err, res) {
    if (err) return done(err);
    if (typeof res.body !== 'object') {
      return done(new Error('Not JSON: ' + res.body));
    }
    res.body.__type.should.equal('com.amazon.coral.validate#ValidationException');
    if (msg instanceof RegExp) {
      res.body.message.should.match(msg);
    }
    else if (Array.isArray(msg)) {
      var prefix = msg.length + ' validation error' + (msg.length === 1 ? '' : 's') + ' detected: ';
      res.body.message.should.startWith(prefix);
      var errors = res.body.message.slice(prefix.length).split('; ');
      for (var i = 0; i < msg.length; i++) {
        errors.should.matchAny(msg[i]);
      }
    }
    else {
      res.body.message.should.equal(msg);
    }
    res.statusCode.should.equal(400);
    done();
  });
}

function assertNotFound (target, data, msg, done) {
  reqHelpers.request(reqHelpers.opts(target, data), function (err, res) {
    if (err) return done(err);
    res.statusCode.should.equal(400);
    res.body.should.eql({
      __type: 'com.amazonaws.dynamodb.v20120810#ResourceNotFoundException',
      message: msg,
    });
    done();
  });
}

function assertInUse (target, data, msg, done) {
  reqHelpers.request(reqHelpers.opts(target, data), function (err, res) {
    if (err) return done(err);
    res.statusCode.should.equal(400);
    res.body.should.eql({
      __type: 'com.amazonaws.dynamodb.v20120810#ResourceInUseException',
      message: msg,
    });
    done();
  });
}

function assertConditional (target, data, done) {
  reqHelpers.request(reqHelpers.opts(target, data), function (err, res) {
    if (err) return done(err);
    res.statusCode.should.equal(400);
    res.body.should.eql({
      __type: 'com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException',
      message: 'The conditional request failed',
    });
    done();
  });
}

exports.assertSerialization = assertSerialization;
exports.assertType = assertType;
exports.assertValidation = assertValidation;
exports.assertNotFound = assertNotFound;
exports.assertInUse = assertInUse;
exports.assertConditional = assertConditional;
exports.assertAccessDenied = assertAccessDenied; 