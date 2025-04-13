const test = require('tape');
const helpers = require('./helpers');

const target = 'DescribeTable';
const assertType = helpers.assertType.bind(null, target);
const assertValidation = helpers.assertValidation.bind(null, target);
const assertNotFound = helpers.assertNotFound.bind(null, target);

test('describeTable', (t) => {

  t.test('serializations', (st) => {
    st.test('should return SerializationException when TableName is not a string', (sst) => {
      assertType('TableName', 'String', (err) => {
        sst.error(err, 'assertType should not error');
        sst.end();
      });
    });
    st.end(); // End serializations tests
  });

  t.test('validations', (st) => {
    st.test('should return ValidationException for no TableName', (sst) => {
      assertValidation({},
        'The parameter \'TableName\' is required but was not present in the request',
        (err) => {
          sst.error(err, 'assertValidation should not error');
          sst.end();
        });
    });

    st.test('should return ValidationException for empty TableName', (sst) => {
      assertValidation({ TableName: '' },
        'TableName must be at least 3 characters long and at most 255 characters long',
        (err) => {
          sst.error(err, 'assertValidation should not error');
          sst.end();
        });
    });

    st.test('should return ValidationException for short TableName', (sst) => {
      assertValidation({ TableName: 'a;' },
        'TableName must be at least 3 characters long and at most 255 characters long',
        (err) => {
          sst.error(err, 'assertValidation should not error');
          sst.end();
        });
    });

    st.test('should return ValidationException for long TableName', (sst) => {
      assertValidation({ TableName: new Array(256 + 1).join('a') },
        'TableName must be at least 3 characters long and at most 255 characters long',
        (err) => {
          sst.error(err, 'assertValidation should not error');
          sst.end();
        });
    });

    st.test('should return ValidationException for null attributes', (sst) => {
      assertValidation({ TableName: 'abc;' },
        '1 validation error detected: ' +
        'Value \'abc;\' at \'tableName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        (err) => {
          sst.error(err, 'assertValidation should not error');
          sst.end();
        });
    });

    st.test('should return ResourceNotFoundException if table does not exist', (sst) => {
      const name = helpers.randomString();
      assertNotFound({ TableName: name }, 'Requested resource not found: Table: ' + name + ' not found',
        (err) => {
          sst.error(err, 'assertNotFound should not error');
          sst.end();
        });
    });

    st.end(); // End validations tests
  });

  t.end(); // End describeTable tests
}); 