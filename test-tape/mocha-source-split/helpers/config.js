var useRemoteDynamo = process.env.REMOTE;
var runSlowTests = true;
if (useRemoteDynamo && !process.env.SLOW_TESTS) runSlowTests = false;

const configData = {
  MAX_SIZE: 409600,
  awsRegion: process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION || 'us-east-1',
  awsAccountId: process.env.AWS_ACCOUNT_ID, // Will be updated by getAccountId
  version: 'DynamoDB_20120810',
  prefix: '__dynalite_test_',
  readCapacity: 10,
  writeCapacity: 5,
  testHashTable: null, // Will be set below
  testHashNTable: null,
  testRangeTable: null,
  testRangeNTable: null,
  testRangeBTable: null,
  runSlowTests: runSlowTests,
  useRemoteDynamo: useRemoteDynamo,
  randomString: randomString,
  randomNumber: randomNumber,
  randomName: randomName,
  strDecrement: strDecrement,
};

function randomString () {
  return ('AAAAAAAAA' + randomNumber()).slice(-10);
}

function randomNumber () {
  return String(Math.random() * 0x100000000);
}

function randomName () {
  return configData.prefix + randomString();
}

function strDecrement (str, regex, length) {
  regex = regex || /.?/;
  length = length || 255;
  var lastIx = str.length - 1, lastChar = str.charCodeAt(lastIx) - 1, prefix = str.slice(0, lastIx), finalChar = 255;
  while (lastChar >= 0 && !regex.test(String.fromCharCode(lastChar))) lastChar--;
  if (lastChar < 0) return prefix;
  prefix += String.fromCharCode(lastChar);
  while (finalChar >= 0 && !regex.test(String.fromCharCode(finalChar))) finalChar--;
  if (lastChar < 0) return prefix;
  while (prefix.length < length) prefix += String.fromCharCode(finalChar);
  return prefix;
}

// Set table names after randomName is defined
configData.testHashTable = useRemoteDynamo ? '__dynalite_test_1' : randomName();
configData.testHashNTable = useRemoteDynamo ? '__dynalite_test_2' : randomName();
configData.testRangeTable = useRemoteDynamo ? '__dynalite_test_3' : randomName();
configData.testRangeNTable = useRemoteDynamo ? '__dynalite_test_4' : randomName();
configData.testRangeBTable = useRemoteDynamo ? '__dynalite_test_5' : randomName();

// Export the container object
module.exports = configData; 