// helpers/config.js
const useRemoteDynamo = process.env.REMOTE;
let runSlowTests = true;
if (useRemoteDynamo && !process.env.SLOW_TESTS) runSlowTests = false;

const MAX_SIZE = 409600;
const awsRegion = process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION || 'us-east-1';
let awsAccountId = process.env.AWS_ACCOUNT_ID; // This will be updated later
const version = 'DynamoDB_20120810';
const prefix = '__dynalite_test_';

const readCapacity = 10;
const writeCapacity = 5;

const CREATE_REMOTE_TABLES = true;
const DELETE_REMOTE_TABLES = true;

module.exports = {
    useRemoteDynamo,
    runSlowTests,
    MAX_SIZE,
    awsRegion,
    // Provide getter/setter for accountId as it's discovered dynamically
    setAwsAccountId: (id) => { awsAccountId = id; },
    getAwsAccountId: () => awsAccountId,
    version,
    prefix,
    readCapacity,
    writeCapacity,
    CREATE_REMOTE_TABLES,
    DELETE_REMOTE_TABLES,
};
