# shmynamo

Gratefully forked from [dynalite](https://github.com/architect/dynalite). This version only provides an in-memory implementation of dynamo for fast and somewhat accurate testing. See the original repository for caveats around accuracy (ala dynamodb local).

# Goals
1. Modern (typed, esm)
1. Accurate
1. Fast
1. No IO


## Example
```js
import { db, httpHandler } from 'shmynamo';
import mitm from 'mitm';

const store = db.create()
const handler = httpHandler.bind(null, store)

const httpIntercept = mitm();
httpIntercept.on('request', handler);

const dynamoClient = DynamoDBClient();
await dynamoClient.send(new CreateTableCommand({...}))
```