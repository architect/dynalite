dynalite
--------

[![Build Status](https://secure.travis-ci.org/mhart/dynalite.png?branch=master)](http://travis-ci.org/mhart/dynalite)

An implementation of Amazon's DynamoDB, focussed on correctness and performance, and built on LevelDB
(well, [@rvagg](https://github.com/rvagg)'s awesome [LevelUP](https://github.com/rvagg/node-levelup) to be precise).

This project aims to match the live DynamoDB instances as closely as possible
(and is tested against them in various regions), including all limits and error messages.

NB: Schema changes in v1.x
--------------------------

If you've been using v0.x with a saved path on your filesystem, you should note
that the schema has been changed to separate out indexes. This means that if
you have tables with indexes on the old schema, you'll need to update them –
this should just be a matter of getting each item and writing it again – a
Scan/BatchWriteItem loop should suffice to populate the indexes correctly.

Why not Amazon's DynamoDB Local?
--------------------------------

Good question! These days it's actually pretty good, and considering it's now probably
used by countless AWS devs, it'll probably be well supported going forward. Unless you
specifically can't, or don't want to, use Java, or you're having problems with it,
you'll probably be better off sticking with it! Originally, however, DynamoDB Local
didn't exist, and when it did, differed a lot from the live instances in ways that caused
my company issues. Most of those issues have been addressed in time, but DynamoDB Local
does still differ in a number of ways from the live DynamoDB instances –
([see below](#problems-with-amazons-dynamodb-local-updated-2016-04-19)) for details.

Example
-------

```sh
$ dynalite --help

Usage: dynalite [--port <port>] [--path <path>] [options]

A DynamoDB http server, optionally backed by LevelDB

Options:
--help                Display this help message and exit
--port <port>         The port to listen on (default: 4567)
--path <path>         The path to use for the LevelDB store (in-memory by default)
--ssl                 Enable SSL for the web server (default: false)
--createTableMs <ms>  Amount of time tables stay in CREATING state (default: 500)
--deleteTableMs <ms>  Amount of time tables stay in DELETING state (default: 500)
--updateTableMs <ms>  Amount of time tables stay in UPDATING state (default: 500)
--maxItemSizeKb <kb>  Maximum item size (default: 400)

Report bugs at github.com/mhart/dynalite/issues
```

Or programmatically:

```js
// Returns a standard Node.js HTTP server
var dynalite = require('dynalite'),
    dynaliteServer = dynalite({path: './mydb', createTableMs: 50})

// Listen on port 4567
dynaliteServer.listen(4567, function(err) {
  if (err) throw err
  console.log('Dynalite started on port 4567')
})
```

Once running, here's how you use the [AWS SDK](https://github.com/aws/aws-sdk-js) to connect
(after [configuring the SDK](http://docs.aws.amazon.com/AWSJavaScriptSDK/guide/node-configuring.html)):

```js
var AWS = require('aws-sdk')

var dynamo = new AWS.DynamoDB({endpoint: 'http://localhost:4567'})

dynamo.listTables(console.log.bind(console))
```

Installation
------------

With [npm](http://npmjs.org/) do:

```sh
$ npm install -g dynalite
```

TODO
----

- Implement DynamoDB Streams
- Implement `ReturnItemCollectionMetrics` on all remaining endpoints
- Implement size info for tables and indexes
- Add ProvisionedThroughput checking
- See [open issues on GitHub](https://github.com/mhart/dynalite/issues) for any further TODOs

Problems with Amazon's DynamoDB Local (UPDATED 2016-04-19)
-------------------------------------

Part of the reason I wrote dynalite was due to the existing mock libraries not exhibiting the same behaviour as the
live instances. Amazon then released their DynamoDB Local Java, but the early versions were still very different.
The latest version I checked (2016-04-19) is much better, but still has a few differences.

[Some of these are documented](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html#Tools.DynamoDBLocal.Differences),
but most aren't - the items below are a rough list of the issues found, vaguely in order of importance:

- Does not return nested attributes correctly for `UpdateItem`
- Does not calculate size limits accurately for `BatchGetItem`/`Query`/`Scan` result sets
- Does deal with `ALL_ATTRIBUTES` correctly for global index on `Query`/`Scan`
- Does not prevent primary keys in `QueryFilter` and `FilterExpression` for `Query`
- Does not detect duplicate values in `AttributesToGet`
- Does not return `LastEvaluatedKey` when size just over limit for `Query`/`Scan`
- Does not return `ConsistentRead` property in `UnprocessedKeys` in `BatchGetItem` even if requested
- Doesn't return `ConsumedCapacity` (documented - but makes it very hard to calculate expected usage)
- Often returns 500 instead of 400 (or similarly appropriate status)
- Different serialization and validation error messages from live instances (makes it hard to debug)
- Does not return `application/json` if `application/json` is requested
- Does not return `Query`/`Scan` items in same order when using hash key or hash `GlobalSecondaryIndex` (shouldn't rely on this anyway)
