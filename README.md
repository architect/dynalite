dynalite
--------

[![Build Status](https://secure.travis-ci.org/mhart/dynalite.png?branch=master)](http://travis-ci.org/mhart/dynalite)

An implementation of Amazon's DynamoDB, focussed on correctness and performance, and built on LevelDB
(well, [@rvagg](https://github.com/rvagg)'s awesome [LevelUP](https://github.com/rvagg/node-levelup) to be precise).

This project aims to match the live DynamoDB instances as closely as possible
(and is tested against them in various regions), including all limits and error messages.

Why not Amazon's DynamoDB Local?
--------------------------------

Because it's too buggy! And it differs too much from the live instances in a number of key areas
([see below](#problems-with-amazons-dynamodb-local))

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

Then force your AWS DynamoDB client to connect to Dynalite by specifying `endpoint` in DynamoDB constructor params, eg, in NodeJS: 

```js 
var db = new AWS.DynamoDB({'endpoint': 'http://:::4567'});
```


Installation
------------

With [npm](http://npmjs.org/) do:

```sh
$ npm install -g dynalite
```

TODO
----

- Add ProvisionedThroughput checking
- Add config settings to turn on/off strict checking
- Use efficient range scans for `Query` calls
- Explore edge cases with `Query` and `ScanIndexForward: false` (combine with above)
- Implement `ReturnItemCollectionMetrics` on all remaining endpoints
- Implement size info for tables and indexes
- Is the `ListTables` limit of names returned 100 if no `Limit` supplied?

Problems with Amazon's DynamoDB Local
-------------------------------------

Part of the reason I wrote dynalite was due to the existing mock libraries not exhibiting the same behaviour as the
live instances. Amazon released their DynamoDB Local Java tool recently, but the current version (2013-09-12) still
has quite a number of issues that have prevented us (at [Adslot](http://adslot.com/)) from testing our production code,
especially in a manner that simulates actual behaviour on the live instances.

Some of these are documented (eg, no `ConsumedCapacity` returned), but most aren't -
the items below are a rough list of the issues we've found (and do not exist in dynalite), vaguely in order of importance:

- Returns 400 when `UpdateItem` uses the default `PUT` `Action` without explicitly specifying it
  (this actually prevents certain client libraries from being used at all)
- Does not return correct number of `UnprocessedKeys` in `BatchGet` (returns one less!)
- Returns 400 when trying to put valid numbers with less than 38 significant digits, eg 1e40
- Returns 200 for duplicated keys in `BatchGetItem`
- Returns 200 when hash key is too big in `GetItem`/`BatchGetItem`
- Returns 200 when range key is too big in `GetItem`/`BatchGetItem`
- Returns 200 for `PutItem`/`GetItem`/`UpdateItem`/`BatchGetItem`/`Scan`/etc with empty strings (eg, `{a: {S: ''}}`)
- Returns 413 when request is over 1MB (eg, in a `BatchWrite` with 25 items of 64k), but live instances allow 8MB
- Returns `ResourceNotFoundException` in `ListTables` if `ExclusiveStartName` no longer exists
- Does not return `ConsistentRead` property in `UnprocessedKeys` in `BatchGet` even if requested
- Returns 200 for empty `RequestItems` in `BatchGetItem`/`BatchWriteItem`
- Returns 200 when trying to delete `NS` from `SS` or `NS` from `N` or add `NS` to `SS` or `N` to `NS`
- Allows `UpdateTable` when read and write capacity are same as current (should be an error)
- Tables are created in `ACTIVE` state, not `CREATING` state
- Tables are removed without going into a `DELETING` state
- Tables are updated without going into a `UPDATING` state
- `PutItem` returns `Attributes` by default, even though none are requested
- Does not add `ProvisionedThroughput.LastIncreaseDateTime` in `UpdateTable`
- Does not update `ProvisionedThroughput.NumberOfDecreasesToday` in `UpdateTable`
- Different serialization and validation error messages from live instances (makes it hard to debug)
- Uses uppercase `Message` for error messages (should only use uppercase for `SerializationException`)
- Often returns 500 instead of 400 (or similarly appropriate status)
- Doesn't return `ConsumedCapacity` (documented - but makes it very hard to calculate expected usage)
- Does not calculate the `Scan` size limits correctly so can return too many items
- Does not return `LastEvaluatedKey` on a `Query` when at end of table
- Does not return `LastEvaluatedKey` on a `Scan` when `Limit` equals number of matching items
- Does not return `X-Amzn-RequestId` header
- Does not return `X-Amz-Crc32` header
- Does not return `application/json` if `application/json` is requested
- Fails to put numbers 1e-130 and -1e-130 (succeeds on live instances)
- Returns an error when calling `Query` on a hash table (succeeds on live instances)
- Returns 500 if random attributes are supplied (just ignored on live instances)
- Does not convert doubles to booleans (returns 500 instead)
