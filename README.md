dynalite
--------

[![Build Status](https://secure.travis-ci.org/mhart/dynalite.png?branch=master)](http://travis-ci.org/mhart/dynalite)

A mock implementation of Amazon's DynamoDB, focussed on correctness and performance, and built on LevelDB
(well, [@rvagg](https://github.com/rvagg)'s awesome [LevelUP](https://github.com/rvagg/node-levelup) to be precise).

This project aims to match the live DynamoDB instances as closely as possible
(and is tested against them in various regions), including all limits and error messages.

Example
-------

```sh
$ dynalite --help

Usage: dynalite [--port <port>] [--path <path>] [options]

A mock DynamoDB http server, optionally backed by LevelDB

Options:
--help                Display this help message and exit
--port <port>         The port to listen on (default: 4567)
--path <path>         The path to use for the LevelDB store (in-memory by default)
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

Installation
------------

With [npm](http://npmjs.org/) do:

```sh
$ npm install dynalite
```

TODO
----

* Add ProvisionedThroughput checking
* Add config settings to turn on/off strict checking
* Allow for other persistence types (LevelDOWN-Hyper, etc)
* Use efficient range scans for Query calls
* Implement `ReturnItemCollectionMetrics` on all remaining endpoints
* At what point does BatchGetItem#UnprocessedKeys get triggered?
* Is the ListTables limit of names returned 100 if no Limit supplied?
