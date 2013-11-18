dynalite
--------

[![Build Status](https://secure.travis-ci.org/mhart/dynalite.png?branch=master)](http://travis-ci.org/mhart/dynalite)

A mock implementation of Amazon's DynamoDB, focussed on correctness and performance, and built on LevelDB
(well, [@rvagg](https://github.com/rvagg)'s awesome [LevelUP](https://github.com/rvagg/node-levelup) to be precise).

All basic actions and validations have now been implemented, but there are still a number of issues (see below)
before this module should be considered for general use.

Example
-------

```js
// Returns a standard Node.js HTTP server
var dynalite = require('dynalite')

// Listen on port 4567
dynalite.listen(4567, function(err) {
  if (err) throw err
  console.log('Dynalite started on port 4567')
})
```

Can also just run on the command line (will listen on port 4567) - a better command line runner is in the works.

```sh
$ node index.js
```

TODO
----

* Add executable script
* Add config settings, especially for the table delays and strict checking
* Allow for different persistence types (LevelDOWN-Hyper, etc)
* Use efficient range scans for Query calls
* Implement `ReturnItemCollectionMetrics` on all remaining endpoints
* Check for any missing `ExclusiveStartKey`/`LastEvaluatedKey` functionality

