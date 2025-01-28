# Dynalite

[![GitHub CI status](https://github.com/architect/dynalite/workflows/Node%20CI/badge.svg)](https://github.com/architect/dynalite/actions?query=workflow%3A%22Node+CI%22)

An implementation of Amazon's DynamoDB built on LevelDB
(well, [@rvagg](https://github.com/rvagg)'s awesome [LevelUP](https://github.com/Level/levelup) to be precise)
for fast in-memory or persistent usage.

This project aims to match the live DynamoDB instances as closely as possible
(and is tested against them in various regions), including all limits and error messages.


## What about Amazon's DynamoDB Local?

This project was created before DynamoDB Local existed, and when it did, it differed a lot from the live instances
in ways that caused my company issues. Since then it's had a lot more development and resources thrown at it,
and is probably more up-to-date than dynalite is. I'd recommend using it over dynalite if you don't mind the
overhead of starting the JVM (or docker) each time. If you need a fast in-memory option that you can start up in
milliseconds, then dynalite might be more suitable for you.


## Example

```sh
$ dynalite --help

Usage: dynalite [--port <port>] [--path <path>] [options]

A DynamoDB http server, optionally backed by LevelDB

Options:
--help, -h            Display this help message and exit
--host                Listen on a specific host address (default: all available)
--port <port>         The port to listen on (default: 4567)
--path <path>         The path to use for the LevelDB store (in-memory by default)
--ssl                 Enable SSL for the web server (default: false)
                        Pass cert file paths with --key, --cert, and --ca
--createTableMs <ms>  Amount of time tables stay in CREATING state (default: 500)
--deleteTableMs <ms>  Amount of time tables stay in DELETING state (default: 500)
--updateTableMs <ms>  Amount of time tables stay in UPDATING state (default: 500)
--maxItemSizeKb <kb>  Maximum item size (default: 400)
--verbose, -v         Enable verbose logging
--debug, -d           Enable debug logging

Report bugs at github.com/architect/dynalite/issues
```

Or programmatically:

```js
// Returns a standard Node.js HTTP server
const dynalite = require('dynalite')
const dynaliteServer = dynalite()

// Listen on port 4567
dynaliteServer.listen(4567, function(err) {
  if (err) throw err
  console.log('Dynalite started on port 4567')
})
```

```js
// Use Dynalite in HTTPS mode
const dynalite = require('dynalite')
const dynaliteServer = dynalite({
  ssl: true,
  key: 'Your private key in PEM format...',
  cert: 'Your cert chain in PEM format...',
  ca: 'Your CA certificate string...',
  // Alternately, pass `useSSLFilePaths: true` to use file paths for `key`, `cert`, and `ca`
})
```

Once running, here's how you use the [AWS SDK](https://github.com/aws/aws-sdk-js) to connect
(after [configuring the SDK](https://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/configuring-the-jssdk.html)):

```js
const AWS = require('aws-sdk')

const dynamo = new AWS.DynamoDB({ endpoint: 'http://localhost:4567' })

dynamo.listTables(console.log.bind(console))
```


## Installation

With [npm](https://www.npmjs.com/), to install the CLI:

```sh
npm install -g dynalite
```

Or to install for development/testing in your project:

```sh
npm install -D dynalite
```

## TODO

- Implement [Transactions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html)
- Implement DynamoDB Streams
- Implement `ReturnItemCollectionMetrics` on all remaining endpoints
- Implement size info for tables and indexes
- Add ProvisionedThroughput checking
- See [open issues on GitHub](https://github.com/architect/dynalite/issues) for any further TODOs
