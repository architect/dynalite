#!/usr/bin/env node

var argv = require('minimist')(process.argv.slice(2))

if (argv.help) {
  return console.log([
    '',
    'Usage: dynalite [--port <port>] [--path <path>] [options]',
    '',
    'A mock DynamoDB http server, optionally backed by LevelDB',
    '',
    'Options:',
    '--help                Display this help message and exit',
    '--port <port>         The port to listen on (default: 4567)',
    '--path <path>         The path to use for the LevelDB store (in-memory by default)',
    '--createTableMs <ms>  Amount of time tables stay in CREATING state (default: 500)',
    '--deleteTableMs <ms>  Amount of time tables stay in DELETING state (default: 500)',
    '--updateTableMs <ms>  Amount of time tables stay in UPDATING state (default: 500)',
    '',
    'Report bugs at github.com/mhart/dynalite/issues',
  ].join('\n'))
}

require('./index.js')(argv).listen(argv.port || 4567)
