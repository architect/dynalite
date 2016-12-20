#!/usr/bin/env node

var argv = require('minimist')(process.argv.slice(2))

if (argv.help) {
  // eslint-disable-next-line no-console
  return console.log([
    '',
    'Usage: dynalite [--port <port>] [--path <path>] [options]',
    '',
    'A DynamoDB http server, optionally backed by LevelDB',
    '',
    'Options:',
    '--help                Display this help message and exit',
    '--port <port>         The port to listen on (default: 4567)',
    '--path <path>         The path to use for the LevelDB store (in-memory by default)',
    '--ssl                 Enable SSL for the web server (default: false)',
    '--createTableMs <ms>  Amount of time tables stay in CREATING state (default: 500)',
    '--deleteTableMs <ms>  Amount of time tables stay in DELETING state (default: 500)',
    '--updateTableMs <ms>  Amount of time tables stay in UPDATING state (default: 500)',
    '--maxItemSizeKb <kb>  Maximum item size (default: 400)',
    '',
    'Report bugs at github.com/mhart/dynalite/issues',
  ].join('\n'))
}

var server = require('./index.js').server(argv).listen(argv.port || 4567, function() {
  var address = server.address(), protocol = argv.ssl ? 'https' : 'http'
  // eslint-disable-next-line no-console
  console.log('Listening at %s://%s:%s', protocol, address.address, address.port)
})
