#!/usr/bin/env node

var argv = require('minimist')(process.argv.slice(2), {alias: {debug: ['d'], verbose: ['v']}})

if (argv.help || argv.h) {
  // eslint-disable-next-line no-console
  return console.log([
    '',
    'Usage: dynalite [--port <port>] [--path <path>] [options]',
    '',
    'A DynamoDB http server, optionally backed by LevelDB',
    '',
    'Options:',
    '--help, -h            Display this help message and exit',
    '--host                Listen on a specific host address (default: all available)',
    '--port <port>         The port to listen on (default: 4567)',
    '--path <path>         The path to use for the LevelDB store (in-memory by default)',
    '--ssl                 Enable SSL for the web server (default: false)',
    '--createTableMs <ms>  Amount of time tables stay in CREATING state (default: 500)',
    '--deleteTableMs <ms>  Amount of time tables stay in DELETING state (default: 500)',
    '--updateTableMs <ms>  Amount of time tables stay in UPDATING state (default: 500)',
    '--maxItemSizeKb <kb>  Maximum item size (default: 400)',
    '--verbose, -v         Enable verbose logging',
    '--debug, -d           Enable debug logging',
    '',
    'Report bugs at github.com/architect/dynalite/issues',
  ].join('\n'))
}

// If we're PID 1, eg in a docker container, SIGINT won't end the process as usual
if (process.pid == 1) process.on('SIGINT', process.exit)

var server = require('./index.js')(argv)
  .listen(argv.port || 4567, argv.host || undefined, function() {
    var address = server.address(), protocol = argv.ssl ? 'https' : 'http'
    // eslint-disable-next-line no-console
    var host = argv.host || 'localhost'
    console.log('Dynalite listening at: %s://%s:%s', protocol, host, address.port)
  })
