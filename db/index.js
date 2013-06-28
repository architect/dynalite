var Readable = require('stream').Readable,
    lazy = require('lazy'),
    levelup = require('levelup'),
    MemDown = require('memdown'),
    sublevel = require('level-sublevel'),
    Lock = require('lock')

var db = sublevel(levelup('./mydb', {db: function(location) { return new MemDown(location) }})),
    tableDb = db.sublevel('table', {valueEncoding: 'json'}),
    itemDb = db.sublevel('item', {valueEncoding: 'json'})

exports.lazy = lazyStream
exports.tableDb = tableDb
exports.itemDb = itemDb
exports.getTable = getTable
exports.createTableMs = 500
exports.deleteTableMs = 500

tableDb.lock = new Lock()

function getTable(name, cb) {
  tableDb.get(name, function(err, table) {
    if (err) {
      if (err.name == 'NotFoundError') {
        err.statusCode = 400
        err.body = {
          __type: 'com.amazonaws.dynamodb.v20120810#ResourceNotFoundException',
          message: 'Requested resource not found: Table: ' + name + ' not found',
        }
      }
      return cb(err)
    }
    cb(null, table)
  })
}

function lazyStream(stream, errHandler) {
  if (Readable) {
    stream = new Readable().wrap(stream)
    stream.setEncoding('utf8')
  }
  if (errHandler) stream.on('error', errHandler)

  return lazy(stream)
}
