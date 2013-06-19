var levelup = require('levelup'),
    MemDown = require('memdown'),
    sublevel = require('level-sublevel'),
    levelUpdate = require('level-update')

var db = sublevel(levelup('./mydb', {db: function(location){ return new MemDown(location) }})),
    tableDb = db.sublevel('table', {valueEncoding: 'json'})

levelUpdate(tableDb, function(newValue, oldValue) {
  if (oldValue) throw new Error('Already exists')
})

exports.tableDb = tableDb
