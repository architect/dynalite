const config = require('./config')

function randomString () {
  return ('AAAAAAAAA' + randomNumber()).slice(-10)
}

function randomNumber () {
  return String(Math.random() * 0x100000000)
}

function randomName () {
  return config.prefix + randomString()
}

module.exports = {
  randomString,
  randomNumber,
  randomName,
}
