var pull = require('pull-stream')

require('./') (
  require('../stream'),
  function (a, b) {
    a.pipe(b).pipe(a)
  }
)
