var update = require('level-update-stream')

module.exports = function (db, index, opts) {
  index = index || db.sublevel('search')
  opts = opts || {}

  if('function' !== typeof index.search)
    throw new Error('level-search api was not detected!')

  var ts = opts.ts || 'ts'

  return function (cb) {
    return replicate({
      read: function (opts) {
        return pull(
          index.search([ts {min: new Date(opts.gte), max: new Date(opts.lt)}]),
          pull.through(function (e) {
            delete e.index
          })
        )
      },
      write: function (cb) {
        return update(db, cb)
      },
      ts: function (o) {
        return o[ts]
      },
      size: opts.size || 24*60*60*1000
    })
  }
}

