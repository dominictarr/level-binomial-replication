var update = require('level-update-stream')
var replicate = require('binomial-hash-list')
var pull = require('pull-stream')

module.exports = function (db, index, opts) {
  if(!opts) opts = index, index = null
  index = index || db.sublevel('search')
  opts = opts || {}

  if('function' !== typeof index.search)
    throw new Error('level-search api was not detected!')

  var ts = opts.ts || 'ts'

  return function (cb) {
    return replicate({
      read: function (opts) {
        opts = opts || {}
        var range = {
          min: opts.gte ? new Date(opts.gte) : new Date(0),
          max: opts.lt  ? new Date(opts.lt)  : new Date('9999-12-31')
        }
        return pull(
          index.search([ts, range]),
          pull.through(function (e) {
            delete e.index
          })
        )
      },
      write: function (cb) {
        return update(db, cb)
      },
      ts: function (o) {
        return new Date(o.value[ts])
      },
      size: opts.size || 24*60*60*1000,
      server: opts.server
    }, cb)
  }
}

