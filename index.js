var update = require('level-update-stream')
var replicate = require('binomial-hash-list')
var pull = require('pull-stream')

function merge(a, b) {
  for(var k in b)
    a[k] = b[k]
  return a
}

module.exports = function (db, index, opts) {
  if(!opts) opts = index, index = null
  index = index || db.sublevel('search')
  opts = opts || {}

  if('function' !== typeof index.search)
    throw new Error('level-search api was not detected!')

  var ts = opts.ts || 'ts'

  function getOpts (_opts) {
    return {
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
      server: _opts.server
    }
  }

  function createStream (_opts, cb) {
    if(!cb) cb = _opts, _opts = {}
    _opts = merge(merge({}, opts), _opts)
    return replicate(getOpts(_opts), cb)
  }

  createStream.binomial = function (_opts, cb) {
    if(!cb) cb = _opts, _opts = {}
    replicate.binomial(getOpts(_opts), cb)
  }

  return createStream
}

