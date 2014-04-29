var create = require('./')
var duplex = require('pull-stream-to-stream')
var serialize = require('stream-serializer')()

module.exports = function (db, index, opts) {
  var createStream = create(db, index, opts)

  return function (opts, cb) {
    var stream = createStream(opts, cb)
    return serialize(duplex(stream.sink, stream.source))
  }
}
