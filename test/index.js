var tape = require('tape')

var sublevel = require('level-sublevel')
var level = require('level-test')()
var search = require('level-search')

var pull = require('pull-stream')
var pl = require('pull-level')
var ranges = require('binomial-hash-list/ranges')
var reduce = require('binomial-hash-list/reduce')

var db1 = level('level-binomial-replicate1')
var index1 = search(db1)

var db2 = level('level-binomial-replicate1')
var index2 = search(db2)

tape('load-db', function (t) {

  var start = new Date('2000-01-01'), rolling = +start
  var total = 10000
  var gap = (Date.now() - +start)/(total/2)
  pull(
    pull.count(total),
    pull.map(function (e) {
      return {
        key: '*'+Math.random(),
        value: {
          count: e,
          ts: new Date(rolling += Math.random()*gap).toISOString()
        }
      }
    }),
/*
    ranges(24*60*60*1000, function (e) { return +new Date(e.value.ts) }),
    reduce(function (err, tree) {
      if(err) throw err
      console.log(tree)
      t.end()
    })
*/
    pl.write(db1, function (err) {
      if(err) throw err
      t.end()
    })
  )
})

tape('replicate', function (t) {

  var createStream1 = replicate(db1)
  var createStream2 = replicate(db2)

  var a = createStream1(function (err, written) {
    if(err) throw err
    console.log('A - done', written)
  })
  var b = createStream2(function (err, written) {
    if(err) throw err
    console.log('b - done', written)
  })

  pull(a, b, a)

})

