var tape = require('tape')
var deepEqual = require('deep-equal')

var sub = require('level-sublevel')
var level = require('level-test')()
var search = require('level-search')

var pull = require('pull-stream')
var pl = require('pull-level')

var ranges = require('binomial-hash-list/ranges')
var reduce = require('binomial-hash-list/reduce')

var createReplicateTests = module.exports = function (replicate, connect) {

  var _db = level('level-binomial-replicate1', {encoding: 'json'})
  var db1 = sub(_db)

  console.log(_db)
  var index1 = search(db1)

  var db2 = sub(level('level-binomial-replicate2', {encoding: 'json'}))
  var index2 = search(db2)


  tape('load-db', function (t) {

    var start = new Date('2000-01-01'), rolling = +start
    var total = 1000
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

  tape('search', function (t) {
    var start = new Date('2002-01-01'), end = new Date('2002-02-01')
    pull(
      index1.search(['ts', {min: start, max: end}]),
      pull.collect(function (err, ary) {
        if(err) throw err
        ary.forEach(function (e) {
          t.ok(new Date(e.value.ts) >= start)
          t.ok(new Date(e.value.ts) <  end)
        })
        t.end()
      })
    )
  })

  tape('binomial', function (t) {
    console.log('replicate?', replicate)
    replicate(db1).binomial(function (err, tree) {
      if(err) throw err
      console.log(tree)
      t.ok(tree)
      t.ok(Array.isArray(tree.tree))
      t.ok(tree.tree.length > 0)

      t.end()
    })
  })

  function missing (a, b) {
    var o = {}, m = []
    a.forEach(function (e) {
      o[e.key] = e
    })
    b.forEach(function (b) {
      if(!o[b.key]) m.push(b)
      else if(!deepEqual(o[b.key], b))
        m.push({found: b, expected: o[b.key]})
    })
    return m
  }

  function testReplicate(db1, db2, cb) {
    var n = 2
    var a = replicate(db1) ({server: true}, function (err, written) {
      if(err) throw err
      console.log('A - done', written)
      done()
    })
    var b = replicate(db2) (function (err, written) {
      if(err) throw err
      console.log('b - done', written)
      done()
    })

    connect(a, b)

    function done () {
      if(--n) return
      var A, B
      pull(pl.read(db1), pull.collect(function (err, ary) { A = ary; next() }))
      pull(pl.read(db2), pull.collect(function (err, ary) { B = ary; next() }))
      function next () {
        if(!(A&&B)) return
        cb(null, A, B)
      }
    }

  }

  tape('replicate', function (t) {
    testReplicate(db1, db2, function (err, A, B) {
      t.deepEqual(A, B)
      t.equal(A.length, B.length)
      console.log(missing(B, A))
      t.end()
    })
  })

  var MUTATE
  tape('mutate', function (t) {
    pull(
      index2.search(['ts', {max: new Date(), min: new Date(0)}], {reverse: true, limit: 10}),
      pull.collect(function (err, ary) {
        console.log(ary)
        var mutate = MUTATE = ary[~~(Math.random()*ary.length)]

        mutate.value.foo = Math.random()
        db1.put(mutate.key, mutate.value, function (err) {
          if(err) throw err
          pull(
            db1.sublevel('search').search(['ts', mutate.value.ts]),
            pull.collect(function (err, value) {
              if(err) throw err
              console.log(value)
              t.deepEqual(value[0], mutate)
              t.end()
            })
          )
        })
      })
    )
  })

  tape('replicate2', function (t) {
    testReplicate(db1, db2, function (err, A, B) {
      t.equal(A.length, B.length)

      console.log(JSON.stringify(missing(B, A), null, 2))
      db2.get(MUTATE.key, function (err, value) {
        console.log(value)
        t.deepEqual(value, MUTATE.value)
        t.end()
      })
    })
  })

}

if(!module.parent)
  createReplicateTests(
    require('../'),
    function (a, b) {
      pull(a,
        pull.through(function (e) {
          console.log('a->b', JSON.stringify(e))
        }),
        b,
        pull.through(function (e) {
          console.log('b->a', JSON.stringify(e))
        }),
        a)
    }
  )

