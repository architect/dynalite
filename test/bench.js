var helpers = require('./helpers')

describe.skip('benchmarks', function () {

  it('should batch write', function (done) {
    this.timeout(1e6)

    var numItems = 1e6, numSegments = 4, start = Date.now(), i, items = new Array(numItems)

    for (i = 0; i < numItems; i++)
      items[i] = { a: { S: String(i) } }

    helpers.batchBulkPut(helpers.testHashTable, items, numSegments, function (err) {
      if (err) return done(err)


      console.log('batchBulkPut: %dms, %d items/sec', Date.now() - start, 1000 * numItems / (Date.now() - start))

      done()
    })
  })

  it('should scan', function (done) {
    this.timeout(1e6)

    scan()

    function scan (key) {
      var start = Date.now()

      helpers.request(helpers.opts('Scan', { TableName: helpers.testHashTable, Limit: 1000, ExclusiveStartKey: key }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)


        console.log('Scan: %d items, %dms, %d items/sec, %s', res.body.Count, Date.now() - start,
          1000 * res.body.Count / (Date.now() - start), JSON.stringify(res.body.LastEvaluatedKey))

        if (res.body.LastEvaluatedKey)
          return scan(res.body.LastEvaluatedKey)

        done()
      })
    }
  })
})
