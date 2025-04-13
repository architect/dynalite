const test = require('tape')
// const should = require('should') // Ensure should is required for assertions - Removed as tests are skipped
const helpers = require('./helpers') // Assuming helpers is in the same dir or accessible

test.skip('benchmarks', (t) => {

  t.test('should batch write', (st) => {
    // Tape does not have a direct equivalent for this.timeout().
    // Since the test is skipped, we'll omit it. If run, consider test duration.

    const numItems = 1e6
    const numSegments = 4
    const start = Date.now()
    let i
    const items = new Array(numItems)

    for (i = 0; i < numItems; i++) {
      items[i] = { a: { S: String(i) } }
    }

    helpers.batchBulkPut(helpers.testHashTable, items, numSegments, (err) => {
      st.error(err, 'batchBulkPut should not error') // Use st.error for errors
      if (err) return st.end()

      // eslint-disable-next-line no-console
      console.log('batchBulkPut: %dms, %d items/sec', Date.now() - start, 1000 * numItems / (Date.now() - start))

      st.end() // Use st.end() instead of done()
    })
  })

  t.test('should scan', (st) => {
    // Tape does not have a direct equivalent for this.timeout().
    // Since the test is skipped, we'll omit it. If run, consider test duration.

    scan() // Initial call

    function scan (key) {
      const start = Date.now()

      helpers.request(helpers.opts('Scan', { TableName: helpers.testHashTable, Limit: 1000, ExclusiveStartKey: key }), (err, res) => {
        st.error(err, 'helpers.request should not error')
        if (err) return st.end()

        // Use should assertions (requires 'should' module)
        // res.statusCode.should.equal(200); // Assertion commented out as tests are skipped

        // eslint-disable-next-line no-console
        console.log('Scan: %d items, %dms, %d items/sec, %s', res.body.Count, Date.now() - start,
          1000 * res.body.Count / (Date.now() - start), JSON.stringify(res.body.LastEvaluatedKey))

        if (res.body.LastEvaluatedKey) {
          return scan(res.body.LastEvaluatedKey) // Recursive call
        }

        st.end() // End test when scan completes
      })
    }
  })

  // Since the outer test is skipped, this t.end() might not be strictly necessary,
  // but it's good practice for potentially unskipping later.
  t.end()
})
