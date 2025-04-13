const async = require('async')
const { request, opts } = require('./request')

function clearTable (name, keyNames, segments, done) {
  if (typeof segments === 'function') { done = segments; segments = 2 }
  if (!Array.isArray(keyNames)) keyNames = [ keyNames ]

  scanAndDelete(done)

  function scanAndDelete (cb) {
    async.times(segments, scanSegmentAndDelete, (err, segmentsHadKeys) => {
      if (err) return cb(err)
      // If any segment had keys, we need to scan again
      if (segmentsHadKeys.some(Boolean)) return setTimeout(() => scanAndDelete(cb), 100) // Add slight delay
      cb()
    })
  }

  function scanSegmentAndDelete (n, cb) {
    request(opts('Scan', { TableName: name, AttributesToGet: keyNames, Segment: n, TotalSegments: segments }), (err, res) => {
      if (err) return cb(err)
      if (res.body && /ProvisionedThroughputExceededException/.test(res.body.__type)) {
        console.log(`ProvisionedThroughputExceededException during clearTable Scan (segment ${n})`) // eslint-disable-line no-console
        return setTimeout(scanSegmentAndDelete, 2000, n, cb)
      }
      else if (res.statusCode != 200) {
        return cb(new Error(`${res.statusCode}: ${JSON.stringify(res.body)}`))
      }
      if (!res.body.Count) return cb(null, false) // Use Count, ScannedCount might be > 0 even if no items match filter

      const keys = res.body.Items
      if (!keys || keys.length === 0) return cb(null, false)

      let batchDeletes = []
      for (let i = 0; i < keys.length; i += 25) {
        batchDeletes.push(batchWriteUntilDone.bind(null, name, { deletes: keys.slice(i, i + 25) }))
      }

      async.parallelLimit(batchDeletes, 10, (err) => { // Limit concurrency
        if (err) return cb(err)
        // Return true indicating keys were found and deleted in this segment scan
        // Also check LastEvaluatedKey for pagination in future if needed
        cb(null, true)
      })
    })
  }
}

function replaceTable (name, keyNames, items, segments, done) {
  if (typeof segments === 'function') { done = segments; segments = 2 }

  clearTable(name, keyNames, segments, (err) => {
    if (err) return done(err)
    batchBulkPut(name, items, segments, done)
  })
}

function batchBulkPut (name, items, segments, done) {
  if (typeof segments === 'function') { done = segments; segments = 2 }

  let itemChunks = []
  for (let i = 0; i < items.length; i += 25) {
    itemChunks.push(items.slice(i, i + 25))
  }

  async.eachLimit(itemChunks, segments * 2, (chunk, cb) => { // Increase limit slightly for puts
    batchWriteUntilDone(name, { puts: chunk }, cb)
  }, done)
}

function batchWriteUntilDone (name, actions, cb) {
  let batchReq = { RequestItems: {} }
  batchReq.RequestItems[name] = (actions.puts || []).map((item) => ({ PutRequest: { Item: item } }))
    .concat((actions.deletes || []).map((key) => ({ DeleteRequest: { Key: key } })))

  if (batchReq.RequestItems[name].length === 0) {
    return cb() // No items to process
  }

  let batchRes = {}

  async.doWhilst(
    (callback) => {
      request(opts('BatchWriteItem', batchReq), (err, res) => {
        if (err) return callback(err)
        batchRes = res

        // Check for unprocessed items first
        if (res.body.UnprocessedItems && Object.keys(res.body.UnprocessedItems).length > 0 && res.body.UnprocessedItems[name]) {
          batchReq.RequestItems = { [name]: res.body.UnprocessedItems[name] } // Prepare only unprocessed for retry
          // console.log(`Retrying ${batchReq.RequestItems[name].length} unprocessed items for ${name}`);
          return setTimeout(callback, 1000 + Math.random() * 1000) // Delay before retry
        }

        // Then check for throughput exceptions
        if (res.body && /ProvisionedThroughputExceededException/.test(res.body.__type)) {
          console.log('ProvisionedThroughputExceededException during BatchWrite') // eslint-disable-line no-console
          // Keep the same batchReq for retry on throughput error
          return setTimeout(callback, 2000 + Math.random() * 1000) // Longer delay
        }

        // Check for other errors
        if (res.statusCode != 200) {
          return callback(new Error(`${res.statusCode}: ${JSON.stringify(res.body)}`))
        }

        // Success or no unprocessed items/throughput errors
        batchReq.RequestItems = {} // Clear items if successful or no unprocessed
        callback()
      })
    },
    (checkCallback) => {
      // Continue while there are items left in batchReq to process
      const shouldContinue = batchReq.RequestItems && batchReq.RequestItems[name] && batchReq.RequestItems[name].length > 0
      checkCallback(null, shouldContinue)
    },
    cb // Final callback when done
  )
}

module.exports = {
  clearTable,
  replaceTable,
  batchBulkPut,
  batchWriteUntilDone,
}
