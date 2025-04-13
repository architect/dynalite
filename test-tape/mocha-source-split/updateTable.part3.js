var helpers = require('./helpers')

var target = 'UpdateTable',
  request = helpers.request,
  opts = helpers.opts.bind(null, target),
  assertType = helpers.assertType.bind(null, target),
  assertValidation = helpers.assertValidation.bind(null, target),
  assertNotFound = helpers.assertNotFound.bind(null, target)

describe('updateTable', function () {
  describe('functionality', function () {

    it('should triple rates and then reduce if requested', function (done) {
      this.timeout(200000)
      var oldRead = helpers.readCapacity, oldWrite = helpers.writeCapacity,
        newRead = oldRead * 3, newWrite = oldWrite * 3, increase = Date.now() / 1000,
        throughput = { ReadCapacityUnits: newRead, WriteCapacityUnits: newWrite }
      request(opts({ TableName: helpers.testHashTable, ProvisionedThroughput: throughput }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        var desc = res.body.TableDescription
        desc.AttributeDefinitions.should.eql([ { AttributeName: 'a', AttributeType: 'S' } ])
        desc.CreationDateTime.should.be.below(Date.now() / 1000)
        desc.ItemCount.should.be.above(-1)
        desc.KeySchema.should.eql([ { AttributeName: 'a', KeyType: 'HASH' } ])
        desc.ProvisionedThroughput.LastIncreaseDateTime.should.be.above(increase - 5)
        desc.ProvisionedThroughput.NumberOfDecreasesToday.should.be.above(-1)
        desc.ProvisionedThroughput.ReadCapacityUnits.should.equal(oldRead)
        desc.ProvisionedThroughput.WriteCapacityUnits.should.equal(oldWrite)
        desc.TableName.should.equal(helpers.testHashTable)
        desc.TableSizeBytes.should.be.above(-1)
        desc.TableStatus.should.equal('UPDATING')

        var numDecreases = desc.ProvisionedThroughput.NumberOfDecreasesToday
        increase = desc.ProvisionedThroughput.LastIncreaseDateTime

        helpers.waitUntilActive(helpers.testHashTable, function (err, res) {
          if (err) return done(err)

          var decrease = Date.now() / 1000
          desc = res.body.Table
          desc.ProvisionedThroughput.ReadCapacityUnits.should.equal(newRead)
          desc.ProvisionedThroughput.WriteCapacityUnits.should.equal(newWrite)
          desc.ProvisionedThroughput.LastIncreaseDateTime.should.be.above(increase)

          increase = desc.ProvisionedThroughput.LastIncreaseDateTime

          throughput = { ReadCapacityUnits: oldRead, WriteCapacityUnits: oldWrite }
          request(opts({ TableName: helpers.testHashTable, ProvisionedThroughput: throughput }), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)

            desc = res.body.TableDescription
            desc.ProvisionedThroughput.LastIncreaseDateTime.should.equal(increase)
            desc.ProvisionedThroughput.LastDecreaseDateTime.should.be.above(decrease - 5)
            desc.ProvisionedThroughput.NumberOfDecreasesToday.should.equal(numDecreases)
            desc.ProvisionedThroughput.ReadCapacityUnits.should.equal(newRead)
            desc.ProvisionedThroughput.WriteCapacityUnits.should.equal(newWrite)
            desc.TableStatus.should.equal('UPDATING')

            decrease = desc.ProvisionedThroughput.LastDecreaseDateTime

            helpers.waitUntilActive(helpers.testHashTable, function (err, res) {
              if (err) return done(err)

              desc = res.body.Table
              desc.ProvisionedThroughput.LastIncreaseDateTime.should.equal(increase)
              desc.ProvisionedThroughput.LastDecreaseDateTime.should.be.above(decrease)
              desc.ProvisionedThroughput.NumberOfDecreasesToday.should.equal(numDecreases + 1)
              desc.ProvisionedThroughput.ReadCapacityUnits.should.equal(oldRead)
              desc.ProvisionedThroughput.WriteCapacityUnits.should.equal(oldWrite)

              done()
            })
          })
        })
      })
    })

    // XXX: this takes more than 20 mins to run
    it.skip('should allow table to be converted to PAY_PER_REQUEST and back again', function (done) {
      this.timeout(1500000)
      var read = helpers.readCapacity, write = helpers.writeCapacity,
        throughput = { ReadCapacityUnits: read, WriteCapacityUnits: write }, decrease = Date.now() / 1000
      request(opts({ TableName: helpers.testRangeTable, BillingMode: 'PAY_PER_REQUEST' }), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        var desc = res.body.TableDescription
        desc.TableStatus.should.equal('UPDATING')
        desc.BillingModeSummary.should.eql({ BillingMode: 'PAY_PER_REQUEST' })
        desc.TableThroughputModeSummary.should.eql({ TableThroughputMode: 'PAY_PER_REQUEST' })
        desc.ProvisionedThroughput.LastDecreaseDateTime.should.be.above(decrease - 5)
        desc.ProvisionedThroughput.NumberOfDecreasesToday.should.be.above(-1)
        desc.ProvisionedThroughput.ReadCapacityUnits.should.equal(0)
        desc.ProvisionedThroughput.WriteCapacityUnits.should.equal(0)

        desc.GlobalSecondaryIndexes.forEach(function (index) {
          index.IndexStatus.should.equal('UPDATING')
          index.ProvisionedThroughput.should.eql({
            NumberOfDecreasesToday: 0,
            ReadCapacityUnits: 0,
            WriteCapacityUnits: 0,
          })
        })

        helpers.waitUntilActive(helpers.testRangeTable, function (err, res) {
          if (err) return done(err)

          var desc = res.body.Table
          desc.BillingModeSummary.BillingMode.should.equal('PAY_PER_REQUEST')
          desc.BillingModeSummary.LastUpdateToPayPerRequestDateTime.should.be.above(decrease - 5)
          desc.TableThroughputModeSummary.TableThroughputMode.should.equal('PAY_PER_REQUEST')
          desc.TableThroughputModeSummary.LastUpdateToPayPerRequestDateTime.should.be.above(decrease - 5)
          desc.ProvisionedThroughput.NumberOfDecreasesToday.should.be.above(-1)
          desc.ProvisionedThroughput.ReadCapacityUnits.should.equal(0)
          desc.ProvisionedThroughput.WriteCapacityUnits.should.equal(0)
          desc.GlobalSecondaryIndexes.forEach(function (index) {
            index.ProvisionedThroughput.LastDecreaseDateTime.should.be.above(decrease - 5)
            index.ProvisionedThroughput.NumberOfDecreasesToday.should.be.above(0)
            index.ProvisionedThroughput.ReadCapacityUnits.should.equal(0)
            index.ProvisionedThroughput.WriteCapacityUnits.should.equal(0)
          })

          assertValidation({ TableName: helpers.testRangeTable, BillingMode: 'PROVISIONED', ProvisionedThroughput: throughput },
            'One or more parameter values were invalid: ' +
              'ProvisionedThroughput must be specified for index: index3,index4', function (err) {
              if (err) return done(err)

              request(opts({
                TableName: helpers.testRangeTable,
                BillingMode: 'PROVISIONED',
                ProvisionedThroughput: throughput,
                GlobalSecondaryIndexUpdates: [ {
                  Update: {
                    IndexName: 'index3',
                    ProvisionedThroughput: throughput,
                  },
                }, {
                  Update: {
                    IndexName: 'index4',
                    ProvisionedThroughput: throughput,
                  },
                } ],
              }), function (err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)

                var desc = res.body.TableDescription
                desc.TableStatus.should.equal('UPDATING')
                desc.BillingModeSummary.BillingMode.should.equal('PROVISIONED')
                desc.BillingModeSummary.LastUpdateToPayPerRequestDateTime.should.be.above(decrease - 5)
                desc.TableThroughputModeSummary.TableThroughputMode.should.equal('PROVISIONED')
                desc.TableThroughputModeSummary.LastUpdateToPayPerRequestDateTime.should.be.above(decrease - 5)
                desc.ProvisionedThroughput.NumberOfDecreasesToday.should.be.above(-1)
                desc.ProvisionedThroughput.ReadCapacityUnits.should.equal(read)
                desc.ProvisionedThroughput.WriteCapacityUnits.should.equal(write)

                desc.GlobalSecondaryIndexes.forEach(function (index) {
                  index.IndexStatus.should.equal('UPDATING')
                  index.ProvisionedThroughput.LastDecreaseDateTime.should.be.above(decrease - 5)
                  index.ProvisionedThroughput.NumberOfDecreasesToday.should.be.above(0)
                  index.ProvisionedThroughput.ReadCapacityUnits.should.equal(read)
                  index.ProvisionedThroughput.WriteCapacityUnits.should.equal(write)
                })

                helpers.waitUntilActive(helpers.testRangeTable, function (err, res) {
                  if (err) return done(err)

                  var desc = res.body.Table
                  desc.BillingModeSummary.BillingMode.should.equal('PROVISIONED')
                  desc.BillingModeSummary.LastUpdateToPayPerRequestDateTime.should.be.above(decrease - 5)
                  desc.TableThroughputModeSummary.TableThroughputMode.should.equal('PROVISIONED')
                  desc.TableThroughputModeSummary.LastUpdateToPayPerRequestDateTime.should.be.above(decrease - 5)
                  desc.ProvisionedThroughput.NumberOfDecreasesToday.should.be.above(-1)
                  desc.ProvisionedThroughput.ReadCapacityUnits.should.equal(read)
                  desc.ProvisionedThroughput.WriteCapacityUnits.should.equal(write)

                  desc.GlobalSecondaryIndexes.forEach(function (index) {
                    index.ProvisionedThroughput.LastDecreaseDateTime.should.be.above(decrease - 5)
                    index.ProvisionedThroughput.NumberOfDecreasesToday.should.be.above(0)
                    index.ProvisionedThroughput.ReadCapacityUnits.should.equal(read)
                    index.ProvisionedThroughput.WriteCapacityUnits.should.equal(write)
                  })

                  done()
                })
              })
            })
        })
      })
    })

  })
})