var async = require('async'),
    helpers = require('./helpers')

var target = 'ListStreams',
    request = helpers.request,
    randomName = helpers.randomName,
    opts = helpers.opts.bind(null, target)

describe('listStreams', function() {

  describe('serializations', function() {
  })

  describe('validations', function() {
  })

  describe('functionality', function() {

    it('should return empty array if no streams are present', function(done) {
      request(opts({}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.Streams.should.be.Array()
        res.body.Streams.should.be.empty
        done()
      })
    })

    it('should return all streams if no table name is provided', function(done) {
      var tableTemplate = {
            AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}],
            KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}],
            ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1},
            StreamSpecification: {StreamEnabled: true, StreamViewType: 'NEW_AND_OLD_IMAGES'},
          },
          table1 = {TableName: randomName()},
          table2 = {TableName: randomName()}

      for (var key in tableTemplate) {
        table1[key] = tableTemplate[key]
        table2[key] = tableTemplate[key]
      }

      async.parallel([
        request.bind(null, helpers.opts('CreateTable', table1)),
        request.bind(null, helpers.opts('CreateTable', table2)),
      ], function(err, res) {
        if (err) return done(err)
        res.should.have.length(2)

        res[0].statusCode.should.be.equal(200)
        res[0].body.TableDescription.LatestStreamArn.should.not.be.empty
        res[0].body.TableDescription.LatestStreamLabel.should.not.be.empty
        var streamArn1 = res[0].body.TableDescription.LatestStreamArn,
            streamLabel1 = res[0].body.TableDescription.LatestStreamLabel

        res[1].statusCode.should.be.equal(200)
        res[1].body.TableDescription.LatestStreamArn.should.not.be.empty
        var streamArn2 = res[1].body.TableDescription.LatestStreamArn,
            streamLabel2 = res[1].body.TableDescription.LatestStreamLabel

        async.parallel([
          helpers.waitUntilActive.bind(null, table1.TableName),
          helpers.waitUntilActive.bind(null, table2.TableName),
        ], function(err) {
          if (err) return done(err)

          request(opts({}), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)

            // DynamoDB saves streams for 24h and it's impossible to force delete them
            // therefore we should expect to receive old streams as well
            res.body.Streams.should.be.Array()
            res.body.Streams.length.should.be.aboveOrEqual(2)

            res.body.Streams.should.containEql({StreamArn: streamArn1, StreamLabel: streamLabel1, TableName: table1.TableName})
            res.body.Streams.should.containEql({StreamArn: streamArn2, StreamLabel: streamLabel2, TableName: table2.TableName})

            done()
          })
        })
      })
    })

  })

})
