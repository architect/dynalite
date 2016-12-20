var helpers = require('./helpers')

var target = 'GetShardIterator',
    request = helpers.request,
    randomName = helpers.randomName,
    opts = helpers.opts.bind(null, target)

describe('getShardIterator', function() {

  describe('serializations', function() {
  })

  describe('validations', function() {
  })

  describe('functionality', function() {

    it('should return shard iterator', function(done) {
      var table = {
        TableName: randomName(),
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}],
        KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1},
        StreamSpecification: {StreamEnabled: true, StreamViewType: 'NEW_AND_OLD_IMAGES'},
      }

      request(helpers.opts('CreateTable', table), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.be.equal(200)

        var streamArn = res.body.TableDescription.LatestStreamArn

        helpers.waitUntilActive(table.TableName, function(err) {
          if (err) return done(err)

          request(helpers.opts('DescribeStream', {StreamArn: streamArn}), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.be.equal(200)
            res.body.StreamDescription.Shards[0].ShardId.should.not.be.empty

            var req = {
              StreamArn: streamArn,
              ShardId: res.body.StreamDescription.Shards[0].ShardId,
              ShardIteratorType: 'TRIM_HORIZON',
            }
            request(opts(req), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.be.equal(200)
              res.body.ShardIterator.should.not.be.empty

              helpers.deleteWhenActive(table.TableName, done)
            })
          })
        })
      })
    })

  })

})
