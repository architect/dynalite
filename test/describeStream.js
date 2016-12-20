var helpers = require('./helpers')

var target = 'DescribeStream',
    request = helpers.request,
    randomName = helpers.randomName,
    opts = helpers.opts.bind(null, target)

describe('describeStream', function() {

  describe('serializations', function() {
  })

  describe('validations', function() {
  })

  describe('functionality', function() {

    it('should return stream description', function(done) {
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
        res.body.TableDescription.LatestStreamArn.should.not.be.empty
        res.body.TableDescription.LatestStreamLabel.should.not.be.empty

        var streamArn = res.body.TableDescription.LatestStreamArn,
            streamLabel = res.body.TableDescription.LatestStreamLabel

        helpers.waitUntilActive(table.TableName, function(err) {
          if (err) return done(err)

          request(opts({StreamArn: streamArn}), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.be.equal(200)
            res.body.StreamDescription.CreationRequestDateTime.should.not.be.empty
            res.body.StreamDescription.KeySchema.should.be.Array()
            res.body.StreamDescription.KeySchema.should.have.length(1)
            res.body.StreamDescription.KeySchema[0].AttributeName.should.be.eql(table.KeySchema[0].AttributeName)
            res.body.StreamDescription.KeySchema[0].KeyType.should.be.eql(table.KeySchema[0].KeyType)
            res.body.StreamDescription.StreamArn.should.be.equal(streamArn)
            res.body.StreamDescription.StreamLabel.should.be.equal(streamLabel)
            res.body.StreamDescription.StreamStatus.should.be.equal('ENABLED')
            res.body.StreamDescription.StreamViewType.should.be.equal(table.StreamSpecification.StreamViewType)
            res.body.StreamDescription.TableName.should.be.equal(table.TableName)

            // This is newly created stream, so it's very unlikely it would have more than one shard
            res.body.StreamDescription.Shards.should.be.Array()
            res.body.StreamDescription.Shards.should.have.length(1)
            res.body.StreamDescription.Shards[0].should.not.have.property('ParentShardId')
            res.body.StreamDescription.Shards[0].ShardId.should.not.be.empty
            res.body.StreamDescription.Shards[0].should.have.property('SequenceNumberRange')
            res.body.StreamDescription.Shards[0].SequenceNumberRange.StartingSequenceNumber.should.not.be.empty
            res.body.StreamDescription.Shards[0].SequenceNumberRange.should.not.have.property('EndingSequenceNumber')

            helpers.deleteWhenActive(table.TableName, done)
          })
        })
      })
    })

  })

})
