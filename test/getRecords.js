var helpers = require('./helpers')

var target = 'GetRecords',
    request = helpers.request,
    randomName = helpers.randomName,
    opts = helpers.opts.bind(null, target)

var tableName = randomName(),
    streamViewType = 'NEW_AND_OLD_IMAGES',
    streamArn, shardId

describe('getRecords', function() {

  describe('serializations', function() {
  })

  describe('validations', function() {
  })

  describe('functionality', function() {
    before(function(done) {
      var table = {
        TableName: tableName,
        AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}],
        KeySchema: [{AttributeName: 'a', KeyType: 'HASH'}],
        ProvisionedThroughput: {ReadCapacityUnits: 1, WriteCapacityUnits: 1},
        StreamSpecification: {StreamEnabled: true, StreamViewType: streamViewType},
      }

      request(helpers.opts('CreateTable', table), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.be.equal(200)

        streamArn = res.body.TableDescription.LatestStreamArn

        helpers.waitUntilActive(tableName, function(err) {
          if (err) return done(err)

          var itemKey1 = {S: 'foo'}, item = {
            a: itemKey1,
            val: {S: 'bar'},
            other: {S: 'baz'},
          }
          request(helpers.opts('PutItem', {TableName: tableName, Item: item}), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.be.equal(200)

            var item = {
              a: {S: 'meaning-of-life'},
              answer: {N: '42'},
            }
            request(helpers.opts('PutItem', {TableName: tableName, Item: item}), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.be.equal(200)

              var req = {
                TableName: tableName,
                Key: {a: itemKey1},
                UpdateExpression: 'SET #ATT = :VAL',
                ExpressionAttributeNames: {'#ATT': 'val'},
                ExpressionAttributeValues: {':VAL': {S: 'New value'}},
              }
              request(helpers.opts('UpdateItem', req), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.be.equal(200)

                var req = {
                  TableName: tableName,
                  Key: {a: itemKey1},
                }
                request(helpers.opts('DeleteItem', req), function(err, res) {
                  if (err) return done(err)
                  res.statusCode.should.be.equal(200)

                  request(helpers.opts('DescribeStream', {StreamArn: streamArn}), function(err, res) {
                    if (err) return done(err)
                    res.statusCode.should.be.equal(200)

                    shardId = res.body.StreamDescription.Shards[0].ShardId

                    done()
                  })
                })
              })
            })
          })
        })
      })
    })

    after(function(done) {
      helpers.deleteWhenActive(tableName, done)
    })

    it('should return all stream records', function(done) {
      var req = {
        StreamArn: streamArn,
        ShardId: shardId,
        ShardIteratorType: 'TRIM_HORIZON',
      }
      request(helpers.opts('GetShardIterator', req), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.be.equal(200)

        var iterator = res.body.ShardIterator
        request(opts({ShardIterator: iterator}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.be.equal(200)

          res.body.NextShardIterator.should.not.be.empty
          res.body.Records.should.be.Array()
          res.body.Records.should.have.length(4)

          var lastSequenceNumber = 0
          for (var i = 0; i < 4; i++) {
            res.body.Records[i].awsRegion.should.be.equal(helpers.awsRegion)
            res.body.Records[i].eventID.should.not.be.empty
            res.body.Records[i].eventSource.should.be.equal('aws:dynamodb')
            res.body.Records[i].eventVersion.should.be.equal('1.1')

            res.body.Records[i].dynamodb.ApproximateCreationDateTime.should.not.be.empty
            res.body.Records[i].dynamodb.SequenceNumber.should.be.above(lastSequenceNumber)
            res.body.Records[i].dynamodb.SizeBytes.should.be.above(0)
            res.body.Records[i].dynamodb.StreamViewType.should.be.equal(streamViewType)

            lastSequenceNumber = res.body.Records[i].dynamodb.SequenceNumber
          }

          res.body.Records[0].eventName.should.be.equal('INSERT')
          res.body.Records[0].dynamodb.Keys.should.be.eql({a: {S: 'foo'}})
          res.body.Records[0].dynamodb.should.not.have.property('OldImage')
          res.body.Records[0].dynamodb.NewImage.should.be.eql({a: {S: 'foo'}, val: {S: 'bar'}, other: {S: 'baz'}})

          res.body.Records[1].eventName.should.be.equal('INSERT')
          res.body.Records[1].dynamodb.Keys.should.be.eql({a: {S: 'meaning-of-life'}})
          res.body.Records[1].dynamodb.should.not.have.property('OldImage')
          res.body.Records[1].dynamodb.NewImage.should.be.eql({a: {S: 'meaning-of-life'}, answer: {N: '42'}})

          res.body.Records[2].eventName.should.be.equal('MODIFY')
          res.body.Records[2].dynamodb.Keys.should.be.eql({a: {S: 'foo'}})
          res.body.Records[2].dynamodb.OldImage.should.be.eql({a: {S: 'foo'}, val: {S: 'bar'}, other: {S: 'baz'}})
          res.body.Records[2].dynamodb.NewImage.should.be.eql({a: {S: 'foo'}, val: {S: 'New value'}, other: {S: 'baz'}})

          res.body.Records[3].eventName.should.be.equal('REMOVE')
          res.body.Records[3].dynamodb.Keys.should.be.eql({a: {S: 'foo'}})
          res.body.Records[3].dynamodb.OldImage.should.be.eql({a: {S: 'foo'}, val: {S: 'New value'}, other: {S: 'baz'}})
          res.body.Records[3].dynamodb.should.not.have.property('NewImage')

          done()
        })
      })
    })

    it('should return respect limit and return valid next iterator', function(done) {
      var req = {
        StreamArn: streamArn,
        ShardId: shardId,
        ShardIteratorType: 'TRIM_HORIZON',
      }
      request(helpers.opts('GetShardIterator', req), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.be.equal(200)

        var iterator = res.body.ShardIterator
        request(opts({ShardIterator: iterator, Limit: 2}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.be.equal(200)

          res.body.NextShardIterator.should.not.be.empty
          res.body.Records.should.be.Array()
          res.body.Records.should.have.length(2)

          var lastSequenceNumber = 0
          for (var i = 0; i < 2; i++) {
            res.body.Records[i].awsRegion.should.be.equal(helpers.awsRegion)
            res.body.Records[i].eventID.should.not.be.empty
            res.body.Records[i].eventName.should.be.equal('INSERT')
            res.body.Records[i].eventSource.should.be.equal('aws:dynamodb')
            res.body.Records[i].eventVersion.should.be.equal('1.1')

            res.body.Records[i].dynamodb.should.not.have.property('OldImage')
            res.body.Records[i].dynamodb.ApproximateCreationDateTime.should.not.be.empty
            res.body.Records[i].dynamodb.SequenceNumber.should.be.above(lastSequenceNumber)
            res.body.Records[i].dynamodb.SizeBytes.should.be.above(0)
            res.body.Records[i].dynamodb.StreamViewType.should.be.equal(streamViewType)

            lastSequenceNumber = res.body.Records[i].dynamodb.SequenceNumber
          }

          res.body.Records[0].dynamodb.Keys.should.be.eql({a: {S: 'foo'}})
          res.body.Records[0].dynamodb.NewImage.should.be.eql({a: {S: 'foo'}, val: {S: 'bar'}, other: {S: 'baz'}})

          res.body.Records[1].dynamodb.Keys.should.be.eql({a: {S: 'meaning-of-life'}})
          res.body.Records[1].dynamodb.NewImage.should.be.eql({a: {S: 'meaning-of-life'}, answer: {N: '42'}})

          var iterator = res.body.NextShardIterator
          request(opts({ShardIterator: iterator, Limit: 2}), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.be.equal(200)

            res.body.NextShardIterator.should.not.be.empty
            res.body.Records.should.be.Array()
            res.body.Records.should.have.length(2)

            var lastSequenceNumber = 0
            for (var i = 0; i < 2; i++) {
              res.body.Records[i].awsRegion.should.be.equal(helpers.awsRegion)
              res.body.Records[i].eventID.should.not.be.empty
              res.body.Records[i].eventSource.should.be.equal('aws:dynamodb')
              res.body.Records[i].eventVersion.should.be.equal('1.1')

              res.body.Records[i].dynamodb.Keys.should.be.eql({a: {S: 'foo'}})
              res.body.Records[i].dynamodb.ApproximateCreationDateTime.should.not.be.empty
              res.body.Records[i].dynamodb.SequenceNumber.should.be.above(lastSequenceNumber)
              res.body.Records[i].dynamodb.SizeBytes.should.be.above(0)
              res.body.Records[i].dynamodb.StreamViewType.should.be.equal(streamViewType)

              lastSequenceNumber = res.body.Records[i].dynamodb.SequenceNumber
            }

            res.body.Records[0].eventName.should.be.equal('MODIFY')
            res.body.Records[0].dynamodb.OldImage.should.be.eql({a: {S: 'foo'}, val: {S: 'bar'}, other: {S: 'baz'}})
            res.body.Records[0].dynamodb.NewImage.should.be.eql({a: {S: 'foo'}, val: {S: 'New value'}, other: {S: 'baz'}})

            res.body.Records[1].eventName.should.be.equal('REMOVE')
            res.body.Records[1].dynamodb.OldImage.should.be.eql({a: {S: 'foo'}, val: {S: 'New value'}, other: {S: 'baz'}})
            res.body.Records[1].dynamodb.should.not.have.property('NewImage')

            var iterator = res.body.NextShardIterator
            request(opts({ShardIterator: iterator}), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.be.equal(200)

              res.body.NextShardIterator.should.not.be.empty
              res.body.Records.should.be.Array()
              res.body.Records.should.be.empty

              done()
            })
          })
        })
      })
    })

    it('latest iterator should return only new records', function(done) {
      var req = {
        StreamArn: streamArn,
        ShardId: shardId,
        ShardIteratorType: 'LATEST',
      }
      request(helpers.opts('GetShardIterator', req), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.be.equal(200)

        var iterator = res.body.ShardIterator
        request(opts({ShardIterator: iterator}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.be.equal(200)

          res.body.NextShardIterator.should.not.be.empty
          res.body.Records.should.be.Array()
          res.body.Records.should.be.empty

          var iterator = res.body.NextShardIterator

          request(helpers.opts('PutItem', {TableName: tableName, Item: {a: {S: 'New Item'}}}), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.be.equal(200)

            request(opts({ShardIterator: iterator}), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.be.equal(200)

              res.body.NextShardIterator.should.not.be.empty
              res.body.Records.should.be.Array()
              res.body.Records.should.have.length(1)

              res.body.Records[0].awsRegion.should.be.equal(helpers.awsRegion)
              res.body.Records[0].eventID.should.not.be.empty
              res.body.Records[0].eventSource.should.be.equal('aws:dynamodb')
              res.body.Records[0].eventVersion.should.be.equal('1.1')
              res.body.Records[0].eventName.should.be.equal('INSERT')
              res.body.Records[0].dynamodb.Keys.should.be.eql({a: {S: 'New Item'}})
              res.body.Records[0].dynamodb.ApproximateCreationDateTime.should.not.be.empty
              res.body.Records[0].dynamodb.SequenceNumber.should.not.be.empty
              res.body.Records[0].dynamodb.SizeBytes.should.be.above(0)
              res.body.Records[0].dynamodb.StreamViewType.should.be.equal(streamViewType)
              res.body.Records[0].dynamodb.NewImage.should.be.eql({a: {S: 'New Item'}})
              res.body.Records[0].dynamodb.should.not.have.property('OldImage')

              var iterator = res.body.NextShardIterator
              request(opts({ShardIterator: iterator}), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.be.equal(200)

                res.body.NextShardIterator.should.not.be.empty
                res.body.Records.should.be.Array()
                res.body.Records.should.be.empty

                done()
              })
            })
          })
        })
      })
    })

  })

})
