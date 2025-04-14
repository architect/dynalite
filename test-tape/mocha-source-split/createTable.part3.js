var helpers = require('./helpers'),
  should = require('should')

var target = 'CreateTable',
  request = helpers.request,
  randomName = helpers.randomName,
  opts = helpers.opts.bind(null, target),
  assertType = helpers.assertType.bind(null, target),
  assertValidation = helpers.assertValidation.bind(null, target)

describe('createTable', function () {
  describe('functionality', function () {

    it('should succeed for basic', function (done) {
      var table = {
          TableName: randomName(),
          AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' } ],
          KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
          ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
        }, createdAt = Date.now() / 1000
      request(opts(table), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        should.exist(res.body.TableDescription)
        var desc = res.body.TableDescription
        desc.TableId.should.match(/[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{8}/)
        delete desc.TableId
        desc.CreationDateTime.should.be.above(createdAt - 5)
        delete desc.CreationDateTime
        desc.TableArn.should.match(new RegExp(
          'arn:aws:dynamodb:' + helpers.awsRegion + ':\\d+:table/' + table.TableName))
        delete desc.TableArn
        table.ItemCount = 0
        table.ProvisionedThroughput.NumberOfDecreasesToday = 0
        table.TableSizeBytes = 0
        table.TableStatus = 'CREATING'
        desc.should.eql(table)
        helpers.deleteWhenActive(table.TableName)
        done()
      })
    })

    it('should succeed for basic PAY_PER_REQUEST', function (done) {
      var table = {
          TableName: randomName(),
          AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' } ],
          KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
          BillingMode: 'PAY_PER_REQUEST',
        }, createdAt = Date.now() / 1000
      request(opts(table), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        should.exist(res.body.TableDescription)
        var desc = res.body.TableDescription
        desc.TableId.should.match(/[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{8}/)
        delete desc.TableId
        desc.CreationDateTime.should.be.above(createdAt - 5)
        delete desc.CreationDateTime
        desc.TableArn.should.match(new RegExp(
          'arn:aws:dynamodb:' + helpers.awsRegion + ':\\d+:table/' + table.TableName))
        delete desc.TableArn
        table.ItemCount = 0
        table.TableSizeBytes = 0
        table.TableStatus = 'CREATING'
        table.BillingModeSummary = { BillingMode: 'PAY_PER_REQUEST' }
        delete table.BillingMode
        table.TableThroughputModeSummary = { TableThroughputMode: 'PAY_PER_REQUEST' }
        table.ProvisionedThroughput = {
          NumberOfDecreasesToday: 0,
          ReadCapacityUnits: 0,
          WriteCapacityUnits: 0,
        }
        desc.should.eql(table)
        helpers.deleteWhenActive(table.TableName)
        done()
      })
    })

    it('should change state to ACTIVE after a period', function (done) {
      this.timeout(100000)
      var table = {
        TableName: randomName(),
        AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' } ],
        KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
        ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
      }
      request(opts(table), function (err, res) {
        if (err) return done(err)
        res.body.TableDescription.TableStatus.should.equal('CREATING')

        helpers.waitUntilActive(table.TableName, function (err, res) {
          if (err) return done(err)
          res.body.Table.TableStatus.should.equal('ACTIVE')
          helpers.deleteWhenActive(table.TableName)
          done()
        })
      })
    })

    // TODO: Seems to block until other tables with secondary indexes have been created
    it('should succeed for LocalSecondaryIndexes', function (done) {
      this.timeout(100000)
      var table = {
          TableName: randomName(),
          AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
          KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' }, { KeyType: 'RANGE', AttributeName: 'b' } ],
          LocalSecondaryIndexes: [ {
            IndexName: 'abc',
            KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
            Projection: { ProjectionType: 'ALL' },
          }, {
            IndexName: 'abd',
            KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
            Projection: { ProjectionType: 'ALL' },
          }, {
            IndexName: 'abe',
            KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
            Projection: { ProjectionType: 'ALL' },
          }, {
            IndexName: 'abf',
            KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
            Projection: { ProjectionType: 'ALL' },
          }, {
            IndexName: 'abg',
            KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
            Projection: { ProjectionType: 'ALL' },
          } ],
          ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
        }, createdAt = Date.now() / 1000
      request(opts(table), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        should.exist(res.body.TableDescription)
        var desc = res.body.TableDescription
        desc.TableId.should.match(/[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{8}/)
        delete desc.TableId
        desc.CreationDateTime.should.be.above(createdAt - 5)
        delete desc.CreationDateTime
        desc.TableArn.should.match(new RegExp(
          'arn:aws:dynamodb:' + helpers.awsRegion + ':\\d+:table/' + table.TableName))
        delete desc.TableArn
        desc.LocalSecondaryIndexes.forEach(function (index) {
          index.IndexArn.should.match(new RegExp(
            'arn:aws:dynamodb:' + helpers.awsRegion + ':\\d+:table/' + table.TableName + '/index/' + index.IndexName))
          delete index.IndexArn
        })
        table.ItemCount = 0
        table.ProvisionedThroughput.NumberOfDecreasesToday = 0
        table.TableSizeBytes = 0
        table.TableStatus = 'CREATING'
        // DynamoDB seem to put them in a weird order, so check separately
        table.LocalSecondaryIndexes.forEach(function (index) {
          index.IndexSizeBytes = 0
          index.ItemCount = 0
          desc.LocalSecondaryIndexes.should.containEql(index)
        })
        desc.LocalSecondaryIndexes.length.should.equal(table.LocalSecondaryIndexes.length)
        delete desc.LocalSecondaryIndexes
        delete table.LocalSecondaryIndexes
        desc.should.eql(table)
        helpers.deleteWhenActive(table.TableName)
        done()
      })
    })

    it('should succeed for multiple GlobalSecondaryIndexes', function (done) {
      this.timeout(300000)
      var table = {
          TableName: randomName(),
          AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
          KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
          GlobalSecondaryIndexes: [ {
            IndexName: 'abc',
            KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
            ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
            Projection: { ProjectionType: 'ALL' },
          }, {
            IndexName: 'abd',
            KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
            ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
            Projection: { ProjectionType: 'ALL' },
          }, {
            IndexName: 'abe',
            KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
            ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
            Projection: { ProjectionType: 'ALL' },
          }, {
            IndexName: 'abf',
            KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
            ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
            Projection: { ProjectionType: 'ALL' },
          }, {
            IndexName: 'abg',
            KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
            ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
            Projection: { ProjectionType: 'ALL' },
          } ],
          ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
        }, createdAt = Date.now() / 1000, globalIndexes = table.GlobalSecondaryIndexes
      request(opts(table), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        should.exist(res.body.TableDescription)
        var desc = res.body.TableDescription
        desc.TableId.should.match(/[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{8}/)
        delete desc.TableId
        desc.CreationDateTime.should.be.above(createdAt - 5)
        delete desc.CreationDateTime
        desc.TableArn.should.match(new RegExp(
          'arn:aws:dynamodb:' + helpers.awsRegion + ':\\d+:table/' + table.TableName))
        delete desc.TableArn
        desc.GlobalSecondaryIndexes.forEach(function (index) {
          index.IndexArn.should.match(new RegExp(
            'arn:aws:dynamodb:' + helpers.awsRegion + ':\\d+:table/' + table.TableName + '/index/' + index.IndexName))
          delete index.IndexArn
        })
        table.ItemCount = 0
        table.ProvisionedThroughput.NumberOfDecreasesToday = 0
        table.TableSizeBytes = 0
        table.TableStatus = 'CREATING'
        // DynamoDB seem to put them in a weird order, so check separately
        globalIndexes.forEach(function (index) {
          index.IndexSizeBytes = 0
          index.ItemCount = 0
          index.IndexStatus = 'CREATING'
          index.ProvisionedThroughput.NumberOfDecreasesToday = 0
          desc.GlobalSecondaryIndexes.should.containEql(index)
        })
        desc.GlobalSecondaryIndexes.length.should.equal(globalIndexes.length)
        delete desc.GlobalSecondaryIndexes
        delete table.GlobalSecondaryIndexes
        desc.should.eql(table)

        // Ensure that the indexes become active too
        helpers.waitUntilIndexesActive(table.TableName, function (err, res) {
          if (err) return done(err)
          res.body.Table.GlobalSecondaryIndexes.forEach(function (index) { delete index.IndexArn })
          globalIndexes.forEach(function (index) {
            index.IndexStatus = 'ACTIVE'
            res.body.Table.GlobalSecondaryIndexes.should.containEql(index)
          })
          helpers.deleteWhenActive(table.TableName)
          done()
        })
      })
    })

    it('should succeed for PAY_PER_REQUEST GlobalSecondaryIndexes', function (done) {
      var table = {
          TableName: randomName(),
          AttributeDefinitions: [ { AttributeName: 'a', AttributeType: 'S' }, { AttributeName: 'b', AttributeType: 'S' } ],
          KeySchema: [ { KeyType: 'HASH', AttributeName: 'a' } ],
          BillingMode: 'PAY_PER_REQUEST',
          GlobalSecondaryIndexes: [ {
            IndexName: 'abc',
            KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
            Projection: { ProjectionType: 'ALL' },
          }, {
            IndexName: 'abd',
            KeySchema: [ { AttributeName: 'a', KeyType: 'HASH' }, { AttributeName: 'b', KeyType: 'RANGE' } ],
            Projection: { ProjectionType: 'ALL' },
          } ],
        }, createdAt = Date.now() / 1000, globalIndexes = table.GlobalSecondaryIndexes
      request(opts(table), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        should.exist(res.body.TableDescription)
        var desc = res.body.TableDescription
        desc.TableId.should.match(/[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{8}/)
        delete desc.TableId
        desc.CreationDateTime.should.be.above(createdAt - 5)
        delete desc.CreationDateTime
        desc.TableArn.should.match(new RegExp(
          'arn:aws:dynamodb:' + helpers.awsRegion + ':\\d+:table/' + table.TableName))
        delete desc.TableArn
        desc.GlobalSecondaryIndexes.forEach(function (index) {
          index.IndexArn.should.match(new RegExp(
            'arn:aws:dynamodb:' + helpers.awsRegion + ':\\d+:table/' + table.TableName + '/index/' + index.IndexName))
          delete index.IndexArn
        })
        table.ItemCount = 0
        table.TableSizeBytes = 0
        table.BillingModeSummary = { BillingMode: 'PAY_PER_REQUEST' }
        delete table.BillingMode
        table.TableThroughputModeSummary = { TableThroughputMode: 'PAY_PER_REQUEST' }
        table.ProvisionedThroughput = {
          NumberOfDecreasesToday: 0,
          ReadCapacityUnits: 0,
          WriteCapacityUnits: 0,
        }
        table.TableStatus = 'CREATING'
        globalIndexes.forEach(function (index) {
          index.IndexSizeBytes = 0
          index.ItemCount = 0
          index.IndexStatus = 'CREATING'
          index.ProvisionedThroughput = {
            ReadCapacityUnits: 0,
            WriteCapacityUnits: 0,
            NumberOfDecreasesToday: 0,
          }
          desc.GlobalSecondaryIndexes.should.containEql(index)
        })
        desc.GlobalSecondaryIndexes.length.should.equal(globalIndexes.length)
        delete desc.GlobalSecondaryIndexes
        delete table.GlobalSecondaryIndexes
        desc.should.eql(table)

        // Ensure that the indexes become active too
        helpers.waitUntilIndexesActive(table.TableName, function (err, res) {
          if (err) return done(err)
          res.body.Table.GlobalSecondaryIndexes.forEach(function (index) { delete index.IndexArn })
          globalIndexes.forEach(function (index) {
            index.IndexStatus = 'ACTIVE'
            res.body.Table.GlobalSecondaryIndexes.should.containEql(index)
          })
          helpers.deleteWhenActive(table.TableName)
          done()
        })
      })
    })

  })
})