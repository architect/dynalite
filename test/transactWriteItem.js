var async = require('async'),
    helpers = require('./helpers'),
    db = require('../db')

var target = 'TransactWriteItems',
    request = helpers.request,
    randomName = helpers.randomName,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertTransactionCanceled = helpers.assertTransactionCanceled.bind(null, target)
    assertNotFound = helpers.assertNotFound.bind(null, target)

describe('transactWriteItem', function() {

    describe('serializations', function() {

        it('should return SerializationException when TransactItems is not a list', function(done) {
            assertType('TransactItems', 'List', done)
        })

        it('should return SerializationException when TransactItems.0.Delete.Key is not a map', function(done) {
            assertType('TransactItems.0.Delete.Key', 'Map<AttributeValue>', done)
        })

        it('should return SerializationException when TransactItems.0.Delete.Key.Attr is not an attr struct', function(done) {
            this.timeout(60000)
            assertType('TransactItems.0.Delete.Key.Attr', 'AttrStruct<ValueStruct>', done)
        })

        it('should return SerializationException when TransactItems.0.Put is not a struct', function(done) {
            assertType('TransactItems.0.Put', 'FieldStruct<Put>', done)
        })

        it('should return SerializationException when TransactItems.0.Put.Item is not a map', function(done) {
            assertType('TransactItems.0.Put.Item', 'Map<AttributeValue>', done)
        })

        it('should return SerializationException when TransactItems.0.Put.Item.Attr is not an attr struct', function(done) {
            this.timeout(60000)
            assertType('TransactItems.0.Put.Item.Attr', 'AttrStruct<ValueStruct>', done)
        })

        it('should return SerializationException when TransactItems.0.Update is not a struct', function(done) {
            assertType('TransactItems.0.Update', 'FieldStruct<Update>', done)
        })

        it('should return SerializationException when TransactItems.0.Update.UpdateExpression is not a string', function(done) {
            assertType('TransactItems.0.Update.UpdateExpression', 'String', done)
        })

        it('should return SerializationException when ReturnConsumedCapacity is not a string', function(done) {
            assertType('ReturnConsumedCapacity', 'String', done)
        })

        it('should return SerializationException when ReturnItemCollectionMetrics is not a string', function(done) {
            assertType('ReturnItemCollectionMetrics', 'String', done)
        })
    })
    
    describe('validations', function() {

        it('should return ValidationException for empty body', function (done) {
            assertValidation({},
                '1 validation error detected: ' +
                'Value null at \'transactItems\' failed to satisfy constraint: ' +
                'Member must not be null', done)
        })

        it('should return ValidationException for missing TransactItems', function (done) {
            assertValidation({ReturnConsumedCapacity: 'hi', ReturnItemCollectionMetrics: 'hi'}, [
                'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
                'Member must satisfy enum value set: [INDEXES, TOTAL, NONE]',
                'Value \'hi\' at \'returnItemCollectionMetrics\' failed to satisfy constraint: ' +
                'Member must satisfy enum value set: [SIZE, NONE]',
                'Value null at \'transactItems\' failed to satisfy constraint: ' +
                'Member must not be null',
            ], done)
        })

        it('should return ValidationException for empty TransactItems', function (done) {
            assertValidation({TransactItems: []},
                '1 validation error detected: ' +
                'Value \'[]\' at \'transactItems\' failed to satisfy constraint: ' +
                'Member must have length greater than or equal to 1', done)
        })

        it('should return ValidationException for invalid update request in TransactItems', function (done) {
            assertValidation({TransactItems: [{Update: {}}]},
                '1 validation error detected: ' +
                'Value null at \'transactItems.1.member.update.key\' failed to satisfy constraint: ' +
                'Member must not be null', done)
        })


        it('should return ValidationException for invalid put request in TransactItems', function (done) {
            assertValidation({TransactItems: [{Put: {}}]},
                '1 validation error detected: ' +
                'Value null at \'transactItems.1.member.put.item\' failed to satisfy constraint: ' +
                'Member must not be null', done)
        })


        it('should return ValidationException for invalid delete request in TransactItems', function (done) {
            assertValidation({TransactItems: [{Delete: {}}]},
                '1 validation error detected: ' +
                'Value null at \'transactItems.1.member.delete.key\' failed to satisfy constraint: ' +
                'Member must not be null', done)
        })

        it('should return ValidationException for invalid ConditionCheck request in TransactItems', function (done) {
            assertValidation({TransactItems: [{ConditionCheck: {}}]},
                '1 validation error detected: ' +
                'Value null at \'transactItems.1.member.conditionCheck.key\' failed to satisfy constraint: ' +
                'Member must not be null', done)
        })

        it('should return ValidationException for invalid metadata and missing requests', function (done) {
            assertValidation({TransactItems: [], ReturnConsumedCapacity: 'hi', ReturnItemCollectionMetrics: 'hi'}, [
                'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
                'Member must satisfy enum value set: [INDEXES, TOTAL, NONE]',
                'Value \'hi\' at \'returnItemCollectionMetrics\' failed to satisfy constraint: ' +
                'Member must satisfy enum value set: [SIZE, NONE]',
                'Value \'[]\' at \'transactItems\' failed to satisfy constraint: ' +
                'Member must have length greater than or equal to 1',
            ], done)
        })

        it('should return ValidationException for incorrect attributes', function (done) {
            assertValidation({
                TransactItems: [{Put: {}, Delete: {}}],
                ReturnConsumedCapacity: 'hi', ReturnItemCollectionMetrics: 'hi'
            }, [
                'Value \'hi\' at \'returnConsumedCapacity\' failed to satisfy constraint: ' +
                'Member must satisfy enum value set: [INDEXES, TOTAL, NONE]',
                'Value \'hi\' at \'returnItemCollectionMetrics\' failed to satisfy constraint: ' +
                'Member must satisfy enum value set: [SIZE, NONE]',
                'Value null at \'transactItems.1.member.delete.key\' failed to satisfy constraint: ' +
                'Member must not be null',
                'Value null at \'transactItems.1.member.put.item\' failed to satisfy constraint: ' +
                'Member must not be null',
            ], done)
        })

        it('should return ValidationException when writing more than 25 items', function (done) {
            var requests = [], i
            for (i = 0; i < 26; i++) {
                requests.push(i % 2 ? {Delete: {Key: {a: {S: String(i)}}}} : {Put: {Item: {a: {S: String(i)}}}})
            }
            assertValidation({TransactItems: requests},
                [new RegExp('Member must have length less than or equal to 25')], done)
        })

        it('should return ResourceNotFoundException when fetching exactly 25 items and table does not exist', function (done) {
            var requests = [], i
            for (i = 0; i < 25; i++) {
                requests.push(i % 2 ? {Delete: {TableName: 'a', Key: {a: {S: String(i)}}}} : {
                    Put: {
                        TableName: 'a',
                        Item: {a: {S: String(i)}}
                    }
                })
            }
            assertNotFound({TransactItems: requests},
                'Requested resource not found', done)
        })

        it('should check table exists first before checking for duplicate keys', function (done) {
            assertNotFound({
                    TransactItems: [{Delete: {TableName: 'a', Key: {a: {S: '1'}}}}, {
                        Put: {
                            TableName: 'a',
                            Item: {a: {S: '1'}}
                        }
                    }]
                },
                'Requested resource not found', done)
        })

        it('should return TransactionCanceledException for puts and deletes of the same item with delete first', function (done) {
            var transaction = {
                TransactItems: [{
                    Delete: {
                        TableName: helpers.testHashTable,
                        Key: {a: {S: 'aaaaa'}}
                    }
                }, {Put: {TableName: helpers.testHashTable, Item: {a: {S: 'aaaaa'}}}}]
            }
            assertTransactionCanceled(transaction, 'Transaction cancelled, please refer cancellation reasons for specific reasons', done)
        })

        it('should return TransactionCanceledException for puts and deletes of the same item with put first', function (done) {
            var transaction = {
                TransactItems: [{
                    Put: {
                        TableName: helpers.testHashTable,
                        Item: {a: {S: 'aaaaa'}}
                    }
                }, {Delete: {TableName: helpers.testHashTable, Key: {a: {S: 'aaaaa'}}}}]
            }
            assertTransactionCanceled(transaction, 'Transaction cancelled, please refer cancellation reasons for specific reasons', done)
        })

        it('should return TransactionCanceledException for puts and updates of the same item with put first', function (done) {
            var transaction = {
                TransactItems: [{
                    Put: {
                        TableName: helpers.testHashTable,
                        Item: {a: {S: 'aaaaa'}}
                    }
                }, {
                    Update: {
                        TableName: helpers.testHashTable,
                        Key: {a: {S: 'aaaaa'}},
                        UpdateExpression: 'SET b = :b',
                        ExpressionAttributeValues: {
                            ':b': {
                                S: 'b'
                            }
                        }
                    }
                }]
            }
            assertTransactionCanceled(transaction, 'Transaction cancelled, please refer cancellation reasons for specific reasons', done)
        })

        it('should return TransactionCanceledException for puts and condition checks of the same item with put first', function (done) {
            var transaction = {
                TransactItems: [{
                    Put: {
                        TableName: helpers.testHashTable,
                        Item: {a: {S: 'aaaaa'}}
                    }
                }, {
                    ConditionCheck: {
                        TableName: helpers.testHashTable,
                        Key: {a: {S: 'aaaaa'}},
                        ConditionExpression: 'attribute_exists(a)'
                    }
                }]
            }
            assertTransactionCanceled(transaction, 'Transaction cancelled, please refer cancellation reasons for specific reasons', done)
        })

        it('should return ValidationException for item too large', function(done) {
            var key = {a: {S: helpers.randomString()}}
            var expressionAttributeValues = {
                ':b': {S: new Array(helpers.MAX_SIZE).join('a')},
                ':c': {N: new Array(38 + 1).join('1') + new Array(89).join('0')},
            }
            assertValidation({TransactItems: [
                {Update:
                        {TableName: helpers.testHashTable,
                            Key: key,
                            UpdateExpression: 'SET b = :b, c = :c',
                            ExpressionAttributeValues: expressionAttributeValues
                }}]},
                'Item size to update has exceeded the maximum allowed size',
                done)
        })

        it('should return ValidationException for key type mismatch in Put Item', function (done) {
            async.forEach([
                {NULL: true},
                {M: {a: {S: ''}}},
                {L: [{M: {a: {S: ''}}}]}
            ], function (expr, cb) {
                assertValidation({TransactItems: [{Put: {TableName: helpers.testHashTable, Item: {a: expr}}}]},
                    'The provided key element does not match the schema', cb)
            }, done)
        })

        it('should return ValidationException for single invalid action', function (done) {
            var transaction = {
                TransactItems: [{
                    NotARealAction: {
                        TableName: helpers.testHashTable,
                        Item: {a: {S: 'aaaaa'}}
                    }
                }]
            }
            assertValidation(transaction, 'The action or operation requested is invalid. Verify that the action is typed correctly.', done)
        })

        it('should return ValidationException for one invalid action and one valid action', function (done) {
            var transaction = {
                TransactItems: [{
                    NotARealAction: {
                        TableName: helpers.testHashTable,
                        Item: {a: {S: 'aaaaa'}}
                    }
                },
                    {
                        Put: {
                            TableName: helpers.testHashTable,
                            Item: {a: {S: 'aaaaa'}}
                        }
                    }]
            }
            assertValidation(transaction, 'The action or operation requested is invalid. Verify that the action is typed correctly.', done)
        })


        it('should return ValidationException for multiple invalid actions', function (done) {
            var transaction = {
                TransactItems: [{
                    NotARealAction: {
                        TableName: helpers.testHashTable,
                        Item: {a: {S: 'aaaaa'}}
                    }
                },
                    {
                        AnotherFakeAction: {
                            TableName: helpers.testHashTable,
                            Item: {a: {S: 'aaaaa'}}
                        }
                    }]
            }
            assertValidation(transaction, 'The action or operation requested is invalid. Verify that the action is typed correctly.', done)
        })

    describe('functionality', function() {
        it('should write a single item', function(done) {
            var item = {
                a: {S: helpers.randomString()},
                    c: {S: 'c'}},
                batchReq = {TransactItems: []}
            batchReq.TransactItems = [{Put: {TableName: helpers.testHashTable, Item: item}}]
            request(opts(batchReq), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)
                res.body.should.eql({UnprocessedItems: {}})
                request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item.a}, ConsistentRead: true}), function(err, res) {
                    if (err) return done(err)
                    res.statusCode.should.equal(200)
                    res.body.should.eql({Item: item})
                    done()
                })
            })
        })

        it('should put multiple items', function(done) {
            var item = {
                    a: {S: helpers.randomString()},
                    c: {S: 'c'}},
                item2 = {
                    a: {S: helpers.randomString()},
                    c: {S: 'c'}},
                transactReq = {TransactItems: []}
            transactReq.TransactItems = [{Put: {TableName: helpers.testHashTable, Item: item}}, {Put: {TableName: helpers.testHashTable, Item: item2}}]
            request(opts(transactReq), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)
                res.body.should.eql({UnprocessedItems: {}})
                request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item.a}, ConsistentRead: true}), function(err, res) {
                    if (err) return done(err)
                    res.statusCode.should.equal(200)
                    res.body.should.eql({Item: item})
                    request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item2.a}, ConsistentRead: true}), function(err, res) {
                        if (err) return done(err)
                        res.statusCode.should.equal(200)
                        res.body.should.eql({Item: item2})
                        done()
                    })
                })
            })
        })

        it('should update multiple items', function(done) {
            var item = {
                    a: {S: helpers.randomString()},
                    c: {S: 'c'}},
                item2 = {
                    a: {S: helpers.randomString()},
                    c: {S: 'c'}},
                transactReq = {TransactItems: []}

            transactReq.TransactItems = [
                {
                    Update: {
                        TableName: helpers.testHashTable,
                        Key: {
                            a: item.a
                        },
                        UpdateExpression: 'SET c=:d',
                        ExpressionAttributeValues: {
                            ':d': {
                                S: 'd'
                            }
                        }
                    }
                },
                {
                    Update: {
                        TableName: helpers.testHashTable,
                        Key: {
                            a: item2.a
                        },
                        UpdateExpression: 'SET c=:d',
                        ExpressionAttributeValues: {
                            ':d': {
                                S: 'd'
                            }
                        }
                    }
                }
            ]

            request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)
                request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item2}), function(err, res) {
                    if (err) return done(err)
                    res.statusCode.should.equal(200)
                    request(opts(transactReq), function(err, res) {
                        if (err) return done(err)
                        res.statusCode.should.equal(200)
                        res.body.should.eql({UnprocessedItems: {}})
                        request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item.a}, ConsistentRead: true}), function(err, res) {
                            // update item
                            if (err) return done(err)
                            res.statusCode.should.equal(200)
                            res.body.Item.should.eql({...item, c: {S: 'd'}})
                            request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item2.a}, ConsistentRead: true}), function(err, res) {
                                // put item
                                if (err) return done(err)
                                res.statusCode.should.equal(200)
                                res.body.Item.should.eql({...item2, c: {S: 'd'}})
                                done()
                            })
                        })
                    })
                })
            })
        })

        it('should delete multiple items', function(done) {
            var item = {
                    a: {S: helpers.randomString()},
                    c: {S: 'c'}},
                item2 = {
                    a: {S: helpers.randomString()},
                    c: {S: 'c'}},
                transactReq = {TransactItems: []}

            transactReq.TransactItems = [
                {
                    Delete: {
                        TableName: helpers.testHashTable,
                        Key: {
                            a: item.a
                        }
                    }
                },
                {
                    Delete: {
                        TableName: helpers.testHashTable,
                        Key: {
                            a: item2.a
                        }
                    }
                }
            ]

            request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)
                request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item2}), function(err, res) {
                    if (err) return done(err)
                    res.statusCode.should.equal(200)
                    request(opts(transactReq), function(err, res) {
                        if (err) return done(err)
                        res.statusCode.should.equal(200)
                        res.body.should.eql({UnprocessedItems: {}})
                        request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item.a}, ConsistentRead: true}), function(err, res) {
                            // update item
                            if (err) return done(err)
                            res.statusCode.should.equal(200)
                            res.body.should.eql({})
                            request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item2.a}, ConsistentRead: true}), function(err, res) {
                                // put item
                                if (err) return done(err)
                                res.statusCode.should.equal(200)
                                res.body.should.eql({})
                                done()
                            })
                        })
                    })
                })
            })
        })

        it('should write, update, and delete in one transaction', function(done) {
            var item = {
                    a: {S: helpers.randomString()},
                    c: {S: 'c'}},
                item2 = {
                    a: {S: helpers.randomString()},
                    c: {S: 'c'}},
                item3 = {
                    a: {S: helpers.randomString()},
                    c: {S: 'c'}},
                transactReq = {TransactItems: []}

            transactReq.TransactItems = [
                {
                    Put: {
                        TableName: helpers.testHashTable,
                        Item: item3
                    }
                },
                {
                    Update: {
                        TableName: helpers.testHashTable,
                        Key: {
                            a: item.a
                        },
                        UpdateExpression: 'SET c=:d',
                        ExpressionAttributeValues: {
                            ':d': {
                                S: 'd'
                            }
                        }
                    }
                },
                {
                    Delete: {
                        TableName: helpers.testHashTable,
                        Key: {
                            a: item2.a
                        }
                    }
                }
            ]

            request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)
                request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item2}), function(err, res) {
                    if (err) return done(err)
                    res.statusCode.should.equal(200)
                    request(opts(transactReq), function(err, res) {
                        if (err) return done(err)
                        res.statusCode.should.equal(200)
                        res.body.should.eql({UnprocessedItems: {}})
                        request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item.a}, ConsistentRead: true}), function(err, res) {
                            // update item
                            if (err) return done(err)
                            res.statusCode.should.equal(200)
                            res.body.Item.should.eql({...item, c: {S: 'd'}})
                            request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item3.a}, ConsistentRead: true}), function(err, res) {
                                // put item
                                if (err) return done(err)
                                res.statusCode.should.equal(200)
                                res.body.should.eql({Item: item3})
                                request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item2.a}, ConsistentRead: true}), function(err, res) {
                                    // delete item
                                    if (err) return done(err)
                                    res.statusCode.should.equal(200)
                                    res.body.should.eql({})
                                    done()
                                })
                            })
                        })
                    })
                })
            })
        })

        it('should write & update with condition expression in one transaction', function(done) {
            var item = {
                    a: {S: helpers.randomString()},
                    c: {S: 'c'}},
                item2 = {
                    a: {S: helpers.randomString()},
                    c: {S: 'c'}},
                transactReq = {TransactItems: []}

            transactReq.TransactItems = [
                {
                    Put: {
                        TableName: helpers.testHashTable,
                        Item: item
                    }
                },
                {
                    Update: {
                        TableName: helpers.testHashTable,
                        Key: {
                            a: item2.a
                        },
                        ConditionExpression: 'attribute_not_exists(f)',
                        UpdateExpression: 'SET c=:d',
                        ExpressionAttributeValues: {
                            ':d': {
                                S: 'd'
                            }
                        }
                    }
                }
            ]

            request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item2}), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)
                request(opts(transactReq), function(err, res) {
                    if (err) return done(err)
                    res.statusCode.should.equal(200)
                    res.body.should.eql({UnprocessedItems: {}})
                    request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item.a}, ConsistentRead: true}), function(err, res) {
                        // update item
                        if (err) return done(err)
                        res.statusCode.should.equal(200)
                        res.body.Item.should.eql(item)
                        request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item2.a}, ConsistentRead: true}), function(err, res) {
                            // put item
                            if (err) return done(err)
                            res.statusCode.should.equal(200)
                            res.body.Item.should.eql({...item2, c: {S: 'd'}})
                            done()
                        })
                    })
                })
            })
        })

        it('should fail to write & update with failed condition expression in one transaction', function(done) {
            var item = {
                    a: {S: helpers.randomString()},
                    c: {S: 'c'}},
                item2 = {
                    a: {S: helpers.randomString()},
                    c: {S: 'c'}},
                transactReq = {TransactItems: []}

            transactReq.TransactItems = [
                {
                    Put: {
                        TableName: helpers.testHashTable,
                        Item: item
                    }
                },
                {
                    Update: {
                        TableName: helpers.testHashTable,
                        Key: {
                            a: item2.a
                        },
                        ConditionExpression: 'attribute_not_exists(c)',
                        UpdateExpression: 'SET c=:d',
                        ExpressionAttributeValues: {
                            ':d': {
                                S: 'd'
                            }
                        }
                    }
                }
            ]

            request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item2}), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)
                request(opts(transactReq), function(err, res) {
                    if (err) return done(err)
                    res.statusCode.should.equal(400)
                    res.body.message.should.equal('The conditional request failed')
                    request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item.a}, ConsistentRead: true}), function(err, res) {
                        // update item
                        if (err) return done(err)
                        res.statusCode.should.equal(200)
                        res.body.should.eql({})
                        request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item2.a}, ConsistentRead: true}), function(err, res) {
                            // put item
                            if (err) return done(err)
                            res.statusCode.should.equal(200)
                            res.body.Item.should.eql(item2)
                            done()
                        })
                    })
                })
            })
        })

        it('should fail to write with failed ConditionCheck in one transaction', function(done) {
            var item = {
                    a: {S: helpers.randomString()},
                    c: {S: 'c'}},
                item2 = {
                    a: {S: helpers.randomString()},
                    c: {S: 'c'}},
                transactReq = {TransactItems: []}

            transactReq.TransactItems = [
                {
                    Put: {
                        TableName: helpers.testHashTable,
                        Item: item
                    }
                },
                {
                    ConditionCheck: {
                        TableName: helpers.testHashTable,
                        Key: {
                            a: item2.a
                        },
                        ConditionExpression: 'attribute_not_exists(c)'
                    }
                }
            ]

            request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item2}), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)
                request(opts(transactReq), function(err, res) {
                    if (err) return done(err)
                    res.statusCode.should.equal(400)
                    res.body.message.should.equal('The conditional request failed')
                    request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item.a}, ConsistentRead: true}), function(err, res) {
                        if (err) return done(err)
                        res.statusCode.should.equal(200)
                        res.body.should.eql({})
                        done()
                    })
                })
            })
        })

        it('should succeed to write with ConditionCheck in one transaction', function(done) {
            var item = {
                    a: {S: helpers.randomString()},
                    c: {S: 'c'}},
                item2 = {
                    a: {S: helpers.randomString()},
                    c: {S: 'c'}},
                transactReq = {TransactItems: []}

            transactReq.TransactItems = [
                {
                    Put: {
                        TableName: helpers.testHashTable,
                        Item: item
                    }
                },
                {
                    ConditionCheck: {
                        TableName: helpers.testHashTable,
                        Key: {
                            a: item2.a
                        },
                        ConditionExpression: 'attribute_not_exists(c)'
                    }
                }
            ]

            request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item2}), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)
                request(opts(transactReq), function(err, res) {
                    if (err) return done(err)
                    res.statusCode.should.equal(400)
                    res.body.message.should.equal('The conditional request failed')
                    request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item.a}, ConsistentRead: true}), function(err, res) {
                        if (err) return done(err)
                        res.statusCode.should.equal(200)
                        res.body.should.eql({})
                        done()
                    })
                })
            })
        })

        it('should succeed to write in table A with succeeded ConditionCheck in table B in one transaction', function(done) {
            var item = {
                    a: {S: helpers.randomString()},
                    c: {S: 'c'}},
                item2 = {a: {S: helpers.randomString()}, b: {S: helpers.randomString()}, g: {N: '23'}},
                transactReq = {TransactItems: []}

            transactReq.TransactItems = [
                {
                    Put: {
                        TableName: helpers.testHashTable,
                        Item: item
                    }
                },
                {
                    ConditionCheck: {
                        TableName: helpers.testRangeTable,
                        Key: {a: item2.a, b: item2.b},
                        ConditionExpression: 'attribute_not_exists(c)'
                    }
                }
            ]

            request(helpers.opts('PutItem', {TableName: helpers.testRangeTable, Item: item2}), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)
                request(opts(transactReq), function(err, res) {
                    if (err) return done(err)
                    res.statusCode.should.equal(200)
                    request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item.a}, ConsistentRead: true}), function(err, res) {
                        if (err) return done(err)
                        res.statusCode.should.equal(200)
                        res.body.Item.should.eql(item)
                        done()
                    })
                })
            })
        })

        it('should fail to write in table A with failed ConditionCheck in table B in one transaction', function(done) {
            var item = {
                    a: {S: helpers.randomString()},
                    c: {S: 'c'}},
                item2 = {a: {S: helpers.randomString()}, b: {S: helpers.randomString()}, g: {N: '23'}},
                transactReq = {TransactItems: []}

            transactReq.TransactItems = [
                {
                    Put: {
                        TableName: helpers.testHashTable,
                        Item: item
                    }
                },
                {
                    ConditionCheck: {
                        TableName: helpers.testRangeTable,
                        Key: {a: item2.a, b: item2.b},
                        ConditionExpression: 'attribute_not_exists(g)'
                    }
                }
            ]

            request(helpers.opts('PutItem', {TableName: helpers.testRangeTable, Item: item2}), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)
                request(opts(transactReq), function(err, res) {
                    if (err) return done(err)
                    res.statusCode.should.equal(400)
                    res.body.message.should.equal('The conditional request failed')
                    request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item.a}, ConsistentRead: true}), function(err, res) {
                        if (err) return done(err)
                        res.statusCode.should.equal(200)
                        res.body.should.eql({})
                        done()
                    })
                })
            })
        })

        it('should write to two different tables', function(done) {
            var hashItem = {a: {S: helpers.randomString()}, c: {S: 'c'}},
                rangeItem = {a: {S: helpers.randomString()}, b: {S: helpers.randomString()}, g: {N: '23'}}
            transactReq = {TransactItems: []}
            transactReq.TransactItems = [{Put: {TableName: helpers.testHashTable, Item: hashItem}}, {Put: {TableName: helpers.testRangeTable, Item: rangeItem}}]
            request(opts(transactReq), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)
                res.body.should.eql({UnprocessedItems: {}})
                request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: hashItem.a}, ConsistentRead: true}), function(err, res) {
                    if (err) return done(err)
                    res.statusCode.should.equal(200)
                    res.body.should.eql({Item: hashItem})
                    request(helpers.opts('GetItem', {TableName: helpers.testRangeTable, Key: {a: rangeItem.a, b: rangeItem.b}, ConsistentRead: true}), function(err, res) {
                        if (err) return done(err)
                        res.statusCode.should.equal(200)
                        res.body.should.eql({Item: rangeItem})
                        done()
                    })
                })
            })
        })

        it('should return ConsumedCapacity from each specified table when putting and deleting small item', function(done) {
            var a = helpers.randomString(), b = new Array(1010 - a.length).join('b'),
                item = {a: {S: a}, b: {S: b}, c: {N: '12.3456'}, d: {B: 'AQI='}, e: {BS: ['AQI=', 'Ag==', 'AQ==']}},
                key2 = helpers.randomString(), key3 = helpers.randomNumber(),
                batchReq = {TransactItems: {}, ReturnConsumedCapacity: 'TOTAL'}
            batchReq.TransactItems = [
                {Put: {Item: item, TableName: helpers.testHashTable}},
                {Put: {Item: {a: {S: key2}}, TableName: helpers.testHashTable}},
                {Put: {Item: {a: {N: key3}}, TableName: helpers.testHashNTable}}
            ]
            request(opts(batchReq), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)
                res.body.ConsumedCapacity.should.containEql({CapacityUnits: 4, TableName: helpers.testHashTable})
                res.body.ConsumedCapacity.should.containEql({CapacityUnits: 2, TableName: helpers.testHashNTable})
                batchReq.ReturnConsumedCapacity = 'INDEXES'
                request(opts(batchReq), function(err, res) {
                    if (err) return done(err)
                    res.statusCode.should.equal(200)
                    res.body.ConsumedCapacity.should.containEql({CapacityUnits: 4, Table: {CapacityUnits: 4}, TableName: helpers.testHashTable})
                    res.body.ConsumedCapacity.should.containEql({CapacityUnits: 2, Table: {CapacityUnits: 2}, TableName: helpers.testHashNTable})
                    batchReq.ReturnConsumedCapacity = 'TOTAL'
                    batchReq.TransactItems[helpers.testHashTable] = [{DeleteRequest: {Key: {a: item.a}}}, {DeleteRequest: {Key: {a: {S: key2}}}}]
                    batchReq.TransactItems[helpers.testHashNTable] = [{DeleteRequest: {Key: {a: {N: key3}}}}]
                    request(opts(batchReq), function(err, res) {
                        if (err) return done(err)
                        res.statusCode.should.equal(200)
                        res.body.ConsumedCapacity.should.containEql({CapacityUnits: 4, TableName: helpers.testHashTable})
                        res.body.ConsumedCapacity.should.containEql({CapacityUnits: 2, TableName: helpers.testHashNTable})
                        batchReq.ReturnConsumedCapacity = 'INDEXES'
                        request(opts(batchReq), function(err, res) {
                            if (err) return done(err)
                            res.statusCode.should.equal(200)
                            res.body.ConsumedCapacity.should.containEql({CapacityUnits: 4, Table: {CapacityUnits: 4}, TableName: helpers.testHashTable})
                            res.body.ConsumedCapacity.should.containEql({CapacityUnits: 2, Table: {CapacityUnits: 2}, TableName: helpers.testHashNTable})
                            done()
                        })
                    })
                })
            })
        })

        it('should return ConsumedCapacity from each specified table when putting and deleting larger item', function(done) {
            var a = helpers.randomString(), b = new Array(1012 - a.length).join('b'),
                item = {a: {S: a}, b: {S: b}, c: {N: '12.3456'}, d: {B: 'AQI='}, e: {BS: ['AQI=', 'Ag==']}},
                key2 = helpers.randomString(), key3 = helpers.randomNumber(),
                batchReq = {TransactItems: [], ReturnConsumedCapacity: 'TOTAL'}
            batchReq.TransactItems = [
                {Put: {Item: item, TableName: helpers.testHashTable}},
                {Put: {Item: {a: {S: key2}}, TableName: helpers.testHashTable}},
                {Put: {Item: {a: {N: key3}}, TableName: helpers.testHashNTable}}
            ]
            request(opts(batchReq), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)
                res.body.ConsumedCapacity.should.containEql({CapacityUnits: 5, TableName: helpers.testHashTable})
                res.body.ConsumedCapacity.should.containEql({CapacityUnits: 2, TableName: helpers.testHashNTable})
                batchReq.ReturnConsumedCapacity = 'INDEXES'
                request(opts(batchReq), function(err, res) {
                    if (err) return done(err)
                    res.statusCode.should.equal(200)
                    res.body.ConsumedCapacity.should.containEql({CapacityUnits: 5, Table: {CapacityUnits: 5}, TableName: helpers.testHashTable})
                    res.body.ConsumedCapacity.should.containEql({CapacityUnits: 2, Table: {CapacityUnits: 2}, TableName: helpers.testHashNTable})
                    batchReq.ReturnConsumedCapacity = 'TOTAL'
                    batchReq.TransactItems[helpers.testHashTable] = [{DeleteRequest: {Key: {a: item.a}}}, {DeleteRequest: {Key: {a: {S: key2}}}}]
                    batchReq.TransactItems[helpers.testHashNTable] = [{DeleteRequest: {Key: {a: {N: key3}}}}]
                    request(opts(batchReq), function(err, res) {
                        if (err) return done(err)
                        res.statusCode.should.equal(200)
                        res.body.ConsumedCapacity.should.containEql({CapacityUnits: 5, TableName: helpers.testHashTable})
                        res.body.ConsumedCapacity.should.containEql({CapacityUnits: 2, TableName: helpers.testHashNTable})
                        batchReq.ReturnConsumedCapacity = 'INDEXES'
                        request(opts(batchReq), function(err, res) {
                            if (err) return done(err)
                            res.statusCode.should.equal(200)
                            res.body.ConsumedCapacity.should.containEql({CapacityUnits: 5, Table: {CapacityUnits: 5}, TableName: helpers.testHashTable})
                            res.body.ConsumedCapacity.should.containEql({CapacityUnits: 2, Table: {CapacityUnits: 2}, TableName: helpers.testHashNTable})
                            done()
                        })
                    })
                })
            })
        })

        it('should complete successfully with updates and atomic counter decrement', function (done) {
            var atomicCounter = {
                a: {
                    S: 'atomicCounter'
                },
                counter: {
                    N: '1'
                }
            }

            var item = {
                a: {
                    S: 'item'
                },
                b: {
                    M: {
                        'one': {
                            S: 'itemname',
                        },
                        'two': {
                            N: '123456'
                        }
                    }
                }
            }

            var transaction = {
                TransactItems: [{
                    Update: {
                        TableName: helpers.testHashTable,
                        Key: {
                            a: atomicCounter.a
                        },
                        UpdateExpression: `SET #counter = if_not_exists(#counter, :zero) + :increment`,
                        ExpressionAttributeNames: {
                            '#counter': 'counter'
                        },
                        ExpressionAttributeValues: {
                            ':increment': {
                                N: '-1'
                        },
                            ':zero': {
                                N: '0'
                            }
                        }
                    }
                },
                    {
                    Update: {
                        TableName: helpers.testHashTable,
                        Key: {a: item.a},
                        UpdateExpression: 'SET b = :b',
                        ExpressionAttributeValues: {
                            ':b': {
                                M: {
                                    'one': {
                                        S: 'itemname',
                                    },
                                    'two': {
                                        N: '654321'
                                    }
                                }
                            }
                        }
                    }
                }
                ]
            }

            request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: atomicCounter}), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.eql(200)
                request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err, res) {
                    if (err) return done(err)
                    res.statusCode.should.equal(200)
                    request(opts(transaction), function(err, res) {
                        if (err) return done(err)
                        res.statusCode.should.equal(200)
                        res.body.should.eql({UnprocessedItems: {}})
                        request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item.a}, ConsistentRead: true}), function(err, res) {
                            if (err) return done(err)
                            res.statusCode.should.equal(200)
                            res.body.Item.b.M.should.eql({'one': {
                                    S: 'itemname',
                                },
                                'two': {
                                    N: '654321'
                                }})
                            request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: atomicCounter.a}, ConsistentRead: true}), function(err, res) {
                                if (err) return done(err)
                                res.statusCode.should.equal(200)
                                res.body.Item.counter.should.eql({N: '0'})
                                done()
                            })
                        })
                    })
                })
            })
        })

        it('should update the same row multiple times', function (done) {
            var atomicCounter = {
                a: {
                    S: 'atomicCounter'
                },
                counter: {
                    N: '1'
                }
            }

            var transaction = {
                TransactItems: [{
                    Update: {
                        TableName: helpers.testHashTable,
                        Key: {
                            a: atomicCounter.a
                        },
                        UpdateExpression: `SET #counter = if_not_exists(#counter, :zero) + :increment`,
                        ExpressionAttributeNames: {
                            '#counter': 'counter'
                        },
                        ExpressionAttributeValues: {
                            ':increment': {
                                N: '1'
                            },
                            ':zero': {
                                N: '0'
                            }
                        }
                    }
                }
                ]
            }

            request(opts(transaction), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)
                res.body.should.eql({UnprocessedItems: {}})
                request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: atomicCounter.a}, ConsistentRead: true}), function(err, res) {
                    if (err) return done(err)
                    res.statusCode.should.equal(200)
                    res.body.Item.counter.N.should.eql('1')
                    request(opts(transaction), function(err, res) {
                        if (err) return done(err)
                        res.statusCode.should.equal(200)
                        res.body.should.eql({UnprocessedItems: {}})
                        request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: atomicCounter.a}, ConsistentRead: true}), function(err, res) {
                            if (err) return done(err)
                            res.statusCode.should.equal(200)
                            res.body.Item.counter.N.should.eql('2')
                            done()
                        })
                    })
                })
            })
        })

        it('should update the index', function (done) {
            var key = {a: {S: helpers.randomString()}, b: {S: helpers.randomString()}}
            var transaction = {
                TransactItems: [{
                    Update: {
                        TableName: helpers.testRangeTable,
                        Key: key,
                        UpdateExpression: 'set c = a, d = b, e = a, f = b'
                    }
                }
                ]
            }
            request(opts(transaction), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)
                request(helpers.opts('Query', {
                    TableName: helpers.testRangeTable,
                    ConsistentRead: true,
                    IndexName: 'index1',
                    KeyConditions: {
                        a: {ComparisonOperator: 'EQ', AttributeValueList: [key.a]},
                        c: {ComparisonOperator: 'EQ', AttributeValueList: [key.a]},
                    },
                }), function(err, res) {
                    if (err) return done(err)
                    res.statusCode.should.equal(200)
                    res.body.Items.should.eql([{a: key.a, b: key.b, c: key.a, d: key.b, e: key.a, f: key.b}])
                    request(helpers.opts('Query', {
                        TableName: helpers.testRangeTable,
                        ConsistentRead: true,
                        IndexName: 'index2',
                        KeyConditions: {
                            a: {ComparisonOperator: 'EQ', AttributeValueList: [key.a]},
                            d: {ComparisonOperator: 'EQ', AttributeValueList: [key.b]},
                        },
                    }), function(err, res) {
                        if (err) return done(err)
                        res.statusCode.should.equal(200)
                        res.body.Items.should.eql([{a: key.a, b: key.b, c: key.a, d: key.b}])
                        transaction.TransactItems[0].Update = {
                            TableName: helpers.testRangeTable,
                            Key: key,
                            UpdateExpression: 'set c = b, d = a, e = b, f = a',
                        }
                        request(opts(transaction), function(err, res) {
                            if (err) return done(err)
                            res.statusCode.should.equal(200)
                            request(helpers.opts('Query', {
                                TableName: helpers.testRangeTable,
                                ConsistentRead: true,
                                IndexName: 'index1',
                                KeyConditions: {
                                    a: {ComparisonOperator: 'EQ', AttributeValueList: [key.a]},
                                    c: {ComparisonOperator: 'EQ', AttributeValueList: [key.a]},
                                },
                            }), function(err, res) {
                                if (err) return done(err)
                                res.statusCode.should.equal(200)
                                res.body.Items.should.eql([])
                                request(helpers.opts('Query', {
                                    TableName: helpers.testRangeTable,
                                    ConsistentRead: true,
                                    IndexName: 'index2',
                                    KeyConditions: {
                                        a: {ComparisonOperator: 'EQ', AttributeValueList: [key.a]},
                                        d: {ComparisonOperator: 'EQ', AttributeValueList: [key.b]},
                                    },
                                }), function(err, res) {
                                    if (err) return done(err)
                                    res.statusCode.should.equal(200)
                                    res.body.Items.should.eql([])
                                    request(helpers.opts('Query', {
                                        TableName: helpers.testRangeTable,
                                        ConsistentRead: true,
                                        IndexName: 'index1',
                                        KeyConditions: {
                                            a: {ComparisonOperator: 'EQ', AttributeValueList: [key.a]},
                                            c: {ComparisonOperator: 'EQ', AttributeValueList: [key.b]},
                                        },
                                    }), function(err, res) {
                                        if (err) return done(err)
                                        res.statusCode.should.equal(200)
                                        res.body.Items.should.eql([{a: key.a, b: key.b, c: key.b, d: key.a, e: key.b, f: key.a}])
                                        request(helpers.opts('Query', {
                                            TableName: helpers.testRangeTable,
                                            ConsistentRead: true,
                                            IndexName: 'index2',
                                            KeyConditions: {
                                                a: {ComparisonOperator: 'EQ', AttributeValueList: [key.a]},
                                                d: {ComparisonOperator: 'EQ', AttributeValueList: [key.a]},
                                            },
                                        }), function(err, res) {
                                            if (err) return done(err)
                                            res.statusCode.should.equal(200)
                                            res.body.Items.should.eql([{a: key.a, b: key.b, c: key.b, d: key.a}])
                                            request(helpers.opts('Query', {
                                                TableName: helpers.testRangeTable,
                                                IndexName: 'index3',
                                                KeyConditions: {
                                                    c: {ComparisonOperator: 'EQ', AttributeValueList: [key.b]},
                                                },
                                            }), function(err, res) {
                                                if (err) return done(err)
                                                res.statusCode.should.equal(200)
                                                res.body.Items.should.eql([{a: key.a, b: key.b, c: key.b, d: key.a, e: key.b, f: key.a}])
                                                request(helpers.opts('Query', {
                                                    TableName: helpers.testRangeTable,
                                                    IndexName: 'index4',
                                                    KeyConditions: {
                                                        c: {ComparisonOperator: 'EQ', AttributeValueList: [key.b]},
                                                        d: {ComparisonOperator: 'EQ', AttributeValueList: [key.a]},
                                                    },
                                                }), function(err, res) {
                                                    if (err) return done(err)
                                                    res.statusCode.should.equal(200)
                                                    res.body.Items.should.eql([{a: key.a, b: key.b, c: key.b, d: key.a, e: key.b}])
                                                    done()
                                                })
                                            })
                                        })
                                    })
                                })
                            })
                        })
                    })
                })
            })
        })

        })
    })
})