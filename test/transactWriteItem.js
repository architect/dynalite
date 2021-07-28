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

        it('should write multiple items', function(done) {
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
    //
    //     it('should delete an item from each table', function(done) {
    //         var item = {a: {S: helpers.randomString()}, c: {S: 'c'}},
    //             item2 = {a: {S: helpers.randomString()}, b: {S: helpers.randomString()}, c: {S: 'c'}},
    //             batchReq = {TransactItems: {}}
    //         batchReq.TransactItems[helpers.testHashTable] = [{DeleteRequest: {Key: {a: item.a}}}]
    //         batchReq.TransactItems[helpers.testRangeTable] = [{DeleteRequest: {Key: {a: item2.a, b: item2.b}}}]
    //         request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err, res) {
    //             if (err) return done(err)
    //             res.statusCode.should.equal(200)
    //             request(helpers.opts('PutItem', {TableName: helpers.testRangeTable, Item: item2}), function(err, res) {
    //                 if (err) return done(err)
    //                 res.statusCode.should.equal(200)
    //                 request(opts(batchReq), function(err, res) {
    //                     if (err) return done(err)
    //                     res.statusCode.should.equal(200)
    //                     res.body.should.eql({UnprocessedItems: {}})
    //                     request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item.a}, ConsistentRead: true}), function(err, res) {
    //                         if (err) return done(err)
    //                         res.statusCode.should.equal(200)
    //                         res.body.should.eql({})
    //                         request(helpers.opts('GetItem', {TableName: helpers.testRangeTable, Key: {a: item2.a, b: item2.b}, ConsistentRead: true}), function(err, res) {
    //                             if (err) return done(err)
    //                             res.statusCode.should.equal(200)
    //                             res.body.should.eql({})
    //                             done()
    //                         })
    //                     })
    //                 })
    //             })
    //         })
    //     })
    //
    //     it('should deal with puts and deletes together', function(done) {
    //         var item = {a: {S: helpers.randomString()}, c: {S: 'c'}},
    //             item2 = {a: {S: helpers.randomString()}, c: {S: 'c'}},
    //             batchReq = {TransactItems: {}}
    //         request(helpers.opts('PutItem', {TableName: helpers.testHashTable, Item: item}), function(err, res) {
    //             if (err) return done(err)
    //             res.statusCode.should.equal(200)
    //             batchReq.TransactItems[helpers.testHashTable] = [{DeleteRequest: {Key: {a: item.a}}}, {PutRequest: {Item: item2}}]
    //             request(opts(batchReq), function(err, res) {
    //                 if (err) return done(err)
    //                 res.body.should.eql({UnprocessedItems: {}})
    //                 batchReq.TransactItems[helpers.testHashTable] = [{PutRequest: {Item: item}}, {DeleteRequest: {Key: {a: item2.a}}}]
    //                 request(opts(batchReq), function(err, res) {
    //                     if (err) return done(err)
    //                     res.body.should.eql({UnprocessedItems: {}})
    //                     request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item.a}, ConsistentRead: true}), function(err, res) {
    //                         if (err) return done(err)
    //                         res.statusCode.should.equal(200)
    //                         res.body.should.eql({Item: item})
    //                         request(helpers.opts('GetItem', {TableName: helpers.testHashTable, Key: {a: item2.a}, ConsistentRead: true}), function(err, res) {
    //                             if (err) return done(err)
    //                             res.statusCode.should.equal(200)
    //                             res.body.should.eql({})
    //                             done()
    //                         })
    //                     })
    //                 })
    //             })
    //         })
    //     })
    //
    //     it('should return ConsumedCapacity from each specified table when putting and deleting small item', function(done) {
    //         var a = helpers.randomString(), b = new Array(1010 - a.length).join('b'),
    //             item = {a: {S: a}, b: {S: b}, c: {N: '12.3456'}, d: {B: 'AQI='}, e: {BS: ['AQI=', 'Ag==', 'AQ==']}},
    //             key2 = helpers.randomString(), key3 = helpers.randomNumber(),
    //             batchReq = {TransactItems: {}, ReturnConsumedCapacity: 'TOTAL'}
    //         batchReq.TransactItems[helpers.testHashTable] = [{PutRequest: {Item: item}}, {PutRequest: {Item: {a: {S: key2}}}}]
    //         batchReq.TransactItems[helpers.testHashNTable] = [{PutRequest: {Item: {a: {N: key3}}}}]
    //         request(opts(batchReq), function(err, res) {
    //             if (err) return done(err)
    //             res.statusCode.should.equal(200)
    //             res.body.ConsumedCapacity.should.containEql({CapacityUnits: 2, TableName: helpers.testHashTable})
    //             res.body.ConsumedCapacity.should.containEql({CapacityUnits: 1, TableName: helpers.testHashNTable})
    //             batchReq.ReturnConsumedCapacity = 'INDEXES'
    //             request(opts(batchReq), function(err, res) {
    //                 if (err) return done(err)
    //                 res.statusCode.should.equal(200)
    //                 res.body.ConsumedCapacity.should.containEql({CapacityUnits: 2, Table: {CapacityUnits: 2}, TableName: helpers.testHashTable})
    //                 res.body.ConsumedCapacity.should.containEql({CapacityUnits: 1, Table: {CapacityUnits: 1}, TableName: helpers.testHashNTable})
    //                 batchReq.ReturnConsumedCapacity = 'TOTAL'
    //                 batchReq.TransactItems[helpers.testHashTable] = [{DeleteRequest: {Key: {a: item.a}}}, {DeleteRequest: {Key: {a: {S: key2}}}}]
    //                 batchReq.TransactItems[helpers.testHashNTable] = [{DeleteRequest: {Key: {a: {N: key3}}}}]
    //                 request(opts(batchReq), function(err, res) {
    //                     if (err) return done(err)
    //                     res.statusCode.should.equal(200)
    //                     res.body.ConsumedCapacity.should.containEql({CapacityUnits: 2, TableName: helpers.testHashTable})
    //                     res.body.ConsumedCapacity.should.containEql({CapacityUnits: 1, TableName: helpers.testHashNTable})
    //                     batchReq.ReturnConsumedCapacity = 'INDEXES'
    //                     request(opts(batchReq), function(err, res) {
    //                         if (err) return done(err)
    //                         res.statusCode.should.equal(200)
    //                         res.body.ConsumedCapacity.should.containEql({CapacityUnits: 2, Table: {CapacityUnits: 2}, TableName: helpers.testHashTable})
    //                         res.body.ConsumedCapacity.should.containEql({CapacityUnits: 1, Table: {CapacityUnits: 1}, TableName: helpers.testHashNTable})
    //                         done()
    //                     })
    //                 })
    //             })
    //         })
    //     })
    //
    //     it('should return ConsumedCapacity from each specified table when putting and deleting larger item', function(done) {
    //         var a = helpers.randomString(), b = new Array(1012 - a.length).join('b'),
    //             item = {a: {S: a}, b: {S: b}, c: {N: '12.3456'}, d: {B: 'AQI='}, e: {BS: ['AQI=', 'Ag==']}},
    //             key2 = helpers.randomString(), key3 = helpers.randomNumber(),
    //             batchReq = {TransactItems: {}, ReturnConsumedCapacity: 'TOTAL'}
    //         batchReq.TransactItems[helpers.testHashTable] = [{PutRequest: {Item: item}}, {PutRequest: {Item: {a: {S: key2}}}}]
    //         batchReq.TransactItems[helpers.testHashNTable] = [{PutRequest: {Item: {a: {N: key3}}}}]
    //         request(opts(batchReq), function(err, res) {
    //             if (err) return done(err)
    //             res.statusCode.should.equal(200)
    //             res.body.ConsumedCapacity.should.containEql({CapacityUnits: 3, TableName: helpers.testHashTable})
    //             res.body.ConsumedCapacity.should.containEql({CapacityUnits: 1, TableName: helpers.testHashNTable})
    //             batchReq.ReturnConsumedCapacity = 'INDEXES'
    //             request(opts(batchReq), function(err, res) {
    //                 if (err) return done(err)
    //                 res.statusCode.should.equal(200)
    //                 res.body.ConsumedCapacity.should.containEql({CapacityUnits: 3, Table: {CapacityUnits: 3}, TableName: helpers.testHashTable})
    //                 res.body.ConsumedCapacity.should.containEql({CapacityUnits: 1, Table: {CapacityUnits: 1}, TableName: helpers.testHashNTable})
    //                 batchReq.ReturnConsumedCapacity = 'TOTAL'
    //                 batchReq.TransactItems[helpers.testHashTable] = [{DeleteRequest: {Key: {a: item.a}}}, {DeleteRequest: {Key: {a: {S: key2}}}}]
    //                 batchReq.TransactItems[helpers.testHashNTable] = [{DeleteRequest: {Key: {a: {N: key3}}}}]
    //                 request(opts(batchReq), function(err, res) {
    //                     if (err) return done(err)
    //                     res.statusCode.should.equal(200)
    //                     res.body.ConsumedCapacity.should.containEql({CapacityUnits: 3, TableName: helpers.testHashTable})
    //                     res.body.ConsumedCapacity.should.containEql({CapacityUnits: 1, TableName: helpers.testHashNTable})
    //                     batchReq.ReturnConsumedCapacity = 'INDEXES'
    //                     request(opts(batchReq), function(err, res) {
    //                         if (err) return done(err)
    //                         res.statusCode.should.equal(200)
    //                         res.body.ConsumedCapacity.should.containEql({CapacityUnits: 2, Table: {CapacityUnits: 2}, TableName: helpers.testHashTable})
    //                         res.body.ConsumedCapacity.should.containEql({CapacityUnits: 1, Table: {CapacityUnits: 1}, TableName: helpers.testHashNTable})
    //                         done()
    //                     })
    //                 })
    //             })
    //         })
    //     })
    //
    //
    //     // All capacities seem to have a burst rate of 300x => full recovery is 300sec
    //     // Max size = 1638400 = 25 * 65536 = 1600 capacity units
    //     // Will process all if capacity >= 751. Below this value, the algorithm is something like:
    //     // min(capacity * 300, min(capacity, 336) + 677) + random(mean = 80, stddev = 32)
    //     it.skip('should return UnprocessedItems if over limit', function(done) {
    //         this.timeout(1e8)
    //
    //         var CAPACITY = 3
    //
    //         async.times(10, createAndWrite, done)
    //
    //         function createAndWrite(i, cb) {
    //             var name = helpers.randomName(), table = {
    //                 TableName: name,
    //                 AttributeDefinitions: [{AttributeName: 'a', AttributeType: 'S'}],
    //                 KeySchema: [{KeyType: 'HASH', AttributeName: 'a'}],
    //                 ProvisionedThroughput: {ReadCapacityUnits: CAPACITY, WriteCapacityUnits: CAPACITY},
    //             }
    //             helpers.createAndWait(table, function(err) {
    //                 if (err) return cb(err)
    //                 async.timesSeries(50, function(n, cb) { batchWrite(name, n, cb) }, cb)
    //             })
    //         }
    //
    //         function batchWrite(name, n, cb) {
    //             var i, item, items = [], totalSize = 0, batchReq = {TransactItems: {}, ReturnConsumedCapacity: 'TOTAL'}
    //
    //             for (i = 0; i < 25; i++) {
    //                 item = {a: {S: ('0' + i).slice(-2)},
    //                     b: {S: new Array(Math.floor((64 - (16 * Math.random())) * 1024) - 3).join('b')}}
    //                 totalSize += db.itemSize(item)
    //                 items.push({PutRequest: {Item: item}})
    //             }
    //
    //             batchReq.TransactItems[name] = items
    //             request(opts(batchReq), function(err, res) {
    //                 // if (err) return cb(err)
    //                 if (err) {
    //                     // console.log('Caught err: ' + err)
    //                     return cb()
    //                 }
    //                 if (/ProvisionedThroughputExceededException$/.test(res.body.__type)) {
    //                     // console.log('ProvisionedThroughputExceededException$')
    //                     return cb()
    //                 } else if (res.body.__type) {
    //                     // return cb(new Error(JSON.stringify(res.body)))
    //                     return cb()
    //                 }
    //                 res.statusCode.should.equal(200)
    //                 // eslint-disable-next-line no-console
    //                 console.log([CAPACITY, res.body.ConsumedCapacity[0].CapacityUnits, totalSize].join())
    //                 setTimeout(cb, res.body.ConsumedCapacity[0].CapacityUnits * 1000 / CAPACITY)
    //             })
    //         }
        })
    })
})