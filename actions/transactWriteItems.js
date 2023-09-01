var async = require('async'),
    db = require('../db')

module.exports = function transactWriteItem(store, data, cb) {
    var seenKeys = {}
    var batchOpts = {}
    var releaseLocks = []
    var indexUpdates = {}

    async.eachSeries(data.TransactItems, addActions, function (err) {
        if (err) {
            if (err.body && (/Missing the key/.test(err.body.message) || /Type mismatch for key/.test(err.body.message)))
                err.body.message = 'The provided key element does not match the schema'
            return cb(err)
        }

        // this does NOT ensure atomicity across tables - but the items on each table are already locked
        // and the actions have been validated. at this point the only thing that would fail would be the
        // database itself, and that's a lot of work to get around so I'm just ¯\_(ツ)_/¯ not gonna do that
        var operationsbyTable = Object.entries(batchOpts)

        async.each(operationsbyTable, function([tableName, ops], callback) {
            var itemDb = store.getItemDb(tableName)

            store.getTable(tableName, function(err, table) {
                indexUpdates[tableName].forEach(update => {
                    db.updateIndexes(store, table, update.existingItem, update.item, function(err) {
                        if (err) return callback(err)
                    })
                })
                itemDb.batch(ops, function(err, results) {
                    if (err) callback(err)
                    callback(results)
                })
            })
        }, function(err) {
            releaseLocks.forEach(release => release()())
            if (err) return cb(err)

            var res = {UnprocessedItems: {}}, tableUnits = {}

            if (~['TOTAL', 'INDEXES'].indexOf(data.ReturnConsumedCapacity)) {
                operationsbyTable.forEach(([table, operations]) => {
                    tableUnits[table] = 0
                    operations.forEach(op => {
                        let readCapacity = db.capacityUnits(op.value, true, true)
                        let writeCapacity = db.capacityUnits(op.value, false, true)
                        tableUnits[table] += readCapacity + writeCapacity
                    })
                })
                res.ConsumedCapacity = Object.keys(tableUnits).map(function (table) {
                    return {
                        CapacityUnits: tableUnits[table],
                        TableName: table,
                        Table: data.ReturnConsumedCapacity == 'INDEXES' ? {CapacityUnits: tableUnits[table]} : undefined,
                    }
                })
            }

            cb(null, res)
        })
    })

    function addActions(transactItem, cb) {
        var options = {}
        var tableName

        if (data.ReturnConsumedCapacity) options.ReturnConsumedCapacity = data.ReturnConsumedCapacity

        if (transactItem.Put) {
            tableName = transactItem.Put.TableName

            store.getTable(tableName, function (err, table) {
                if (err) return cb(err)
                if ((err = db.validateItem(transactItem.Put.Item, table)) != null) return cb(err)

                let value = transactItem.Put.Item
                let key = db.createKey(transactItem.Put.Item, table)
                if (seenKeys[key]) {
                    return cb(db.transactionCancelledException('Transaction cancelled, please refer cancellation reasons for specific reasons'))
                }

                seenKeys[key] = true

                var itemDb = store.getItemDb(tableName)

                itemDb.get(key, function(err, oldItem) {
                    if (oldItem) {
                        itemDb.lock(key, function(release) {
                            releaseLocks.push(release)
                        })
                    }

                    if (err && err.name != 'NotFoundError') return cb(err)

                    if ((err = db.checkConditional(transactItem.Put, oldItem)) != null) return cb(err)

                    let operation = {
                        type: 'put',
                        key,
                        value
                    }

                    let indexUpdate = {
                        existingItem: oldItem,
                        item: value
                    }

                    if (batchOpts[tableName]) {
                        batchOpts[tableName].push(operation)
                        indexUpdates[tableName].push(indexUpdate)
                    } else {
                        batchOpts[tableName] = [operation]
                        indexUpdates[tableName] = [indexUpdate]
                    }

                    return cb()
                })
            })
        } else if (transactItem.Delete) {
            tableName = transactItem.Delete.TableName

            store.getTable(tableName, function (err, table) {
                if (err) return cb(err)
                if ((err = db.validateKey(transactItem.Delete.Key, table) != null)) return cb(err)

                let key = db.createKey(transactItem.Delete.Key, table)
                if (seenKeys[key]) {
                    return cb(db.transactionCancelledException('Transaction cancelled, please refer cancellation reasons for specific reasons'))
                }

                seenKeys[key] = true

                var itemDb = store.getItemDb(tableName)

                itemDb.lock(key, function(release) {
                    releaseLocks.push(release)
                    itemDb.get(key, function(err, oldItem) {
                        if (err && err.name != 'NotFoundError') return cb(err)

                        if ((err = db.checkConditional(transactItem.Delete, oldItem)) != null) return cb(err)

                        let operation = {
                            type: 'del',
                            key
                        }

                        let indexUpdate = {
                            existingItem: oldItem
                        }

                        if (batchOpts[tableName]) {
                            batchOpts[tableName].push(operation)
                            indexUpdates[tableName].push(indexUpdate)
                        } else {
                            batchOpts[tableName] = [operation]
                            indexUpdates[tableName] = [indexUpdate]
                        }
                        return cb()
                    })
                })
            })
        } else if (transactItem.Update) {
            tableName = transactItem.Update.TableName

            store.getTable(tableName, function (err, table) {
                if (err) return cb(err)

                if ((err = db.validateUpdates(transactItem.Update.AttributeUpdates, transactItem.Update._updates, table)) != null) return cb(err)

                let key = db.createKey(transactItem.Update.Key, table)
                if (seenKeys[key]) {
                    return cb(db.transactionCancelledException('Transaction cancelled, please refer cancellation reasons for specific reasons'))
                }

                seenKeys[key] = true

                var itemDb = store.getItemDb(tableName)

                itemDb.lock(key, function(release) {
                    releaseLocks.push(release)
                    itemDb.get(key, function(err, oldItem) {
                        if (err && err.name != 'NotFoundError') return cb(err)

                        if ((err = db.checkConditional(transactItem.Update, oldItem)) != null) return cb(err)

                        var item = transactItem.Update.Key

                        if (oldItem) {
                            item = db.deepClone(oldItem)
                        }

                        err = transactItem.Update._updates ? db.applyUpdateExpression(transactItem.Update._updates.sections, table, item) :
                            db.applyAttributeUpdates(transactItem.Update.AttributeUpdates, table, item)
                        if (err) return cb(err)

                        if (db.itemSize(item) > store.options.maxItemSize)
                            return cb(db.validationError('Item size to update has exceeded the maximum allowed size'))

                        let operation = {
                            type: 'put',
                            key,
                            value: item
                        }

                        let indexUpdate = {
                            existingItem: oldItem,
                            item: item
                        }

                        if (batchOpts[tableName]) {
                            batchOpts[tableName].push(operation)
                            indexUpdates[tableName].push(indexUpdate)
                        } else {
                            batchOpts[tableName] = [operation]
                            indexUpdates[tableName] = [indexUpdate]
                        }

                        return cb()
                    })
                })
            })
        } else if (transactItem.ConditionCheck) {
            tableName = transactItem.ConditionCheck.TableName

            store.getTable(tableName, function (err, table) {
                if (err) return cb(err)

                let key = db.createKey(transactItem.ConditionCheck.Key, table)
                if (seenKeys[key]) {
                    return cb(db.transactionCancelledException('Transaction cancelled, please refer cancellation reasons for specific reasons'))
                }

                seenKeys[key] = true

                var itemDb = store.getItemDb(tableName)

                itemDb.lock(key, function(release) {
                    releaseLocks.push(release)
                    itemDb.get(key, function(err, oldItem) {
                        if (err && err.name != 'NotFoundError') return cb(err)

                        if ((err = db.checkConditional(transactItem.ConditionCheck, oldItem)) != null) return cb(err)

                        return cb()
                    })
                })
            })
        }
    }
}
