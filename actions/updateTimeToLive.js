var db = require('../db');

module.exports = function updateTimeToLive(store, data, cb) {
    var key = data.TableName,
     TimeToLiveSpecification = data.TimeToLiveSpecification,
     tableDb = store.tableDb,
        returnValue;

    store.getTable(key, false, function(err, table) {
        if (err) return cb(err)

        if (TimeToLiveSpecification.Enabled) {
            if (table.TimeToLiveDescription && table.TimeToLiveDescription.TimeToLiveStatus === 'ENABLED') {
                return cb(db.validationError('TimeToLive is already enabled'))
            }
            table.TimeToLiveDescription = {
                AttributeName: TimeToLiveSpecification.AttributeName,
                TimeToLiveStatus: 'ENABLED',
            }
            returnValue = TimeToLiveSpecification
        } else {
            if (table.TimeToLiveDescription == null || table.TimeToLiveDescription.TimeToLiveStatus === 'DISABLED') {
                return cb(db.validationError('TimeToLive is already disabled'))
            }

            table.TimeToLiveDescription = {
                TimeToLiveStatus: 'DISABLED',
            }
            returnValue = {Enabled: false}
        }

        tableDb.put(key, table, function(err) {
            if (err) return cb(err)

            cb(null, {TimeToLiveSpecification: returnValue})
        })
    })
}
