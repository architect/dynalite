const test = require('tape')
// const should = require('should') // No longer needed after conversion to tape assertions
const helpers = require('./helpers')

const target = 'UpdateTable'
// Bind helper functions
const request = helpers.request
const opts = helpers.opts.bind(null, target)
// const assertType = helpers.assertType.bind(null, target) // Unused in part3
// const assertValidation = helpers.assertValidation.bind(null, target) // Unused in part3
// const assertNotFound = helpers.assertNotFound.bind(null, target) // Unused in part3

test('updateTable - functionality', (t) => {

  t.test('should triple rates and then reduce if requested', (st) => {
    // Timeout removed
    const oldRead = helpers.readCapacity
    const oldWrite = helpers.writeCapacity
    const newRead = oldRead * 3
    const newWrite = oldWrite * 3
    let increaseTimestamp = Date.now() / 1000
    const throughput = { ReadCapacityUnits: newRead, WriteCapacityUnits: newWrite }

    request(opts({ TableName: helpers.testHashTable, ProvisionedThroughput: throughput }), (err, res) => {
      st.error(err, 'Initial UpdateTable request should not error')
      if (!res) return st.end('No response from initial UpdateTable')
      st.equal(res.statusCode, 200, 'Initial UpdateTable status code should be 200')

      const desc = res.body.TableDescription
      st.deepEqual(desc.AttributeDefinitions, [ { AttributeName: 'a', AttributeType: 'S' } ], 'AttributeDefinitions should match')
      st.ok(desc.CreationDateTime < (Date.now() / 1000), 'CreationDateTime seems valid')
      st.ok(desc.ItemCount >= 0, 'ItemCount should be non-negative')
      st.deepEqual(desc.KeySchema, [ { AttributeName: 'a', KeyType: 'HASH' } ], 'KeySchema should match')
      st.ok(desc.ProvisionedThroughput.LastIncreaseDateTime >= (increaseTimestamp - 5), 'LastIncreaseDateTime should be recent')
      st.ok(desc.ProvisionedThroughput.NumberOfDecreasesToday >= 0, 'NumberOfDecreasesToday should be non-negative')
      st.equal(desc.ProvisionedThroughput.ReadCapacityUnits, oldRead, 'ReadCapacityUnits should still be old value during update')
      st.equal(desc.ProvisionedThroughput.WriteCapacityUnits, oldWrite, 'WriteCapacityUnits should still be old value during update')
      st.equal(desc.TableName, helpers.testHashTable, 'TableName should match')
      st.ok(desc.TableSizeBytes >= 0, 'TableSizeBytes should be non-negative')
      st.equal(desc.TableStatus, 'UPDATING', 'TableStatus should be UPDATING')

      const numDecreases = desc.ProvisionedThroughput.NumberOfDecreasesToday
      increaseTimestamp = desc.ProvisionedThroughput.LastIncreaseDateTime // Update timestamp from response

      helpers.waitUntilActive(helpers.testHashTable, (errWaitActive1, resWaitActive1) => {
        st.error(errWaitActive1, 'waitUntilActive (1) should succeed')
        if (!resWaitActive1) return st.end('No response from waitUntilActive (1)')

        let decreaseTimestamp = Date.now() / 1000
        let descActive1 = resWaitActive1.body.Table
        st.equal(descActive1.ProvisionedThroughput.ReadCapacityUnits, newRead, 'ReadCapacityUnits should be updated after active')
        st.equal(descActive1.ProvisionedThroughput.WriteCapacityUnits, newWrite, 'WriteCapacityUnits should be updated after active')
        st.ok(descActive1.ProvisionedThroughput.LastIncreaseDateTime >= increaseTimestamp, 'LastIncreaseDateTime should be updated')

        increaseTimestamp = descActive1.ProvisionedThroughput.LastIncreaseDateTime // Update timestamp again

        const revertThroughput = { ReadCapacityUnits: oldRead, WriteCapacityUnits: oldWrite }
        request(opts({ TableName: helpers.testHashTable, ProvisionedThroughput: revertThroughput }), (errRevert, resRevert) => {
          st.error(errRevert, 'Second UpdateTable request should not error')
          if (!resRevert) return st.end('No response from second UpdateTable')
          st.equal(resRevert.statusCode, 200, 'Second UpdateTable status code should be 200')

          const descRevert = resRevert.body.TableDescription
          st.equal(descRevert.ProvisionedThroughput.LastIncreaseDateTime, increaseTimestamp, 'LastIncreaseDateTime should be unchanged during decrease')
          st.ok(descRevert.ProvisionedThroughput.LastDecreaseDateTime >= (decreaseTimestamp - 5), 'LastDecreaseDateTime should be recent')
          st.equal(descRevert.ProvisionedThroughput.NumberOfDecreasesToday, numDecreases, 'NumberOfDecreasesToday should be unchanged during update')
          st.equal(descRevert.ProvisionedThroughput.ReadCapacityUnits, newRead, 'ReadCapacityUnits should still be new value during decrease update')
          st.equal(descRevert.ProvisionedThroughput.WriteCapacityUnits, newWrite, 'WriteCapacityUnits should still be new value during decrease update')
          st.equal(descRevert.TableStatus, 'UPDATING', 'TableStatus should be UPDATING again')

          decreaseTimestamp = descRevert.ProvisionedThroughput.LastDecreaseDateTime // Update timestamp

          helpers.waitUntilActive(helpers.testHashTable, (errWaitActive2, resWaitActive2) => {
            st.error(errWaitActive2, 'waitUntilActive (2) should succeed')
            if (!resWaitActive2) return st.end('No response from waitUntilActive (2)')

            const descActive2 = resWaitActive2.body.Table
            st.equal(descActive2.ProvisionedThroughput.LastIncreaseDateTime, increaseTimestamp, 'LastIncreaseDateTime should remain the same')
            st.ok(descActive2.ProvisionedThroughput.LastDecreaseDateTime >= decreaseTimestamp, 'LastDecreaseDateTime should be updated')
            st.equal(descActive2.ProvisionedThroughput.NumberOfDecreasesToday, numDecreases + 1, 'NumberOfDecreasesToday should be incremented')
            st.equal(descActive2.ProvisionedThroughput.ReadCapacityUnits, oldRead, 'ReadCapacityUnits should be reverted')
            st.equal(descActive2.ProvisionedThroughput.WriteCapacityUnits, oldWrite, 'WriteCapacityUnits should be reverted')

            st.end() // End of test flow
          })
        })
      })
    })
  })

  // XXX: this takes more than 20 mins to run - keeping skipped
  /*
  t.test.skip('should allow table to be converted to PAY_PER_REQUEST and back again', (st) => {
    // Timeout removed
    const read = helpers.readCapacity;
    const write = helpers.writeCapacity;
    const throughput = { ReadCapacityUnits: read, WriteCapacityUnits: write };
    let decreaseTimestamp = Date.now() / 1000;

    request(opts({ TableName: helpers.testRangeTable, BillingMode: 'PAY_PER_REQUEST' }), (errPPR, resPPR) => {
      st.error(errPPR, 'UpdateTable to PPR should not error');
      if (!resPPR) return st.end('No response from UpdateTable to PPR');
      st.equal(resPPR.statusCode, 200, 'UpdateTable to PPR status code should be 200');

      const descPPR = resPPR.body.TableDescription;
      st.equal(descPPR.TableStatus, 'UPDATING', 'TableStatus should be UPDATING after PPR request');
      st.deepEqual(descPPR.BillingModeSummary, { BillingMode: 'PAY_PER_REQUEST' }, 'BillingModeSummary should reflect PPR');
      // Original test checked TableThroughputModeSummary, but this might not exist or be standard?
      // st.deepEqual(descPPR.TableThroughputModeSummary, { TableThroughputMode: 'PAY_PER_REQUEST' }, 'TableThroughputModeSummary PPR');
      st.ok(descPPR.ProvisionedThroughput.LastDecreaseDateTime >= (decreaseTimestamp - 5), 'PPR LastDecreaseDateTime should be recent');
      st.ok(descPPR.ProvisionedThroughput.NumberOfDecreasesToday >= 0, 'PPR NumberOfDecreasesToday');
      st.equal(descPPR.ProvisionedThroughput.ReadCapacityUnits, 0, 'PPR ReadCapacityUnits should be 0');
      st.equal(descPPR.ProvisionedThroughput.WriteCapacityUnits, 0, 'PPR WriteCapacityUnits should be 0');

      descPPR.GlobalSecondaryIndexes.forEach((index) => {
        st.equal(index.IndexStatus, 'UPDATING', `GSI ${index.IndexName} status should be UPDATING`);
        st.deepEqual(index.ProvisionedThroughput, {
          NumberOfDecreasesToday: 0, // GSI decreases might have different tracking
          ReadCapacityUnits: 0,
          WriteCapacityUnits: 0,
        }, `GSI ${index.IndexName} throughput should be zeroed`);
      });

      helpers.waitUntilActive(helpers.testRangeTable, (errWaitActivePPR, resWaitActivePPR) => {
        st.error(errWaitActivePPR, 'waitUntilActive (PPR) should succeed');
        if (!resWaitActivePPR) return st.end('No response from waitUntilActive (PPR)');

        const descActivePPR = resWaitActivePPR.body.Table;
        st.equal(descActivePPR.BillingModeSummary.BillingMode, 'PAY_PER_REQUEST', 'Active BillingMode should be PPR');
        st.ok(descActivePPR.BillingModeSummary.LastUpdateToPayPerRequestDateTime >= (decreaseTimestamp - 5), 'Active LastUpdateToPayPerRequestDateTime');
        // Check TableThroughputModeSummary if it exists in the response
        if (descActivePPR.TableThroughputModeSummary) {
          st.equal(descActivePPR.TableThroughputModeSummary.TableThroughputMode, 'PAY_PER_REQUEST', 'Active TableThroughputMode');
          st.ok(descActivePPR.TableThroughputModeSummary.LastUpdateToPayPerRequestDateTime >= (decreaseTimestamp - 5), 'Active LastUpdateToPayPerRequestDateTime (Throughput)');
        }
        st.ok(descActivePPR.ProvisionedThroughput.NumberOfDecreasesToday >= 0, 'Active PPR NumberOfDecreasesToday');
        st.equal(descActivePPR.ProvisionedThroughput.ReadCapacityUnits, 0, 'Active PPR ReadCapacityUnits');
        st.equal(descActivePPR.ProvisionedThroughput.WriteCapacityUnits, 0, 'Active PPR WriteCapacityUnits');

        descActivePPR.GlobalSecondaryIndexes.forEach((index) => {
          // Should might fail here if LastDecreaseDateTime is not set for GSI on PPR conversion, adjust if needed
          st.ok(index.ProvisionedThroughput.LastDecreaseDateTime >= (decreaseTimestamp - 5), `Active GSI ${index.IndexName} LastDecreaseDateTime`);
          st.ok(index.ProvisionedThroughput.NumberOfDecreasesToday > 0, `Active GSI ${index.IndexName} NumberOfDecreasesToday`); // Expecting > 0 now
          st.equal(index.ProvisionedThroughput.ReadCapacityUnits, 0, `Active GSI ${index.IndexName} ReadCapacityUnits`);
          st.equal(index.ProvisionedThroughput.WriteCapacityUnits, 0, `Active GSI ${index.IndexName} WriteCapacityUnits`);
        });

        // Test reverting back to PROVISIONED (this part had assertValidation in original)
        const updateToProvOpts = {
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
        };

        request(opts(updateToProvOpts), (errProv, resProv) => {
          st.error(errProv, 'UpdateTable to PROVISIONED should not error');
          if (!resProv) return st.end('No response from UpdateTable to PROVISIONED');
          st.equal(resProv.statusCode, 200, 'UpdateTable to PROVISIONED status code should be 200');

          const descProv = resProv.body.TableDescription;
          st.equal(descProv.TableStatus, 'UPDATING', 'TableStatus should be UPDATING after PROVISIONED request');
          st.equal(descProv.BillingModeSummary.BillingMode, 'PROVISIONED', 'BillingModeSummary should reflect PROVISIONED');
          st.ok(descProv.BillingModeSummary.LastUpdateToPayPerRequestDateTime >= (decreaseTimestamp - 5), 'PROVISIONED LastUpdateToPayPerRequestDateTime');
          // Check TableThroughputModeSummary if it exists
          if (descProv.TableThroughputModeSummary) {
            st.equal(descProv.TableThroughputModeSummary.TableThroughputMode, 'PROVISIONED', 'PROVISIONED TableThroughputMode');
            st.ok(descProv.TableThroughputModeSummary.LastUpdateToPayPerRequestDateTime >= (decreaseTimestamp - 5), 'PROVISIONED LastUpdateToPayPerRequestDateTime (Throughput)');
          }
          st.ok(descProv.ProvisionedThroughput.NumberOfDecreasesToday >= 0, 'PROVISIONED NumberOfDecreasesToday');
          st.equal(descProv.ProvisionedThroughput.ReadCapacityUnits, read, 'PROVISIONED ReadCapacityUnits');
          st.equal(descProv.ProvisionedThroughput.WriteCapacityUnits, write, 'PROVISIONED WriteCapacityUnits');

          descProv.GlobalSecondaryIndexes.forEach((index) => {
            st.equal(index.IndexStatus, 'UPDATING', `PROVISIONED GSI ${index.IndexName} status`);
            // LastDecreaseDateTime might be tricky here, may need adjustment
            st.ok(index.ProvisionedThroughput.LastDecreaseDateTime >= (decreaseTimestamp - 5), `PROVISIONED GSI ${index.IndexName} LastDecreaseDateTime`);
            st.ok(index.ProvisionedThroughput.NumberOfDecreasesToday > 0, `PROVISIONED GSI ${index.IndexName} NumberOfDecreasesToday`);
            st.equal(index.ProvisionedThroughput.ReadCapacityUnits, read, `PROVISIONED GSI ${index.IndexName} ReadCapacityUnits`);
            st.equal(index.ProvisionedThroughput.WriteCapacityUnits, write, `PROVISIONED GSI ${index.IndexName} WriteCapacityUnits`);
          });

          helpers.waitUntilActive(helpers.testRangeTable, (errWaitActiveProv, resWaitActiveProv) => {
            st.error(errWaitActiveProv, 'waitUntilActive (PROVISIONED) should succeed');
            if (!resWaitActiveProv) return st.end('No response from waitUntilActive (PROVISIONED)');

            const descActiveProv = resWaitActiveProv.body.Table;
            st.equal(descActiveProv.BillingModeSummary.BillingMode, 'PROVISIONED', 'Final Active BillingMode');
            st.ok(descActiveProv.BillingModeSummary.LastUpdateToPayPerRequestDateTime >= (decreaseTimestamp - 5), 'Final Active LastUpdateToPayPerRequestDateTime');
            if (descActiveProv.TableThroughputModeSummary) {
              st.equal(descActiveProv.TableThroughputModeSummary.TableThroughputMode, 'PROVISIONED', 'Final Active TableThroughputMode');
              st.ok(descActiveProv.TableThroughputModeSummary.LastUpdateToPayPerRequestDateTime >= (decreaseTimestamp - 5), 'Final Active LastUpdateToPayPerRequestDateTime (Throughput)');
            }
            st.ok(descActiveProv.ProvisionedThroughput.NumberOfDecreasesToday >= 0, 'Final Active NumberOfDecreasesToday');
            st.equal(descActiveProv.ProvisionedThroughput.ReadCapacityUnits, read, 'Final Active ReadCapacityUnits');
            st.equal(descActiveProv.ProvisionedThroughput.WriteCapacityUnits, write, 'Final Active WriteCapacityUnits');

            descActiveProv.GlobalSecondaryIndexes.forEach((index) => {
              st.ok(index.ProvisionedThroughput.LastDecreaseDateTime >= (decreaseTimestamp - 5), `Final Active GSI ${index.IndexName} LastDecreaseDateTime`);
              st.ok(index.ProvisionedThroughput.NumberOfDecreasesToday > 0, `Final Active GSI ${index.IndexName} NumberOfDecreasesToday`);
              st.equal(index.ProvisionedThroughput.ReadCapacityUnits, read, `Final Active GSI ${index.IndexName} ReadCapacityUnits`);
              st.equal(index.ProvisionedThroughput.WriteCapacityUnits, write, `Final Active GSI ${index.IndexName} WriteCapacityUnits`);
            });

            st.end(); // Final end of this long test
          });
        });
      });
    });
  });
  */

  t.end() // End updateTable - functionality tests
})
