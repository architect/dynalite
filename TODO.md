# TODO: Migrate Dynalite Tests from Mocha to Tape

## Rules - never delete this

- Make sure to never change the signature of helpers without refactoring all the code that uses them. Use static analysis in that case. You mess up when faced with a lot of things to refactor. Let's NOT make this mistake again.

- When converting to tape - test files individually, but when rewriting helpers ALWAYS run all the tests after to make sure we're cuasing regressions

- ALSO before checkin - run ALL tape tests to make sure we haven't caused regressions 

## TODO

| File                                | LOC  | Status      | Notes                                     |
|-------------------------------------|-----:|-------------|-------------------------------------------|
| test-tape/mocha-source-split/bench.js | 46   | ✅ Converted | Kept skipped, uses helpers.batchBulkPut, helpers.request |
| test-tape/mocha-source-split/getItem.part1.js | 52   | ✅ Converted | Type checks using helpers.assertType |
| test-tape/mocha-source-split/describeTable.js | 56   | ✅ Converted | Type/validation checks using helpers |
| test-tape/mocha-source-split/batchGetItem.part1.js | 61   | ✅ Converted | Type checks using helpers.assertType |
| test-tape/mocha-source-split/batchWriteItem.part1.js | 62   | ✅ Converted |                                           |
| test-tape/mocha-source-split/describeTimeToLive.js | 71   | ✅ Converted |                                           |
| test-tape/mocha-source-split/deleteItem.part1.js | 77   | ✅ Converted |                                           |
| test-tape/mocha-source-split/putItem.part1.js | 79   | ✅ Converted |                                           |
| test-tape/mocha-source-split/untagResource.js | 87   | ✅ Converted | Fixed assertion discrepancy vs Mocha src. |
| test-tape/mocha-source-split/tagResource.js | 95   | ✅ Converted | Fixed assertion discrepancy vs Mocha src. |
| test-tape/mocha-source-split/updateItem.part1.js | 100  | ✅ Converted |                                           |
| test-tape/mocha-source-split/deleteTable.js | 106  | ✅ Converted |                                           |
| test-tape/mocha-source-split/scan.part1.js | 107  | ⬜ Not started |                                           |
| test-tape/mocha-source-split/updateTable.part1.js | 121  | ⬜ Not started |                                           |
| test-tape/mocha-source-split/listTagsOfResource.js | 125  | ⬜ Not started |                                           |
| test-tape/mocha-source-split/query.part1.js | 132  | ⬜ Not started |                                           |
| test-tape/mocha-source-split/createTable.part1.js | 166  | ⬜ Not started |                                           |
| test-tape/mocha-source-split/updateTable.part3.js | 195  | ⬜ Not started |                                           |
| test-tape/mocha-source-split/updateTable.part2.js | 214  | ⬜ Not started |                                           |
| test-tape/mocha-source-split/getItem.part3.js | 225  | ⬜ Not started |                                           |
| test-tape/mocha-source-split/batchWriteItem.part3.js | 238  | ⬜ Not started |                                           |
| test-tape/mocha-source-split/deleteItem.part3.js | 244  | ⬜ Not started |                                           |
| test-tape/mocha-source-split/listTables.js | 268  | ⬜ Not started |                                           |
| test-tape/mocha-source-split/createTable.part3.js | 322  | ⬜ Not started |                                           |
| test-tape/mocha-source-split/batchGetItem.part3.js | 343  | ⬜ Not started |                                           |
| test-tape/mocha-source-split/batchGetItem.part2.js | 352  | ⬜ Not started |                                           |
| test-tape/mocha-source-split/getItem.part2.js | 364  | ⬜ Not started |                                           |
| test-tape/mocha-source-split/batchWriteItem.part2.js | 370  | ⬜ Not started |                                           |
| test-tape/mocha-source-split/deleteItem.part2.js | 382  | ⬜ Not started |                                           |
| test-tape/mocha-source-split/connection.js | 387  | ⬜ Not started |                                           |
| test-tape/mocha-source-split/putItem.part2.js | 486  | ⬜ Not started |                                           |
| test-tape/mocha-source-split/updateItem.part3.js | 666  | ⬜ Not started |                                           |
| test-tape/mocha-source-split/updateItem.part2.js | 902  | ⬜ Not started |                                           |
| test-tape/mocha-source-split/putItem.part3.js | 980  | ⬜ Not started |                                           |
| test-tape/mocha-source-split/createTable.part2.js | 984  | ⬜ Not started |                                           |
| test-tape/mocha-source-split/scan.part2.js | 1068 | ⬜ Not started |                                           |
| test-tape/mocha-source-split/query.part3.js | 1485 | ⬜ Not started |                                           |
| test-tape/mocha-source-split/query.part2.js | 1780 | ⬜ Not started |                                           |
| test-tape/mocha-source-split/scan.part3.js | 2719 | ⬜ Not started |                                           |
| test-tape/mocha-source-split/helpers/ | N/A  | ⬜ Not started | Split helpers from original helpers.js |

