# TODO: Migrate Dynalite Tests from Mocha to Tape

## Rules - never delete this

- Make sure to never change the signature of helpers without refactoring all the code that uses them. Use static analysis in that case. You mess up when faced with a lot of things to refactor. Let's NOT make this mistake again.

- When converting to tape test files individually, but when rewriting helpers ALWAYS run all the tests after to make sure we're cuasing regressions

- ALSO before checkin - run ALL tests to make sure we haven't caused regressions 

## TODO
