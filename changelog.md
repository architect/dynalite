# Dynalite changelog

---

## [4.0.0] 2023-tbd

Hello! After a bit of a hiatus, the [Architect team](https://github.com/architect/dynalite/issues/166) is pleased bring the first new Dynalite release in over two years!

### Added

- Added `--host` / `options.host` option for specifying a particular host
- Added `--verbose|-v` / `options.verbose`, `--debug|-d` / `options.debug` logging modes, and some additional logging
- Added `-h` help alias for CLI
- Added Typescript Types via JSDOC comments

### Changed

- [Breaking change] Introduced minimum Node.js version of >= 16; fixes [#169](https://github.com/architect/dynalite/issues/169)
- Changed license from MIT to Apache 2.0; see [#166](https://github.com/architect/dynalite/issues/166)
- Updated dependencies (which themselves dropped support for older versions of Node.js)
- Updated tests
- Added Architect Code of Conduct, new CI flow, etc.

---

## [3.2.2 (and prior)]

This changelog was introduced as of 4.0. Big thanks to [@mhart](https://github.com/mhart) for creating and shepherding this project!

---

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
