# Product Overview

Dynalite is a fast, in-memory implementation of Amazon DynamoDB built on LevelDB. It provides a local DynamoDB-compatible server for development and testing purposes.

## Key Features
- Full DynamoDB API compatibility (matches live instances closely)
- Fast in-memory or persistent storage via LevelDB
- Supports both CLI and programmatic usage
- SSL support with self-signed certificates
- Configurable table state transition timings
- Comprehensive validation matching AWS DynamoDB

## Use Cases
- Local development and testing
- Fast startup alternative to DynamoDB Local (no JVM overhead)
- CI/CD pipelines requiring DynamoDB functionality
- Offline development environments

## Target Compatibility
- Matches AWS DynamoDB behavior including limits and error messages
- Tested against live DynamoDB instances across regions
- Supports DynamoDB API versions: DynamoDB_20111205, DynamoDB_20120810