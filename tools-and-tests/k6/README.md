# K6 Performance Tests

This directory contains performance tests for the application using [k6](https://k6.io/), a modern load testing tool.

## Prerequisites

- Ensure you have [k6](https://grafana.com/docs/k6/latest/set-up/install-k6/) installed on your machine.
- Make sure have solo installed on your machine.

## Setup

The tests utilize protobuf files specific to the Block Node (BN) application for validation purposes and config settings
located in the `./tools-and-tests/k6/data.json` file.

The BN protobuf files should be downloaded from the desired [BN Release](https://github.com/hiero-ledger/hiero-block-node/releases) and placed in the `./k6` directory before
running the tests e.g. `tools-and-tests/k6/proto`.

The `data.json` file should be updated to reflect the paths to these
protobuf files.
- `configs.blockNodeUrl`: URL of the Block Node instance to be tested.
- `configs.protobufPath`: Path to the directory containing the Block Node protobuf files.

- Example:

  ```json
  {
    "configs": [{
        "blockNodeUrl": "localhost:40840",
        "protobufPath": "./../k6-proto",
        ...
    }]
  }
  ```

## Test Types

The k6 setup will be used to run different [test types](https://grafana.com/docs/k6/latest/testing-guides/test-types/) located in subdirectories.
Each subdirectory contains its own k6 test scripts and configuration files.

### Average Load Tests

The `./tools-and-tests/k6/average-load` directory contains test scripts designed to simulate average load conditions on
the application.

### Smoke Tests

The `./tools-and-tests/k6/smoke` directory contains basic smoke test scripts to verify the application's core functionality.
No additional load is applied during these tests but a test runner can configure virtual users if needed.

The `data.json` file may be updated to change the following settings
protobuf files.
- `configs.smokeTestConfigs.numOfBlocksToStream`: Number of blocks to stream as a subscriber during the smoke test.

- Example:

  ```json
  {
    "configs": [{
        ...,
        "smokeTestConfigs": {
            "numOfBlocksToStream": 10
        }
    }]
  }
  ```

## Running the Tests

1. From the project root directory run: `./gradlew runK6Tests`

This gradle task calls `run-k6-tests.sh` script which runs all the tests. This script then performs the following steps:
1. `task up` from solo-e2e-test which uses the default config and starts a 1 CN, 1 BN, 1MN environment for testing
2. runs the tests
3. `task down` from solo-e2e-test which tears down the solo environment
