# K6 Performance Tests

This directory contains performance tests for the application using [k6](https://k6.io/), a modern load testing tool.

## Prerequisites

- Ensure you have [k6](https://grafana.com/docs/k6/latest/set-up/install-k6/) installed on your machine.
- Ensure the [solo-e2e-test prerequisites](../scripts/solo-e2e-test/README.md#prerequisites) are met
  (the test runner uses `task up`/`task down` to spin up and tear down the test environment).

## Setup

The tests utilize protobuf files specific to the Block Node (BN) application for validation purposes and config settings
located in the `./tools-and-tests/k6/data.json` file.

The BN protobuf files are generated automatically by the `runK6Tests` Gradle task and extracted to
`tools-and-tests/k6/k6-proto/`. No manual download is needed when using `./gradlew runK6Tests`.

The `data.json` file should be updated to reflect the paths to these
protobuf files.
- `configs.blockNodeUrl`: URL of the Block Node instance to be tested.
- `configs.protobufPath`: Path to the directory containing the Block Node protobuf files.

- Example:

  ```json
  {
    "configs": [{
        "blockNodeUrl": "localhost:40840",
        "protobufPath": "../../k6-proto",
        ...
    }]
  }
  ```

## Test Types

The k6 setup will be used to run different [test types](https://grafana.com/docs/k6/latest/testing-guides/test-types/) located in subdirectories.
Each subdirectory contains its own k6 test scripts and configuration files.

### Average Load Tests

The `./tools-and-tests/k6/src/average-load` directory contains test scripts designed to simulate average load conditions on
the application.

### Smoke Tests

The `./tools-and-tests/k6/src/smoke` directory contains basic smoke test scripts to verify the application's core functionality.
No additional load is applied during these tests but a test runner can configure virtual users if needed.

The `data.json` file may be updated to change the following settings:
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
1. `task up` from solo-e2e-test which uses the default config and starts a 1 CN (Consensus Node), 1 BN (Block Node), 1 MN (Mirror Node) environment for testing
2. runs the tests
3. `task down` from solo-e2e-test which tears down the solo environment
