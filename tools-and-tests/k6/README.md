# K6 Performance Tests
This directory contains performance tests for the application using [k6](https://k6.io/), a modern load testing tool.

## Prerequisites
- Ensure you have [k6](https://grafana.com/docs/k6/latest/set-up/install-k6/) installed on your machine.
- Make sure the application you want to test is running and accessible.

## Running the Tests
1. Navigate to the desired directory `tools-and-tests/k6/<testTypeDir>`:
   ```bash
   cd tools-and-tests/k6/<testTypeDir>
   ```
2. Run the desired k6 test script using the following command:
   ```bash
   k6 run <script-name>.js
   ```
   Replace `<script-name>.js` with the k6 test script you want to execute.

## Test Types

The k6 setup will be used to run different https://grafana.com/docs/k6/latest/testing-guides/test-types/ located in subdirectories.
Each subdirectory contains its own k6 test scripts and configuration files.

### Smoke Tests

The `./tools-and-tests/k6/smoke` directory contains basic smoke test scripts to verify the application's core functionality.
No additional load is applied during these tests but a test runner can configure virtual users if needed.

The BN protobuf files are required for these tests and should be downloaded from the desired [BN Release](https://github.com/hiero-ledger/hiero-block-node/releases)
and placed in the `./smoke` directory before running the tests e.g. `tools-and-tests/k6/smoke/block-node-protobuf-0.23.1`.
The `./tools-and-tests/k6/smoke/data.json` file `configs.protobufPath` should be updated to reflect the paths to these protobuf files.

```bash
cd tools-and-tests/k6/smoke
k6 run bn-query-validation.js
```
