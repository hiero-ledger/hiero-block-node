# Hiero Block Node

Implementation of the Hiero Block Node, which is responsible for consuming the block streams, maintaining state and exposing additional targeted value adding APIs to the Hiero community.

## Table of Contents

1. [Project Links](#project-links)
2. [Prerequisites](#prerequisites)
3. [Overview of child modules](#overview-of-child-modules)
4. [Getting Started](#getting-started)
5. [Contributing](#contributing)
6. [Code of Conduct](#code-of-conduct)
7. [License](#license)

## Project Links

[![Build Application](https://github.com/hashgraph/hedera-block-node/actions/workflows/build-application.yaml/badge.svg?branch=main)](https://github.com/hashgraph/hedera-block-node/actions/workflows/build-application.yaml)
[![E2E Test Suites](https://github.com/hashgraph/hedera-block-node/actions/workflows/e2e-tests.yaml/badge.svg?branch=main)](https://github.com/hashgraph/hedera-block-node/actions/workflows/e2e-tests.yaml)
[![codecov](https://codecov.io/github/hashgraph/hedera-block-node/graph/badge.svg?token=OF6T6E8V7U)](https://codecov.io/github/hashgraph/hedera-block-node)

[![Latest Version](https://img.shields.io/github/v/tag/hashgraph/hedera-block-node?sort=semver&label=version)](README.md)
[![Made With](https://img.shields.io/badge/made_with-java-blue)](https://github.com/hashgraph/hedera-block-node/)
[![Development Branch](https://img.shields.io/badge/docs-quickstart-green.svg)](docs/overview.md)
[![License](https://img.shields.io/badge/license-apache2-blue.svg)](LICENSE)

## Prerequisites

- Java 21 (temurin recommended)
- Gradle (using the wrapper `./gradlew` is highly recommended)
- Docker (recommended for running the projects)
- IntelliJ IDEA (recommended for development)

## Overview of child modules

- [`server`](./docs/server/README.md): implementation of the block node, which contains the main application and all the necessary code to run the block node.
- [`simulator`](./docs/simulator/README.md): A simulator for the block node, which can be used to test the block node in a local environment.
- `common`: Module responsible for holding common literals, utilities and types used by the other modules.
- `suites`: A set of e2e tests that can be used to verify the correctness of the block node.
- `tools`: A set of command line tools for working with block stream files.

## Getting Started

Refer to the [Hiero Block Node Documentation Overview](docs/overview.md) for more information about the project, design and guides.

## Contributing

Whether you’re fixing bugs, enhancing features, or improving documentation, your contributions are important — let’s build something great together!

Please read the governing [Hiero contributing guide](https://github.com/hiero-ledger/.github/blob/main/CONTRIBUTING.md) for the overall project.

Please read our [contributing guide](./docs/contributing.md) to see how you can get involved.

## Code of Conduct

Hiero uses the Linux Foundation Decentralised Trust [Code of Conduct](https://www.lfdecentralizedtrust.org/code-of-conduct).

## License

[Apache License 2.0](LICENSE)
