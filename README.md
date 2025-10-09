# Hiero Block Node

[![Build Application](https://github.com/hiero-ledger/hiero-block-node/actions/workflows/build-application.yaml/badge.svg?branch=main)](https://github.com/hiero-ledger/hiero-block-node/actions/workflows/build-application.yaml)
[![E2E Test Suites](https://github.com/hiero-ledger/hiero-block-node/actions/workflows/e2e-tests.yaml/badge.svg?branch=main)](https://github.com/hiero-ledger/hiero-block-node/actions/workflows/e2e-tests.yaml)
[![codecov](https://codecov.io/github/hiero-ledger/hiero-block-node/graph/badge.svg?token=OF6T6E8V7U)](https://codecov.io/github/hiero-ledger/hiero-block-node)

[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/hiero-ledger/hiero-block-node/badge)](https://scorecard.dev/viewer/?uri=github.com/hiero-ledger/hiero-block-node)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/10697/badge)](https://bestpractices.coreinfrastructure.org/projects/10697)
[![Latest Version](https://img.shields.io/github/v/tag/hiero-ledger/hiero-block-node?sort=semver&label=version)](README.md)
[![Made With](https://img.shields.io/badge/made_with-java-blue)](https://github.com/hiero-ledger/hiero-block-node/)
[![Development Branch](https://img.shields.io/badge/docs-quickstart-green.svg)](docs/overview.md)
[![License](https://img.shields.io/badge/license-apache2-blue.svg)](LICENSE)

The Beta Hiero Block Node is the implementation of [HIP 1081](https://hips.hedera.com/hip/hip-1081) and serves as the
decentralized data lake for the Hiero ledgers block and state information.

The Block Node is responsible for consuming the block streams (defined in [HIP 1056](https://hips.hedera.com/hip/hip-1056)),
verifying each block’s integrity, storing the blockchain, distributing blocks to downstream clients and maintaining a
copy of consensus network state.

The Block Node will also expose additional targeted value adding APIs to the Hiero community such as Proofs.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Overview of child modules](#overview-of-child-modules)
3. [Getting Started](#getting-started)
4. [Contributing](#contributing)
5. [Code of Conduct](#code-of-conduct)
6. [License](#license)

## Prerequisites

- Java 21 (temurin recommended)
- Gradle (using the wrapper `./gradlew` is highly recommended)
- Docker (recommended for running the projects)
- IntelliJ IDEA (recommended for development)

## Overview of child modules

- [`block-node`](docs/block-node/README.md): implementation of the Block Node.
- [`simulator`](docs/simulator/README.md): A simulator for the Block Node, which can be used to test the Block Node in a local environment.
- `common`: Module responsible for holding common literals, utilities and types used by the other modules.
- `suites`: A set of e2e tests that can be used to verify the correctness of the Block Node.
- [tools](tools-and-tests/tools/README.md): A set of command line tools for working with Block Stream files.

## Getting Started

Refer to the [Hiero Block Node Documentation Overview](docs/overview.md) for more information about the project, design and guides.

## Contributing

Whether you’re fixing bugs, enhancing features, or improving documentation, your contributions are important — let’s build something great together!

Please read the governing [Hiero contributing guide](https://github.com/hiero-ledger/.github/blob/main/CONTRIBUTING.md) for the overall project.

Please read our [contributing guide](docs/contributing) to see how you can get involved.

## Code of Conduct

Hiero uses the Linux Foundation Decentralised Trust [Code of Conduct](https://www.lfdecentralizedtrust.org/code-of-conduct).

## License

[Apache License 2.0](LICENSE)
