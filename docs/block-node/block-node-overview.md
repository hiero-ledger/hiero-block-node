# Block Node Overview

Block Nodes represent a new class of nodes in the Hedera network designed to increase decentralization and network data distributions. They enable operators to provide to assume responsibility for long-term block and state storage while supporting the security and performance characteristics of a Hiero network. This overview document provides operators with the essential concepts needed to understand Block Node roles and responsibilities before diving into deployment.

## What is a Block Node?

A Block Node is a special kind of server that keeps a complete, trustworthy copy of what is happening on the Hedera network. It receives a stream of already-agreed blocks from Consensus Nodes, checks that each block is valid, and then stores those blocks and the current network state so they are easy to query later.

Instead of pushing this data into centralized cloud storage, Block Nodes act as a decentralized data layer for the network. They stream blocks to Mirror Nodes and other Block Nodes, answer questions from apps and services about past blocks or current state, and provide cryptographic proofs so users can independently verify that the data is correct.

## How Block Nodes differ from other nodes

|           **Aspect**            |                                        **Consensus Node**                                         |                                  **Block Node**                                   |                             **Mirror Node**                             |
|---------------------------------|---------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------|-------------------------------------------------------------------------|
| Primary role                    | Reach consensus and update canonical state.                                                       | Ingest, verify, store, and serve blocks, sidecars, and state.                     | Provide value‑added access to historical data and analytics.            |
| Produces blocks?                | Yes – produces finalized block streams per **([HIP-1056](https://hips.hedera.com/hip/hip-1056))** | No – consumes and verifies blocks from Consensus Nodes or upstream Block Nodes.   | No – typically consumes data from Block Nodes or record streams.        |
| Maintains full consensus state? | Yes – authoritative state, optimized for consensus.                                               | Yes – replicated state plus saved states and snapshots for queries and reconnect. | Optional/derived – maintains state as needed for queries and analytics. |
| Data APIs                       | gRPC for transactions/queries; limited history.                                                   | Rich gRPC/REST APIs for streaming, random access, state, and proofs.              | Public REST and custom APIs for queries and observability.              |
| Who runs it?                    | Governing Council and approved operators.                                                         | Tier 1: Council / trusted; Tier 2: permissionless community and infra providers.  | Permissionless operators, service providers, and app teams.             |

## Key Terms

Before diving deeper, familiarize yourself with these core concepts:

- **Block Stream ([HIP-1056](https://hips.hedera.com/hip/hip-1056))** - A continuous, ordered feed of finalized blocks produced by Consensus Nodes. Each block contains transactions, state changes, event records, and cryptographic signatures. Block streams are delivered via gRPC in Protocol Buffer format at a rate of approximately [X blocks per second / Y seconds per block].
- **Sidecar Files** - Supplementary data files associated with each block that contain additional information such as transaction records, smart contract bytecode, state proofs, and other metadata that don't fit in the core block structure. Sidecars enable efficient verification and querying without bloating block size.
- **State Snapshot** - A point-in-time capture of the complete network state (accounts, balances, smart contract storage, etc.) at a specific block height. State snapshots enable fast synchronization and recovery without replaying every transaction from genesis.
- **Aggregated Signatures** - Cryptographic signatures from multiple Consensus Nodes combined into a single compact signature that proves a block was finalized by network consensus. Defined in [HIP-1200](https://hips.hedera.com/hip/hip-1200).
- **Reconnect Services** - APIs and data streams that help Consensus Nodes catch up to the current network state after downtime or network partitions by providing recent blocks and state snapshots.
- **Saved States** - Periodic checkpoints of network state stored to disk that allow Block Nodes to restart quickly without reprocessing all historical blocks.
- Tier 1 Block Node – A Block Node that receives block streams directly from Consensus Nodes and typically provides reconnect/state snapshot services back to the network.
- Tier 2+ Block Node – A Block Node that receives block streams from one or more upstream Block Nodes (Tier 1 or Tier 2) and is permissionless to operate.

## Role in the Hiero Network

Block Nodes act as the trusted historians and data providers for the Hedera network. They receive block streams from Consensus Nodes, verify that each block and its data are correct, and store these blocks along with the current state of the network. By doing so, Block Nodes replace old methods of storing network data in centralized cloud buckets and instead make this data reliably available in a decentralized way.

Block Nodes distribute blocks to downstream clients—including Mirror Nodes, other Block Nodes, and applications—so anyone can access real-time or historical data. They also generate cryptographic proofs for transactions and state, making it possible for users and applications to independently verify the accuracy of the blockchain’s history without relying on a single provider.

Finally, Block Nodes help the network scale and run smoothly. When a Consensus Node needs to catch up with its peers or recover from downtime, it can rely on Block Nodes to provide the latest data and state snapshots—relieving Consensus Nodes from heavy data storage duties and improving overall efficiency.

## Core Block Node Services

Block Nodes provide several core services that turn block streams into reliable, consumable data for the network:

- Block stream ingestion, verification, and distribution.
- Cryptographic proofs for transactions and network state.
- Durable storage of blocks, and consensus state.
- Real-time and historical data streaming to downstream clients.
- Random-access retrieval of blocks and state at specific block heights.
- State snapshots and reconnect services to help Consensus Nodes catch up or recover.

## Example Use Cases

- Running an API server for wallets, explorers, or dApps to access verified blockchain data.
- Operating analytics or aggregation pipelines using live and historical block streams.
- Providing specialized compliance, archival, or network recovery support for Hedera services.

## Block Node Types

|    **Type**     |                         **Description**                          |        **Typical Operators**        |                              **Key Focus**                              |
|-----------------|------------------------------------------------------------------|-------------------------------------|-------------------------------------------------------------------------|
| Tier 1          | Receive streams directly from Consensus Nodes; high reliability. | Governing Council, trusted entities | Verification, reconnect, state snapshots .                              |
| Tier 2          | Receive from Tier 1/Tier 2; permissionless.                      | Community, enterprises              | Streaming, proofs, geographic redundancy.                               |
| Full-History    | Retain genesis-to-present history in hot/online storage.         | Any                                 | Random-access queries, full historical streams.                         |
| Partial-History | Store only recent blocks (e.g., 30-90 days).                     | Cost-optimized                      | Recent-data streaming, light storage footprint.                         |
| Archive         | Move verified blocks/state to cold or long-term storage.         | Compliance / archival providers     | Disaster recovery, regulatory retention. |

## High-Level Architecture

Block Nodes follow a modular design, receiving **block streams**, verifying integrity using **aggregated signatures** and cryptographic mechanisms defined in related HIPs ([HIP-1200](https://hips.hedera.com/hip/hip-1200) and [HIP-1056](https://hips.hedera.com/hip/hip-1056)), and persisting blocks and state to local disk and remote archives. They may provide four common functions:

- **Block stream ingestion and verification** - Receives block streams from Consensus Nodes and verifies their integrity using aggregated signatures and Merkle proofs.
- **State maintenance and snapshot generation** - Applies block state changes to maintain an up-to-date view of network state and periodically produces verifiable state snapshots for reconnect and recovery flows.
- **Durable storage** - Persists blocks, and saved states to local disk or to remote archival storage for long-term, tamper-evident history.
- **Data services** - Exposes gRPC/REST APIs providing real-time block streaming, random-access block retrieval, state queries at specific block heights, and cryptographic proofs to Mirror Nodes, other Block Nodes, and applications.

Block Nodes fan out block streams to Mirror Nodes and Tier 2+ Block Nodes while also serving reconnect and state snapshot services back to Consensus Nodes, creating a scalable data availability layer between consensus and downstream consumers.

![block-node-network-architecture](./../assets/block-node-network-architecture.svg)

### How Data Flows Through the Network

The diagram above illustrates the complete data flow:

1. **Consensus Nodes produce blocks** - Users submit transactions via gRPC to Consensus Nodes, which reach consensus through hashgraph and produce finalized blocks.
2. **Block Nodes receive and verify** - Tier 1 Block Nodes receive block streams directly from Consensus Nodes, verify block integrity using aggregated signatures (as defined in [HIP-1200](https://hips.hedera.com/hip/hip-1200)), and store verified blocks and state to local disk and remote archives.
3. **Block Nodes distribute downstream** - Verified block streams fan out to:
   - **Mirror Nodes** - for public REST APIs and explorer services
   - **Tier 2+ Block Nodes** - for geographic redundancy and permissionless participation
   - **Applications** - via gRPC/REST APIs for custom integrations
4. **Block Nodes support Consensus Node recovery** - When a Consensus Node experiences downtime or falls behind, it requests reconnect data (recent blocks and state snapshots) from Block Nodes to quickly resynchronize without burdening other Consensus Nodes.

## Key Responsibilities

Block Node operators are responsible for:

- Keeping their Block Node online, monitored, and synchronized with the latest network state.
- Managing storage and retention for blocks, sidecars, and state snapshots.
- Securing access to APIs and infrastructure, including authentication, authorization, and network boundaries.
- Applying software upgrades in line with Hedera/Hiero releases and block node compatibility guidance.
- Choosing and configuring which services to expose (streaming, random access, proofs, reconnect/state snapshots) for their consumers.

## Benefits for Operators

- Replaces legacy record stream polling with efficient streaming
- Lower costs via aggregated signatures and no S3 operations
- Enhanced Mirror Node integration with random-access and proofs
- Plugin extensibility without forking

## Choosing Your Block Node Configuration

Use this decision guide to determine which Block Node configuration suits your needs:

### Tier 1 vs Tier 2

**Choose Tier 1 if:**

- You are a Hedera Governing Council member or trusted network partner.
- You have authorization to peer directly with Consensus Nodes.
- You can commit to high-availability SLAs (99.9%+ uptime).
- You want to provide reconnect services to Consensus Nodes.

**Choose Tier 2 if:**

- You are a community operator, enterprise, or infrastructure provider.
- You want to participate without special permissions (permissionless).
- You can receive block streams from existing Tier 1 or Tier 2 Block Nodes.
- Your focus is on streaming verified data to applications or Mirror Nodes.

**Note:** Tier 2 nodes are truly permissionless—anyone can deploy one without approval or registration. You simply configure your node to connect to one or more existing Block Nodes (Tier 1 or Tier 2) for upstream block streams.

## Getting Started

***To start running a Block Node, read:***

- **Solo Weaver Single Node Kubernetes Deployment Guide** – step‑by‑step instructions for deploying a single Block Node instance with the Solo Weaver on a Kubernetes, including environment preparation, deployment, and basic verification.
- **Single Node Kubernetes Deployment** – instructions for deploying the Block Node Server Helm chart in a single‑node Kubernetes environment, suitable for production setups on bare metal or cloud VMs.
