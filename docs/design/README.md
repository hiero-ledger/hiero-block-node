# Block Node Design Docs

> **Internal contributor documents.** These are engineering proposals, design explorations, and architectural decision records written for contributors to the Block Node codebase. They are not user-facing operational documentation.
>
> For operator and integrator documentation, see the [Block Node documentation](../block-node/README.md).

---

## How to write a design doc

- [Design doc template](design-doc-template.md) - Copy this when starting a new design doc
- [How to write a design doc](how-to-design-doc.md) - Guidance on structure and review process

## Design documents

|                                  Document                                   |                             Topic                              |
|-----------------------------------------------------------------------------|----------------------------------------------------------------|
| [Block Stream Forward Compatibility](block-stream-forward-compatibility.md) | Forward compatibility strategy for block stream format changes |
| [Block Verification](block-verification.md)                                 | Block verification design                                      |
| [TSS Block Proof Verification](tss-block-proof-verification.md)             | TSS-based block proof verification                             |
| [TSS Bootstrap Plugin](tss-bootstrap-plugin.md)                             | TSS bootstrap plugin design                                    |
| [Backfill Plugin](backfill-plugin.md)                                       | Historical block backfill plugin design                        |
| [Deployment with Selected Plugins](deployment-with-selected-plugins.md)     | Deploying with a custom plugin selection                       |
| [Change CLI Wrap Algorithm](change-cli-wrap-algorithm.md)                   | CLI wrap algorithm change proposal                             |
| [Communication Protocol](communication-protocol/README.md)                  | Block Node communication protocol                              |

### APIs

|                            Document                            |                  Topic                   |
|----------------------------------------------------------------|------------------------------------------|
| [API Throttling and Admission Control](apis/api-throttling.md) | Request throttling and admission control |
| [ServerStatus API](apis/server-status-api.md)                  | Server status API design                 |

### Architecture

|                                  Document                                  |                        Topic                         |
|----------------------------------------------------------------------------|------------------------------------------------------|
| [Logging Architecture](architecture/Logging-Architecture.md)               | Logging framework, level policy, and coding patterns |
| [Nano-Service Approach](architecture/Nano-Service-Approach.md)             | Nano-service architectural pattern                   |
| [Current Plugin Architecture](architecture/current-plugin-architecture.md) | Current plugin system design                         |
| [New Plugin Architecture](architecture/new-plugin-architecture.md)         | Proposed plugin architecture improvements            |

### Persistence

|                                Document                                |                Topic                 |
|------------------------------------------------------------------------|--------------------------------------|
| [Retention Policy](persistence/block-node-retention-policy.md)         | Block retention and eviction policy  |
| [Block Persistence](persistence/block-persistence.md)                  | Block storage and retrieval design   |
| [Cloud Storage Expanded Plugin](persistence/cloud-storage-expanded.md) | Expanded cloud storage plugin design |

### Proofs

|                       Document                        |           Topic            |
|-------------------------------------------------------|----------------------------|
| [Block Contents Proof](proofs/block-content-proof.md) | Block content proof design |

### Streaming

|                                    Document                                     |                  Topic                  |
|---------------------------------------------------------------------------------|-----------------------------------------|
| [Bi-directional Streaming](streaming/bidi-producer-consumers-streaming.md)      | Producer/consumer gRPC streaming design |
| [Closed-Range Historic Streaming](streaming/closed-range-historic-streaming.md) | Closed-range historic block streaming   |
| [Open-Range Historic Streaming](streaming/open-range-historic-streaming.md)     | Open-range historic block streaming     |

### WRB Streaming

|                                Document                                 |                 Topic                 |
|-------------------------------------------------------------------------|---------------------------------------|
| [Bootstrap RSA Roster Plugin](wrb-streaming/bootstrap-roster-plugin.md) | RSA roster bootstrap plugin design    |
| [Special-Purpose WRB Block Node](wrb-streaming/sp-wrb-bn-design.md)     | Special-purpose WRB Block Node design |

### Archived

|                     Document                      |              Topic              |
|---------------------------------------------------|---------------------------------|
| [Archived Cloud Plugin](Archived-Cloud-Plugin.md) | Cloud storage plugin (archived) |
