# tss-bootstrap

Loads Threshold Signature Scheme (TSS) parameters at Block Node startup and injects them into `BlockNodeContext` so the verification pipeline can validate block proofs from the TSS era (HAPI ≥ 0.72.0).

> **Status:** Implementation is a skeleton. The class structure and plugin registration are in place; the TSS data loading logic contains TODO placeholders and is not yet functional. Do not use in production until the TODOs are resolved.

---

## Key Files

|           File            |                                                                                                                                 Purpose                                                                                                                                 |
|---------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `TssBootstrapPlugin.java` | Plugin entry point. Implements `BlockNodePlugin`. Intended to read TSS parameter files (containing the ledger ID and threshold key material) from disk and make them available via `BlockNodeContext` so `VerificationServicePlugin` can verify block proof signatures. |

---

## Notable Logic

### Relationship to `VerificationServicePlugin`

`VerificationServicePlugin` currently reads TSS parameters lazily on first sight of block 0 via `initializeTssParameters()` and persists them to `verificationConfig.tssParametersFilePath()`. The `tss-bootstrap` plugin is intended to provide an **explicit startup hook** for this data, decoupling the parameter loading lifecycle from the verification event stream. Once implemented, `tss-bootstrap` replaces the lazy-init path in `VerificationServicePlugin`.

### Ledger ID and key material

TSS verification relies on the `LedgerIdPublicationTransactionBody` which carries the network's ledger ID and threshold public key parameters. These values are consensus-derived and must not be hand-configured. The bootstrap plugin is expected to derive them from the genesis block or from the persisted TSS parameters file written by `VerificationServicePlugin`.
