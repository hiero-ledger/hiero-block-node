# JS/TS TSS Verifier Example

This example is a Node-first spike for independently inspecting and partially verifying Hedera
block proof data produced under HIP-1056 and HIP-1200.

It does six things:

1. Loads real `.blk.zstd` (and legacy `.blk.gz`) fixtures.
2. Recomputes the HIP-1056 block root hash from `BlockUnparsed`.
3. Extracts and classifies the HIP-1200 `blockSignature` payload.
4. **Deserializes the 704-byte WRAPS proof** into its Nova IVC fields (ProofData) and validates
   the ledger ID against bootstrap data.
5. **Verifies Schnorr aggregate signatures** for genesis/pre-settled blocks using BabyJubjub
   curve arithmetic, Blake2s, and Poseidon over BN254.
6. Reports BLS12-381 point decode attempts for reference.

The parser follows a shallow design for block items: it scans `BlockUnparsed` and
`BlockItemUnparsed` at the protobuf wire level, preserves each item's original encoded bytes for
hashing, and only deeply decodes the small set of messages needed for block metadata, block proof,
and block-0 bootstrap extraction.

## Setup

Run this from `examples/js-tss-verifier/`:

```bash
npm install
```

The example depends on the combined proto bundle generated into
`protobuf-sources/block-node-protobuf/`. If that directory is missing, regenerate it from the
repo root:

```bash
cd protobuf-sources
./scripts/build-bn-proto.sh -t v0.72.0-rc.3 -v local -o "$PWD/block-node-protobuf" -i true -b "$PWD/src/main/proto"
```

## Usage

Run the default fixture pair (block 0 and block 1000):

```bash
npm run verify
```

Run a specific named fixture:

```bash
npm run verify:block0
npm run verify:block10
npm run verify:block1000
```

Run any fixture by path:

```bash
npm run verify -- path/to/some.blk.zstd
```

Emit JSON:

```bash
npm run verify -- --json
```

## What To Expect

- **Block 0**: Schnorr aggregate signature **VERIFIED** (2/2 signers). Exposes bootstrap data via
  `LedgerIdPublicationTransactionBody`. Uses genesis/Schnorr layout (2920 bytes).
- **Block 10**: Schnorr **VERIFIED** when run after block 0 (bootstrap context carried forward).
- **Block 1000**: WRAPS proof **deserialized successfully**. IVC state fields extracted, ledger ID
  **matches** bootstrap. This is a Nova IVC proof — full verification requires a Nova verifier.

## Current Observations

```
block-0:
  block root:     83871f1f...eac67a
  ledger ID:      60c64bef...c2521b
  proof layout:   genesis-schnorr, 2920 bytes
  Schnorr:        VERIFIED (2/2 signers)

block-1000:
  block root:     ed32913b...86e9fe
  proof layout:   wraps, 3432 bytes
  WRAPS deser:    SUCCESS (IVC step=2, z_0/z_i parsed, ledger ID match)
```

## Verification Coverage

| Verification step | Status |
|---|---|
| Block root recomputation (SHA-384 Merkle) | Working |
| Bootstrap extraction | Working |
| Schnorr aggregate signature (BabyJubjub + Blake2s + Poseidon) | Working |
| WRAPS proof deserialization (704-byte ProofData) | Working |
| WRAPS proof cryptographic verification (Nova IVC) | Not tractable in pure JS |
| hinTS aggregate signature (BLS12-381) | Not tractable — custom ArkWorks format |

## Architecture Notes

The 704-byte WRAPS proof is a `ProofData` struct (not a bare Groth16 proof):
- `i` (IVC step counter), `z_0`, `z_i` (IVC state vectors)
- Nova instance commitments (`U_i`, `u_i`)
- Groth16 decider proof + 2 KZG opening proofs + fold data

Full WRAPS verification requires a Nova IVC verifier, which does not exist in JavaScript.
The most credible path is a WASM build of the Rust verifier from `hedera-cryptography`.

## Oracle Context

The example is meant to be read alongside:

- `docs/design/tss-js-spike-findings.md`
- `docs/design/tss.md`
- `docs/design/tss-block-proof-verification.md`
- `block-node/verification/src/test/java/org/hiero/block/node/verification/session/impl/TssBlockProofVerificationTest.java`
