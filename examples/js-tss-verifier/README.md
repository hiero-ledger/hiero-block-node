# JS/TS TSS Verifier Example

This example is a Node-first spike for independently inspecting Hedera block proof data produced
under HIP-1056 and HIP-1200.

It does four things:

1. Loads real `.blk.zstd` (and legacy `.blk.gz`) fixtures.
2. Recomputes the HIP-1056 block root hash from `BlockUnparsed`.
3. Extracts and classifies the HIP-1200 `blockSignature` payload.
4. Attempts JS-side tractability checks with `@noble/curves/bls12-381`.

The parser follows a shallow design for block items: it scans `BlockUnparsed` and
`BlockItemUnparsed` at the protobuf wire level, preserves each item's original encoded bytes for
hashing, and only deeply decodes the small set of messages needed for block metadata, block proof,
and block-0 bootstrap extraction.

It does not claim full pure-JS TSS verification today. See
`docs/design/tss-js-spike-findings.md` for the current tractability findings.

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

- **Block 0** exposes bootstrap data via `LedgerIdPublicationTransactionBody` and uses the
  genesis/Schnorr layout: `vk (1096) || blsSig (1632) || aggregate_schnorr_sig (192)` = 2920 bytes.
- **Block 10** is a regular Schnorr-path block with no bootstrap transaction.
- **Block 1000** is the first confirmed WRAPS sample:
  `vk (1096) || blsSig (1632) || wraps_proof (704)` = 3432 bytes.
- The noble attempt is intentionally conservative. It reports whether the extracted slices look
  like standard BLS12-381 points; it does not fabricate a passing result when the byte format is
  not canonical.
- Direct verification with `@noble/curves` or `snarkjs` is currently blocked on byte format
  mapping. See `docs/design/tss-js-spike-findings.md` for detail.

## Current Observations

```
block-0:
  block root:  83871f1fbc0bcbdaa6c5f08b29fb6520aa692e02bfe2cc36e6d1c876559baacb95ce51f1fb1b4a542ed6476149eac67a
  ledger ID:   60c64bef22e069e5ba0043363059c7dfdad92e19592431230597ef3dfdc2521b
  proof layout: genesis-schnorr, 2920 bytes

block-1000:
  block root:  ed32913bfc0362bbbdd39b61b1959daf032cc48eafce163b83e2e647e545b11d7785cd7df2432d72b194ea0cb586e9fe
  proof layout: wraps, 3432 bytes
```

## Oracle Context

The example is meant to be read alongside:

- `docs/design/tss-js-spike-findings.md`
- `docs/design/tss.md`
- `docs/design/tss-block-proof-verification.md`
- `block-node/verification/src/test/java/org/hiero/block/node/verification/session/impl/TssBlockProofVerificationTest.java`
