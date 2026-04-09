# JS/TS TSS Block Proof Verifier

A pure JavaScript/TypeScript implementation that independently verifies Hedera block proofs
(HIP-1056 / HIP-1200) using only `@noble/curves` — no native binaries or WASM required.

## What It Verifies

All three proof paths are fully implemented and working:

| Verification | Curve | Status |
|---|---|---|
| **Schnorr** aggregate signature (genesis/pre-settled blocks) | BabyJubjub over BN254 | Working |
| **WRAPS** Nova IVC proof (Groth16 + KZG) | BN254 | Working |
| **hinTS** BLS threshold signature (10 pairing checks) | BLS12-381 | Working |
| Block root recomputation (SHA-384 Merkle tree) | — | Working |
| Bootstrap extraction from block 0 | — | Working |

## Quick Start

```bash
cd tools/js-tss-verifier
npm install
```

Run the default verification (block 0 + block 467):

```bash
npm run verify
```

## Available Scripts

```bash
npm run verify           # Default: block 0 (Schnorr) + block 467 (WRAPS)
npm run verify:block0    # Genesis block only (Schnorr path)
npm run verify:block467  # Block 0 + 467 (Schnorr + WRAPS paths)
npm run verify:all       # All 7 fixtures (blocks 0-4, 466, 467)
```

Run any fixture by path:

```bash
npx tsx src/index.ts path/to/block.blk.gz
```

JSON output:

```bash
npx tsx src/index.ts --json
```

## Expected Output

### Block 0 (genesis-schnorr, 2920 bytes)

```
Schnorr verification: VERIFIED — Schnorr aggregate signature verified successfully (2/3 signers).
hinTS verification:   VERIFIED — 10/10 checks pass
```

### Block 467 (WRAPS, 3432 bytes)

```
WRAPS verification:   VERIFIED — Groth16 valid, KZG valid, ledger ID match (7/7 checks)
hinTS verification:   VERIFIED — 10/10 checks pass
```

## Test Fixtures

The verifier uses fixtures from:

```
block-node/app/src/testFixtures/resources/test-blocks/CN_0_73_TSS_WRAPS/
```

| File | Block | Proof Path | Proof Size |
|---|---|---|---|
| `0.blk.gz` | 0 | genesis-schnorr | 2920 bytes |
| `1.blk.gz` | 1 | genesis-schnorr | 2920 bytes |
| `2.blk.gz` | 2 | genesis-schnorr | 2920 bytes |
| `3.blk.gz` | 3 | genesis-schnorr | 2920 bytes |
| `4.blk.gz` | 4 | genesis-schnorr | 2920 bytes |
| `466.blk.gz` | 466 | wraps | 3432 bytes |
| `467.blk.gz` | 467 | wraps | 3432 bytes |

Block 0 contains the `LedgerIdPublicationTransactionBody` bootstrap transaction, which provides
the public keys and ledger ID needed to verify all subsequent blocks. When verifying non-genesis
blocks, always include block 0 first (or pass it before the target block).

## Architecture

### Shallow Parsing

The parser works at the protobuf wire level — it scans `BlockUnparsed` bytes directly, extracts
each `BlockItemUnparsed` without full deserialization, and preserves original encoded bytes for
hashing. Only control-plane messages (BlockHeader, BlockFooter, BlockProof, bootstrap transactions)
are deeply decoded.

### Proof Layout

The `blockSignature` field in the BlockProof contains:

| Segment | Size | Content |
|---|---|---|
| hinTS verification key | 1096 bytes | BLS12-381 points + polynomial commitments |
| hinTS signature | 1632 bytes | BLS aggregate signature + KZG proofs + bitmap |
| Suffix | 192 or 704 bytes | Schnorr signature (192) or WRAPS proof (704) |

- **2920-byte proofs** = genesis/Schnorr path (192-byte suffix)
- **3432-byte proofs** = settled/WRAPS path (704-byte suffix)

### Verification Details

**Schnorr** (genesis blocks): BabyJubjub curve, Blake2s challenge, Poseidon hash over BN254 for
public key aggregation.

**WRAPS** (settled blocks): The 704-byte suffix is a Nova IVC `ProofData` bundle containing a
Groth16 decider proof and 2 KZG opening proofs, all on BN254. Verification checks: ledger ID match,
hinTS VK hash match, iteration guard, commitment zero-check, Groth16 pairing, and both KZG pairings.

**hinTS** (all blocks): BLS12-381 threshold signature with 10 checks — BLS pairing, merged/parsum
KZG validity, B-SK identity, parsum accumulation/constraint, bitmap well-formedness/constraint, and
degree check. Uses a Fiat-Shamir challenge derived from the block root and proof data.

### Serialization

All cryptographic points use ArkWorks serialization conventions:
- BN254: little-endian, compressed, flag byte has bit7=positive, bit6=infinity
- BLS12-381: big-endian Fp, Fp2 components in (c1, c0) order relative to noble

## Dependencies

- `@noble/curves` — elliptic curve arithmetic (BN254, BLS12-381, BabyJubjub)
- `protobufjs` — protobuf decoding for control-plane messages
- `fzstd` — zstandard decompression (legacy `.blk.zstd` support)

All pure JavaScript, no native or WASM dependencies.

## Proto Setup

The example depends on the combined proto bundle in `protobuf-sources/block-node-protobuf/`.
If missing, regenerate from the repo root:

```bash
cd protobuf-sources
./scripts/build-bn-proto.sh -t v0.72.0-rc.3 -v local -o "$PWD/block-node-protobuf" -i true -b "$PWD/src/main/proto"
```
