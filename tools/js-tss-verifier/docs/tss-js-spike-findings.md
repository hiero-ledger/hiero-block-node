# JS/TS TSS Verification Spike: Findings

## Goal

Determine whether TSS block proofs from Hiero block fixtures can be verified end-to-end in
JavaScript/TypeScript using off-the-shelf libraries (`@noble/curves` for BLS12-381, `snarkjs` for
Groth16/BN254), and get a realistic estimate of the effort involved.

## What Was Built

A working Node.js/TypeScript spike (`examples/js-tss-verifier`) that:

1. Loads and decompresses real block fixtures (`.gz` and `.zstd`).
2. Shallow-parses `BlockUnparsed` / `BlockItemUnparsed` at the protobuf wire level, preserving
   original encoded bytes per item for correct Merkle hashing.
3. Recomputes the HIP-1056 block root hash (five subtrees + virtual tree assembly + timestamp leaf).
4. Extracts `LedgerIdPublicationTransactionBody` bootstrap data from block 0.
5. Classifies the packed `blockSignature` payload into one of the documented proof layouts.
6. Attempts BLS12-381 point decoding against each proof slice using `@noble/curves`.
7. Assesses whether the WRAPS suffix matches a standard Groth16 proof shape for `snarkjs`.

**Line count:** ~750 lines of TypeScript across 8 source files. This covers the full deterministic
pipeline (parsing, hashing, bootstrap extraction, classification, compatibility checks, and
reporting) but not cryptographic verification itself, which remains blocked (see below).

## Fixtures Tested

|         File          | Block |      Layout       |  Suffix   |              Bootstrap              |
|-----------------------|-------|-------------------|-----------|-------------------------------------|
| `block-0.blk.zstd`    | 0     | `genesis-schnorr` | 192 bytes | present (ledger ID + 2 nodes)       |
| `block-10.blk.zstd`   | 10    | `genesis-schnorr` | 192 bytes | not present                         |
| `block-1000.blk.zstd` | 1000  | `wraps`           | 704 bytes | present (carried over from block 0) |

The earlier fixtures (`0.blk.gz`, `50.blk.gz`, `1319.blk.gz`) all decode to the 2920-byte
genesis/Schnorr layout. Block 1000 (`block-1000.blk.zstd`) is the first confirmed WRAPS sample.

## Confirmed Implementation Details (from Rohit)

- **hinTS on BLS12-381, WRAPS as Groth16 on BN254, threshold 1/2** — confirmed as current implementation, not just design intent.
- **ArkWorks → snarkjs compatibility** — the WRAPS proof is produced by ArkWorks libraries and treated as an opaque byte array everywhere else in the stack. Whether snarkjs can consume it is not yet known; a compatibility test needs to be written.
- **Test vectors** exist in the `hedera-cryptography` repo (WRAPS and hinTS test cases). Rohit also pointed to the authoritative source for `TSS.verifyTSS(...)` behavior.

## Observed Proof Byte Layout

Confirmed by Rohit against the Java constants in `BlockVerificationUtils`:

```
[0..1096)    hints_verification_key  — hinTS VK (opaque, not a standard G1/G2 point)
[1096..2728) hints_signature         — hinTS signature (1632 bytes)
[2728..)     suffix — one of:
               192 bytes: aggregate_schnorr_signature (genesis path, total = 2920)
               704 bytes: wraps_proof, compressed Groth16 on BN254 (settled path, total = 3432)
```

Java constants (from `BlockVerificationUtils`):

```java
HINTS_VERIFICATION_KEY_LENGTH  = 1096
HINTS_SIGNATURE_LENGTH         = 1632
COMPRESSED_WRAPS_PROOF_LENGTH  = 704
AGGREGATE_SCHNORR_SIGNATURE_LENGTH = 192
```

## Key Findings

### What works today in JS/TS

- Block root hash recomputation (HIP-1056 Merkle tree): fully working, output matches fixture expectations.
- Bootstrap data extraction from `LedgerIdPublicationTransactionBody`: working for block 0.
- Proof layout classification by byte length: working for all tested fixtures.
- Bootstrap context carry-over across a multi-block run: working.

### What does not work with off-the-shelf libraries

**`@noble/curves` (BLS12-381):**

- The VK (1096 bytes) does not divide into standard compressed G1 (48 bytes) or G2 (96 bytes) chunks.
- The BLS sig (1632 bytes) does not divide into standard G1 or G2 chunks.
- Neither the VK nor the BLS sig decodes as a whole standard BLS12-381 point.
- Conclusion: the packed format is not standard noble input. A custom deserializer mapping the
  Hedera format to canonical curve points is required before `@noble/curves` can be invoked.

**`snarkjs` (Groth16/BN254):**

- The WRAPS suffix (704 bytes) does not match any standard compressed or uncompressed Groth16 proof
  shape (128, 192, 256, or 384 bytes).
- Without the canonical proof, verification key, and ordered public inputs in ArkWorks/snarkjs
  format, a trustworthy `snarkjs` check cannot be attempted.

## Tractability Conclusion

> **Direct verification with off-the-shelf `@noble/curves` or `snarkjs` is not tractable as-is.**

The blocker is byte format, not JS library capability. The libraries are sufficient in principle,
but the packed `blockSignature` blob requires a custom deserializer before either library can be
invoked. The effort to write that deserializer cannot be estimated until the ArkWorks-to-snarkjs
field mapping is known.

The deterministic part of the verifier (parsing, hashing, bootstrap) is fully tractable in JS/TS
and is already working. That portion aligns with Phase 1 of the proposed library design in
`docs/design/tss.md`.

## Open Blockers

1. **ArkWorks → snarkjs format compatibility**: The WRAPS proof is an ArkWorks-produced opaque
   byte array. Whether snarkjs can deserialize it is unproven — a compatibility test needs to be
   written. Until this is resolved, we cannot attempt a `snarkjs` Groth16 verification call even
   with a valid fixture.
2. **WRAPS public inputs**: The ordered public inputs required for Groth16 verification are not
   exposed in the block fixture. These are needed for any `snarkjs` call regardless of format
   compatibility.
3. **hinTS VK / hinTS sig deserialization**: The 1096-byte and 1632-byte fields do not decode as
   standard BLS12-381 points with `@noble/curves`. The internal ArkWorks serialization format
   for these objects needs to be documented or a deserializer provided before noble-based
   hinTS verification is possible.

## Resolved Questions

- **Implementation**: hinTS on BLS12-381, WRAPS as Groth16 on BN254, threshold 1/2 — confirmed.
- **Byte layout**: Confirmed against Java constants in `BlockVerificationUtils` (see above).
- **WRAPS test vectors**: Exist in `hedera-cryptography` repo; Rohit pointed to them.
- **`TSS.verifyTSS(...)` reference**: Rohit identified the authoritative source.

## Reference Observations (block-0 and block-1000)

```
block-0:
  block root:  83871f1fbc0bcbdaa6c5f08b29fb6520aa692e02bfe2cc36e6d1c876559baacb95ce51f1fb1b4a542ed6476149eac67a
  ledger ID:   60c64bef22e069e5ba0043363059c7dfdad92e19592431230597ef3dfdc2521b
  proof total: 2920 bytes (genesis-schnorr)

block-1000:
  block root:  ed32913bfc0362bbbdd39b61b1959daf032cc48eafce163b83e2e647e545b11d7785cd7df2432d72b194ea0cb586e9fe
  proof total: 3432 bytes (wraps)
```

These values serve as the JS-side conformance baseline. The Java oracle test
(`TssBlockProofVerificationTest`) continues to pass on the same fixtures.

## Related Documents

- `docs/design/tss.md` — proposed JS/TS library architecture and phased plan
- `docs/design/tss-block-proof-verification.md` — existing block proof verification design
- `examples/js-tss-verifier/` — spike source code
