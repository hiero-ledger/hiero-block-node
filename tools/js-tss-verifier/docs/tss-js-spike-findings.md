# JS/TS TSS Verification Spike: Findings

## Goal

Determine whether TSS block proofs from Hiero block fixtures can be verified end-to-end in
JavaScript/TypeScript using off-the-shelf libraries (`@noble/curves`, `@noble/hashes`), and get a
realistic estimate of the effort involved.

## What Was Built

A working Node.js/TypeScript spike (`tools/js-tss-verifier`) that:

1. Loads and decompresses real block fixtures (`.gz` and `.zstd`).
2. Shallow-parses `BlockUnparsed` / `BlockItemUnparsed` at the protobuf wire level, preserving
   original encoded bytes per item for correct Merkle hashing.
3. Recomputes the HIP-1056 block root hash (five subtrees + virtual tree assembly + timestamp leaf).
4. Extracts `LedgerIdPublicationTransactionBody` bootstrap data from block 0.
5. Classifies the packed `blockSignature` payload into one of the documented proof layouts.
6. **Verifies Schnorr aggregate signatures** for genesis/pre-settled blocks using BabyJubjub
   (ark_ed_on_bn254) curve arithmetic, Blake2s hashing, and Poseidon over BN254 Fr.
7. **Deserializes the 704-byte WRAPS proof** into its full Nova IVC ProofData structure and
   validates the genesis ledger ID against bootstrap data.
8. Attempts BLS12-381 point decoding against each proof slice using `@noble/curves`.

**Line count:** ~1100 lines of TypeScript across 10 source files.

## Fixtures Tested

| File | Block | Layout | Schnorr | WRAPS Deser | Bootstrap |
|---|---|---|---|---|---|
| `block-0.blk.zstd` | 0 | `genesis-schnorr` | VERIFIED (2/2) | N/A | present |
| `block-10.blk.zstd` | 10 | `genesis-schnorr` | VERIFIED (2/2) | N/A | carried |
| `block-1000.blk.zstd` | 1000 | `wraps` | N/A | SUCCESS (ledger ID MATCH) | present |

## Observed Proof Byte Layout

Confirmed against Java constants (`TSS.java` in `hedera-cryptography`) and the Rust source:

```
[0..1096)    hints_verification_key  — hinTS VK (ArkWorks BLS12-381 composite)
[1096..2728) hints_signature         — hinTS aggregate signature (1632 bytes)
[2728..)     suffix — one of:
               192 bytes: aggregate_schnorr_signature (genesis path, total = 2920)
               704 bytes: wraps_proof, Nova IVC ProofData on BN254 (settled path, total = 3432)
```

### 192-byte Schnorr Suffix Structure

Serialized via ArkWorks `serialize_uncompressed` as `(BitVector, Signature)`:

| Offset | Size | Field |
|---|---|---|
| 0 | 128 | bitvector (1 byte per bool, MAX_AB_SIZE=128 entries) |
| 128 | 32 | prover_response (JubJub Fr, LE) |
| 160 | 32 | verifier_challenge (JubJub Fr, LE) |

### 704-byte WRAPS ProofData Structure

Serialized via ArkWorks `CanonicalSerialize` (compressed mode). This is a Nova IVC decider
bundle, **not** a bare Groth16 proof:

| Offset | Size | Field |
|---|---|---|
| 0 | 32 | `i`: Fr (IVC step counter) |
| 32 | 72 | `z_0`: Vec\<Fr\> (8B prefix + 2 × 32B; z_0[0] = genesis AB hash) |
| 104 | 72 | `z_i`: Vec\<Fr\> (8B prefix + 2 × 32B; z_i[1] = Poseidon(hinTS VK)) |
| 176 | 72 | `U_i_commitments`: Vec\<G1\> (8B prefix + 2 × 32B compressed) |
| 248 | 72 | `u_i_commitments`: Vec\<G1\> (8B prefix + 2 × 32B compressed) |
| 320 | 128 | Groth16 proof: A(G1 32B) + B(G2 64B) + C(G1 32B) |
| 448 | 128 | KZG proofs: 2 × (eval:Fr 32B + proof:G1 32B) |
| 576 | 32 | cmT: G1 (fold commitment) |
| 608 | 32 | r: Fr (fold scalar) |
| 640 | 64 | kzg_challenges: 2 × Fr |

### 192-byte Node Address Book Key Structure

Each `historyProofKey` in the `LedgerIdPublicationTransactionBody` is a
`(SchnorrPubKey, SchnorrPoK)` pair serialized uncompressed:

| Offset | Size | Field |
|---|---|---|
| 0 | 64 | public key (JubJub affine x,y uncompressed) |
| 64 | 64 | PoK commitment (JubJub affine x,y) |
| 128 | 32 | PoK challenge (JubJub Fr) |
| 160 | 32 | PoK response (JubJub Fr) |

## Key Findings

### What works today in pure JS/TS

| Verification step | Status | Detail |
|---|---|---|
| Block root recomputation (SHA-384 Merkle) | Working | Output matches Java oracle |
| Bootstrap extraction | Working | ledger ID, WRAPS VK, node contributions |
| Schnorr aggregate signature | **VERIFIED** | BabyJubjub + Blake2s + Poseidon BN254 |
| WRAPS proof deserialization | **Working** | All 704 bytes parsed, ledger ID cross-check passes |
| WRAPS proof cryptographic verification | Not tractable | Requires Nova IVC verifier (no JS impl) |
| hinTS aggregate signature | Not tractable | Custom ArkWorks BLS12-381 composite format |

### Schnorr Verification Details

The Schnorr verification was implemented from scratch by reading the `hedera-cryptography` source:

- **Curve:** BabyJubjub (`ark_ed_on_bn254`), rescaled form: a=1, d=168696/168700 mod p.
  Defined using `@noble/curves/abstract/edwards`.
- **Generator:** ArkWorks prime-order subgroup generator
  (Gx=`19698561...5298`, Gy=`19298250...7839`).
- **Rotation message:** `ledgerId || Poseidon(hinTS_VK)` where the Poseidon hash uses
  t=5, fullRounds=8, partialRounds=60, alpha=5, rate=4, capacity=1 (matches
  `poseidon_canonical_config::<Fr>()`). Implemented via `@noble/curves/abstract/poseidon`.
- **Challenge recomputation:** `e' = from_le_bytes_mod_order(Blake2s(serialize(aggPK) || serialize(R) || msg)[0..31])`
  via `@noble/hashes/blake2`.
- **Aggregate public key:** sum of selected node public keys based on bitvector.

### WRAPS Proof Analysis (from `hedera-cryptography` source)

After gaining access to the private `hedera-cryptography` repo, we confirmed:

- **The 704-byte WRAPS proof is a Nova IVC `ProofData` bundle**, not a bare Groth16 proof.
- The proof scheme is Nova folding over BN254/Grumpkin curve cycle with a Groth16 decider.
- The inner Groth16 proof is nested inside `EthProof` alongside KZG proofs and fold data.
- **snarkjs cannot verify this** because it requires a Nova IVC verifier, not just Groth16.
- The public inputs (`z_0[0]` = genesis AB hash, `z_i[1]` = Poseidon(hinTS VK)) are embedded
  inside the proof blob and can be extracted via deserialization.
- The WRAPS verification key (`decider_vp`) is 1768 bytes, hard-coded in
  `WRAPSVerificationKey.java`.

### Curves Used

| Component | Curve | Source |
|---|---|---|
| WRAPS (Nova + Groth16 decider) | BN254 | `ark_bn254::Bn254` |
| Nova cycle companion | Grumpkin | `ark_grumpkin::Projective` |
| Schnorr keys | BabyJubjub | `ark_ed_on_bn254::EdwardsProjective` |
| hinTS | BLS12-381 | `ark_bls12_381::Bls12_381` |

### Hashing

- **Block root:** SHA-384 (Node.js `crypto` module)
- **Ledger ID:** Poseidon over BN254 Fr (address book public keys, weights, node IDs)
- **Schnorr challenge:** Blake2s
- **hinTS VK hash:** Poseidon over BN254 Fr (VK bytes chunked into 32-byte LE Fr elements)

## Updated Tractability Conclusion

> **The Schnorr fallback path (genesis/pre-settled blocks) is fully verifiable in pure JS.**

This was demonstrated by implementing BabyJubjub curve arithmetic, Poseidon hashing, and
Blake2s-based Schnorr verification entirely in TypeScript using `@noble/curves` and
`@noble/hashes`.

> **Full WRAPS verification is not tractable with off-the-shelf JS libraries.**

The 704-byte WRAPS proof requires a Nova IVC verifier. No such implementation exists in
JavaScript. The most credible path to full WRAPS verification is a WASM build of the Rust
verifier from `hedera-cryptography`.

> **hinTS aggregate signature verification is not tractable in JS.**

The 1096-byte VK and 1632-byte signature are ArkWorks-serialized BLS12-381 composite
structures, not standard points. A custom deserializer plus the full hinTS verification
algorithm would be needed.

## Open Blockers

1. **Nova IVC verifier in JS/WASM** — required for WRAPS proof cryptographic verification.
   The Rust verifier in `hedera-cryptography` is self-contained and could potentially compile
   to WASM, but this has not been tested.
2. **hinTS VK/signature internal byte layout** — the exact per-field breakdown of the 1096-byte
   and 1632-byte ArkWorks-serialized BLS12-381 structures has not been traced from the Rust code.
3. **hinTS aggregate verification algorithm in JS** — even with a deserializer, the full hinTS
   verification algorithm would need to be reimplemented.

## Resolved Questions (Previously Open)

- **704-byte WRAPS structure:** Fully mapped — it is a Nova IVC ProofData bundle (see table above).
- **WRAPS public inputs:** Encoded inside ProofData as `z_0` and `z_i` vectors.
- **ArkWorks → snarkjs compatibility:** Not applicable — the proof is Nova, not bare Groth16.
- **Schnorr verification feasibility:** Confirmed working in pure JS.
- **Poseidon parameters:** t=5, fullRounds=8, partialRounds=60, alpha=5, rate=4, capacity=1.
- **BabyJubjub curve parameters:** a=1, d=9706598848417545097372247223557719406784115219466060233080913168975159366771.

## Reference Observations

```
block-0:
  block root:     83871f1fbc0bcbdaa6c5f08b29fb6520aa692e02bfe2cc36e6d1c876559baacb95ce51f1fb1b4a542ed6476149eac67a
  ledger ID:      60c64bef22e069e5ba0043363059c7dfdad92e19592431230597ef3dfdc2521b
  proof total:    2920 bytes (genesis-schnorr)
  Schnorr:        VERIFIED (2/2 signers)

block-1000:
  block root:     ed32913bfc0362bbbdd39b61b1959daf032cc48eafce163b83e2e647e545b11d7785cd7df2432d72b194ea0cb586e9fe
  proof total:    3432 bytes (wraps)
  WRAPS deser:    SUCCESS (IVC step=2, ledger ID MATCH)
```

## Related Documents

- `tss.md` (in this directory) — proposed JS/TS library architecture and phased plan
- `docs/design/tss-block-proof-verification.md` (repo root) — existing block proof verification design
