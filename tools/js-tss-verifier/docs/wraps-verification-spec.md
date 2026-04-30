# WRAPS Proof Verification Specification for JS/TS

This document specifies the exact algorithm for verifying a WRAPS (Nova/IVC + Groth16 decider)
block proof in JavaScript/TypeScript, using `@noble/curves` BN254 primitives.

It is derived from the Rust implementation in `hedera-cryptography`:
- `verify_compressed_wraps_proof()` — `wraps/src/lib.rs:1223-1262`
- `DeciderEth::verify()` — `folding-schemes/src/folding/nova/decider_eth.rs:210-266`
- Groth16 verify — `groth16/src/verifier.rs:43-64`
- KZG verify — `folding-schemes/src/commitment/kzg.rs:221-246`

**Scope:** Self-contained spec for implementing the verifier without reading Rust source.

**Status:** The 704-byte proof is a composite Nova IVC decider bundle, not a plain Groth16 proof.
Verification requires Groth16 + KZG pairing checks + EC commitment folding — all on BN254.

---

## 1. BN254 Curve Parameters

```
Fp (base field):  0x30644e72e131a029b85045b68181585d97816a916871ca8d3c208c16d87cfd47
Fr (scalar field): 0x30644e72e131a029b85045b68181585d2833e84879b9709143e1f593f0000001

G1: y^2 = x^3 + 3  over Fp
G2: y^2 = x^3 + 3/(9+u)  over Fp2  (D-type twist)

Fp2 tower: Fp[u] / (u^2 + 1)
Fp2 nonresidue: 9 + u

G1 generator: (1, 2)
```

Noble exports: `bn254.fields.Fp`, `bn254.fields.Fp2`, `bn254.fields.Fp12`, `bn254.fields.Fr` (alias `bn254_Fr`).

---

## 2. ArkWorks Compressed Serialization Format

All data uses ArkWorks `CanonicalSerialize` / `CanonicalDeserialize` in **compressed** mode.

### 2.1 Fr (scalar field element) — 32 bytes

- 32 bytes, little-endian.
- Value must be `< Fr.ORDER`.

### 2.2 G1 compressed — 32 bytes

- 32 bytes: LE x-coordinate with **2 flag bits in bits 7-6 of the last byte** (byte[31]).
- Flag layout (confirmed against real fixtures):
  - bit 7 (0x80) = y is "positive" (y <= (p-1)/2, the smaller root)
  - bit 6 (0x40) = point at infinity
- To decompress:
  1. Extract flags: `isPositive = (byte[31] & 0x80) !== 0`, `isInfinity = (byte[31] & 0x40) !== 0`
  2. Read x as 32-byte LE bigint with top 2 bits cleared: `x = raw & ((1n << 254n) - 1n)`
  3. If `isInfinity` or `x === 0n`, return `G1.ZERO`
  4. Compute `y^2 = x^3 + 3` in Fp
  5. Compute `y = Fp.sqrt(y^2)`
  6. If `isPositive` and `y > (p-1)/2`, negate y. If `!isPositive` and `y <= (p-1)/2`, negate y.
  7. Construct: `bn254.G1.Point.fromAffine({ x, y })`

### 2.3 G1 point at infinity

- Encoded as all zeros with bit 6 (0x40) set in last byte.
- Both `U_i_commitments[1]` (U_cmE) and `u_i_commitments[1]` (u_cmE) are typically identity.
- Noble: `bn254.G1.Point.ZERO`

### 2.4 G2 compressed — 64 bytes

- 64 bytes: Fp2 x-coordinate as (c0: bytes[0..32], c1: bytes[32..64]).
- Same 2-bit flag scheme in bits 7-6 of byte[63] (last byte of c1).
- To decompress:
  1. Extract flags from byte[63]: `isPositive = (byte & 0x80) !== 0`, `isInfinity = (byte & 0x40) !== 0`
  2. Read c0 (32 bytes LE) and c1 (32 bytes LE with top 2 bits cleared)
  3. If infinity or both components zero, return `G2.ZERO`
  4. Construct x as Fp2: `x = { c0, c1 }`
  5. Compute `y^2 = x^3 + B_twist` where B_twist = `{c0: 194858747..., c1: 266929791...}`
  6. Compute `y = Fp2.sqrt(y^2)` (available in noble's tower implementation)
  7. Select y based on flag using Fp2 lexicographic ordering:
     - Compare c1 first: if c1 > (p-1)/2 it is "larger"; if c1 == 0 compare c0
     - `isPositive` means y is the smaller (positive) root
  8. Construct: `bn254.G2.Point.fromAffine({ x: {c0, c1}, y: {c0: y.c0, c1: y.c1} })`

### 2.5 Vec\<T\>

- 8-byte LE u64 length prefix, followed by `length` serialized elements.

---

## 3. Data Structure Byte Layouts

### 3.1 ProofData — 704 bytes

This is the `CompressedProofSerialized` blob extracted from the `tssSignature` field.
The `tssSignature` composite layout is:
- `hintsVerificationKey`: bytes [0..1096) — 1096 bytes
- `hintsSignature`: bytes [1096..2728) — 1632 bytes
- `abProof` (WRAPS suffix): bytes [2728..3432) — 704 bytes (when WRAPS, not Schnorr)

The 704-byte ProofData layout:

| Offset | Size | Field | Type | Description |
|--------|------|-------|------|-------------|
| 0 | 32 | `i` | Fr | IVC iteration counter |
| 32 | 8 | `len(z_0)` | u64 LE | Always 2 |
| 40 | 32 | `z_0[0]` | Fr | Initial state: genesis address book hash |
| 72 | 32 | `z_0[1]` | Fr | Initial state: initial hints VK hash |
| 104 | 8 | `len(z_i)` | u64 LE | Always 2 |
| 112 | 32 | `z_i[0]` | Fr | Current state: latest address book hash |
| 144 | 32 | `z_i[1]` | Fr | Current state: current hints VK hash |
| 176 | 8 | `len(U_i)` | u64 LE | Always 2 |
| 184 | 32 | `U_i[0]` | G1 | Running instance cmW |
| 216 | 32 | `U_i[1]` | G1 | Running instance cmE |
| 248 | 8 | `len(u_i)` | u64 LE | Always 2 |
| 256 | 32 | `u_i[0]` | G1 | Incoming instance cmW' |
| 288 | 32 | `u_i[1]` | G1 | Incoming instance cmE' (must be identity) |
| 320 | 32 | `snark_proof.A` | G1 | Groth16 proof element A |
| 352 | 64 | `snark_proof.B` | G2 | Groth16 proof element B |
| 416 | 32 | `snark_proof.C` | G1 | Groth16 proof element C |
| 448 | 32 | `kzg_proofs[0].eval` | Fr | KZG evaluation for cmW |
| 480 | 32 | `kzg_proofs[0].proof` | G1 | KZG witness for cmW |
| 512 | 32 | `kzg_proofs[1].eval` | Fr | KZG evaluation for cmE |
| 544 | 32 | `kzg_proofs[1].proof` | G1 | KZG witness for cmE |
| 576 | 32 | `cmT` | G1 | Cross-term commitment |
| 608 | 32 | `r` | Fr | Folding randomness |
| 640 | 32 | `kzg_challenges[0]` | Fr | KZG challenge for cmW |
| 672 | 32 | `kzg_challenges[1]` | Fr | KZG challenge for cmE |

Total: 32 + (8+64) + (8+64) + (8+64) + (8+64) + 32 + 64 + 32 + 32 + 32 + 32 + 32 + 32 + 32 + 32 + 32 = **704**

### 3.2 VerifierParam — 1768 bytes

This is the hard-coded decider verification key. Source: `WRAPSVerificationKey.java` `DEFAULT_KEY` (1768 bytes).
It is a `VerifierParam<G1, KZG::VerifierParams, Groth16::VerifyingKey>` serialized compressed.

| Offset | Size | Field | Type | Description |
|--------|------|-------|------|-------------|
| 0 | 32 | `pp_hash` | Fr | Hash of public parameters |
| 32 | 32 | `alpha_g1` | G1 | Groth16 VK: alpha * G |
| 64 | 64 | `beta_g2` | G2 | Groth16 VK: beta * H |
| 128 | 64 | `gamma_g2` | G2 | Groth16 VK: gamma * H |
| 192 | 64 | `delta_g2` | G2 | Groth16 VK: delta * H |
| 256 | 8 | `len(gamma_abc_g1)` | u64 LE | Expected: 41 |
| 264 | 1312 | `gamma_abc_g1[0..40]` | G1 x 41 | Public input coefficients |
| 1576 | 32 | `kzg_vk.g` | G1 | KZG generator G |
| 1608 | 32 | `kzg_vk.gamma_g` | G1 | KZG gamma * G |
| 1640 | 64 | `kzg_vk.h` | G2 | KZG generator H (in G2) |
| 1704 | 64 | `kzg_vk.beta_h` | G2 | KZG beta * H (in G2) |

Total: 32 + 32 + 64 + 64 + 64 + 8 + 1312 + 32 + 32 + 64 + 64 = **1768**

The `gamma_abc_g1` vector has 41 entries: index 0 is the base term, indices 1-40 correspond
to the 40 public input elements (Section 5.2d).

---

## 4. Inputs to Verification

| Input | Source | Size |
|-------|--------|------|
| `proofData` | Extracted from `tssSignature` suffix (704 bytes) | 704 B |
| `verifierParam` | Hard-coded `WRAPSVerificationKey.DEFAULT_KEY` | 1768 B |
| `ab_genesis_hash` | Ledger ID from `LedgerIdPublicationTransactionBody` in block 0 | 32 B (Fr) |
| `hints_vk_bytes` | First 1096 bytes of `tssSignature` | 1096 B |

---

## 5. Verification Algorithm

### 5.0 Deserialize

```
proofData  = deserialize_ProofData(proof_bytes)     // Section 3.1
vp         = deserialize_VerifierParam(vk_bytes)     // Section 3.2
```

Deserialization of ProofData is already implemented in `deserializeWrapsProof.ts`.
The new work is deserializing VerifierParam and decompressing G1/G2 points to actual curve points.

### 5.1 State Consistency Checks

**Check A: Ledger ID**

```
assert(proofData.z_0[0] == ab_genesis_hash)
```

The genesis address book hash (ledger ID) must match the initial IVC state.

**Check B: Hints VK Hash**

```
hints_vk_hash = hash_hints_vk(hints_vk_bytes)
assert(proofData.z_i[1] == hints_vk_hash)
```

`hash_hints_vk` computes a Poseidon hash over the hints verification key:

1. Pad `hints_vk_bytes` (1096 bytes) to a multiple of 32 bytes if needed
2. Split into 32-byte chunks
3. Interpret each chunk as a BN254 Fr element via `from_le_bytes_mod_order`:
   - Read as LE bigint
   - Reduce modulo Fr.ORDER
4. Hash all Fr elements using Poseidon with parameters:
   - t=5, fullRounds=8, partialRounds=60, alpha=5, rate=4, capacity=1
   - (Same Poseidon config already implemented in `verifySchnorr.ts`)

### 5.2 Nova Decider Verification

Source: `DeciderEth::verify()` in `decider_eth.rs:210-266`

#### 5.2a Guard

```
assert(proofData.i > 1)    // At least 2 IVC folding steps
```

#### 5.2b Fold Commitments (Native EC Operations)

Source: `fold_group_elements_native()` in `decider_eth_circuit.rs:183-200`

```
r = proofData.ethProof.r

// Decompress all commitment points to G1 curve points
U_cmW = decompress_G1(proofData.U_i_commitments[0])
U_cmE = decompress_G1(proofData.U_i_commitments[1])
u_cmW = decompress_G1(proofData.u_i_commitments[0])
u_cmE = decompress_G1(proofData.u_i_commitments[1])
cmT   = decompress_G1(proofData.ethProof.cmT)

// u_cmE MUST be the identity point (point at infinity)
assert(u_cmE == G1.ZERO)

// Fold:
cmW_final = U_cmW.add(u_cmW.multiply(r))     // EC scalar mul + EC add on G1
cmE_final = U_cmE.add(cmT.multiply(r))        // EC scalar mul + EC add on G1
```

Note: The Rust code skips the `u_cmE * r^2` term because `u_cmE` is enforced to be zero.

#### 5.2c Nonnative Point Encoding

Source: `inputize_nonnative()` in `nonnative/affine.rs:180-185` and `nonnative/uint.rs:846-852`

To include G1 points in the Groth16 public input vector, each point's affine (x, y) coordinates
(which are Fp elements, not Fr elements) must be split into Fr-sized limbs.

**Algorithm:**

For a G1 affine point with coordinates (x, y) where x, y are Fp values:

```
BITS_PER_LIMB = 55
LIMB_MASK = (1n << 55n) - 1n

function fpToLimbs(v: bigint): bigint[] {
    return [
        (v >>   0n) & LIMB_MASK,   // limb 0: bits [0..55)
        (v >>  55n) & LIMB_MASK,   // limb 1: bits [55..110)
        (v >> 110n) & LIMB_MASK,   // limb 2: bits [110..165)
        (v >> 165n) & LIMB_MASK,   // limb 3: bits [165..220)
        (v >> 220n) & LIMB_MASK,   // limb 4: bits [220..254)
    ]
}

function g1ToNonnative(point: G1Point): bigint[] {
    const { x, y } = point.toAffine()
    return [...fpToLimbs(x), ...fpToLimbs(y)]    // 10 Fr elements
}
```

Each coordinate produces 5 limbs (ceil(254/55) = 5). Each G1 point produces 10 Fr elements.

#### 5.2d Construct Groth16 Public Input Vector (40 Fr Elements)

Source: `decider_eth.rs:238-247`

```
public_input = [
    vp.pp_hash,                         //  1 element  (Fr)
    proofData.i,                         //  1 element  (Fr)
    proofData.z_0[0],                    //  1 element  (Fr)
    proofData.z_0[1],                    //  1 element  (Fr)
    proofData.z_i[0],                    //  1 element  (Fr)
    proofData.z_i[1],                    //  1 element  (Fr)
    ...g1ToNonnative(cmW_final),         // 10 elements (Fp limbs)
    ...g1ToNonnative(cmE_final),         // 10 elements (Fp limbs)
    proofData.ethProof.kzg_challenges[0],//  1 element  (Fr)
    proofData.ethProof.kzg_challenges[1],//  1 element  (Fr)
    proofData.ethProof.kzg_proofs[0].eval,// 1 element  (Fr)
    proofData.ethProof.kzg_proofs[1].eval,// 1 element  (Fr)
    ...g1ToNonnative(cmT_point),         // 10 elements (Fp limbs)
]

assert(public_input.length == 40)
```

**This ordering is critical.** A single transposition invalidates the Groth16 check.

#### 5.2e Groth16 SNARK Verification

Source: `groth16/src/verifier.rs:27-64`

**Step 1: Prepare inputs**

```
// gamma_abc_g1 has 41 entries from the VerifierParam
// Index 0 is the base, indices 1..40 correspond to public_input[0..39]

I = vp.gamma_abc_g1[0]
for j in 0..39:
    I = I.add(vp.gamma_abc_g1[j + 1].multiply(public_input[j]))
```

This computes: `I = gamma_abc_g1[0] + SUM(j=0..39, public_input[j] * gamma_abc_g1[j+1])`

**Step 2: Pairing check**

The Groth16 verification equation is:

```
e(A, B) * e(I, -gamma_g2) * e(C, -delta_g2) == e(alpha_g1, beta_g2)
```

Rearranged for a single batch pairing check (product == 1):

```
e(A, B) * e(I, -gamma_g2) * e(C, -delta_g2) * e(-alpha_g1, beta_g2) == 1
```

Noble implementation:

```typescript
const A = decompress_G1(proofData.ethProof.groth16A)
const B = decompress_G2(proofData.ethProof.groth16B)
const C = decompress_G1(proofData.ethProof.groth16C)

const neg_gamma_g2  = vp.gamma_g2.negate()
const neg_delta_g2  = vp.delta_g2.negate()
const neg_alpha_g1  = vp.alpha_g1.negate()

const result = bn254.pairingBatch([
    { g1: A,             g2: B },
    { g1: I,             g2: neg_gamma_g2 },
    { g1: C,             g2: neg_delta_g2 },
    { g1: neg_alpha_g1,  g2: vp.beta_g2 },
])

const groth16_valid = bn254.fields.Fp12.eql(result, bn254.fields.Fp12.ONE)
```

#### 5.2f KZG Proof Verification (2 proofs)

Source: `kzg.rs:221-246`, called in `decider_eth.rs:256-263`

For each j in {0, 1}:

```
cm        = [cmW_final, cmE_final][j]        // Folded commitment (G1 point)
challenge = proofData.ethProof.kzg_challenges[j]   // Fr
eval_val  = proofData.ethProof.kzg_proofs[j].eval  // Fr
w         = decompress_G1(proofData.ethProof.kzg_proofs[j].proof)  // G1

G      = vp.kzg_vk.g       // G1 generator from KZG VK
H      = vp.kzg_vk.h       // G2 element from KZG VK
beta_H = vp.kzg_vk.beta_h  // G2 element from KZG VK
```

The KZG verification equation:

```
e(cm - eval_val * G,  H)  ==  e(w,  beta_H - challenge * H)
```

Rearranged for batch check:

```
e(cm - eval_val * G, H) * e(-w, beta_H - challenge * H) == 1
```

Noble implementation:

```typescript
const lhs_g1 = cm.add(G.multiply(eval_val).negate())         // cm - eval*G
const rhs_g2 = beta_H.add(H.multiply(challenge).negate())    // beta_H - challenge*H

const kzg_valid = bn254.fields.Fp12.eql(
    bn254.pairingBatch([
        { g1: lhs_g1,       g2: H },
        { g1: w.negate(),   g2: rhs_g2 },
    ]),
    bn254.fields.Fp12.ONE
)
```

Note: `H.multiply(challenge)` is a G2 scalar multiplication. This is supported by noble's
`ProjectivePoint.multiply()` on G2.

### 5.3 Final Result

```
verified = groth16_valid && kzg0_valid && kzg1_valid
           && (proofData.z_0[0] == ab_genesis_hash)
           && (proofData.z_i[1] == hints_vk_hash)
```

---

## 6. Operation Inventory

| Operation | Count | Noble API |
|-----------|-------|-----------|
| G1 point decompression | ~14 | Manual sqrt + `G1.ProjectivePoint.fromAffine({x, y})` |
| G2 point decompression | 5 (B, gamma_g2, delta_g2, kzg.h, kzg.beta_h) | Manual Fp2.sqrt + `G2.ProjectivePoint.fromAffine(...)` |
| G1 scalar multiplication | ~46 (40 input prep + 2 fold + 4 KZG) | `point.multiply(scalar)` |
| G2 scalar multiplication | 2 (KZG challenge * H) | `point.multiply(scalar)` |
| G1 point addition | ~44 (40 input prep + 2 fold + 2 KZG) | `point.add(other)` |
| G2 point addition | 2 (KZG rhs_g2) | `point.add(other)` |
| G1/G2 negate | ~6 | `point.negate()` |
| Pairing (batched) | 4 (Groth16) + 2 + 2 (KZG x2) = 8 | `bn254.pairingBatch([...])` |
| Fp sqrt | ~14 (G1 decompression) | `bn254.fields.Fp.sqrt(v)` |
| Fp2 sqrt | 5 (G2 decompression) | `bn254.fields.Fp2.sqrt(v)` |
| Poseidon hash | 1 (hints VK) | Reuse from `verifySchnorr.ts` |
| Fr comparison | 3 (state checks + iteration guard) | BigInt equality |

---

## 7. Implementation Notes

### 7.1 Noble BN254 API Caveats

- `bn254.G1.Point` and `bn254.G2.Point` do **not** have `fromBytes` / `toBytes` methods.
  There is no standard encoding. Points must be constructed via `fromAffine({x, y})`.
- Decompression (solving for y from compressed x) must be implemented manually.
- `bn254.fields.Fp.sqrt()` is available (BN254 Fp has p % 4 == 3, so sqrt is fast).
- `bn254.fields.Fp2.sqrt()` is available in the tower implementation.
- `pairingBatch` returns `Fp12`; compare with `Fp12.eql(result, Fp12.ONE)`.

### 7.2 ArkWorks vs Noble Sign Conventions

ArkWorks "positive" for Fp: `y <= (p-1)/2`.
ArkWorks "positive" for Fp2: lexicographic ordering — compare c1 first, then c0.
The sign flag in compressed encoding means: `flag == 1 => y is the LARGER root`.

This must be matched exactly or all point decompressions will produce wrong y values,
silently breaking every pairing check.

### 7.3 Nonnative Limb Encoding Precision

The 55-bit limb split must exactly match the Rust `NonNativeUintVar` implementation.
Source: `nonnative/uint.rs:190-204`.

For BN254 Fp (~254 bits): ceil(254/55) = 5 limbs.
Limb boundaries: [0,55), [55,110), [110,165), [165,220), [220,254).

A single off-by-one in limb boundaries will produce a valid-looking but incorrect public input
vector, causing the Groth16 check to reject a valid proof.

### 7.4 Point at Infinity Handling

The `u_i_commitments[1]` field (u_cmE) must be the G1 identity point. ArkWorks uses a
dedicated encoding for point at infinity — typically all zero bytes with specific flag bits.
The existing `readG1Compressed` strips the sign flag but does not explicitly detect infinity.
The decompression logic should handle this case: if all coordinate bytes are zero, return `G1.ZERO`.

### 7.5 Performance

8 BN254 pairings in JS will likely take several seconds. This is acceptable for a verification
spike but would need optimization (e.g., WASM backend) for production use.

### 7.6 VerifierParam Source

The 1768-byte VK is hard-coded in:
```
hedera-cryptography/cryptography/hedera-cryptography-wraps/
  src/main/java/com/hedera/cryptography/wraps/WRAPSVerificationKey.java
```

It can also be obtained from the `historyProofVerificationKey` field in
`LedgerIdPublicationTransactionBody` in block 0 bootstrap data.

---

## 8. Verification Flow Diagram

```
                          ┌──────────────────────────────────┐
                          │  Deserialize ProofData (704 B)   │
                          │  Deserialize VerifierParam (1768 B) │
                          └──────────────┬───────────────────┘
                                         │
                    ┌────────────────────┼────────────────────┐
                    ▼                    ▼                    ▼
          ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
          │ Check A:        │  │ Check B:        │  │ Guard:          │
          │ z_0[0] ==       │  │ z_i[1] ==       │  │ i > 1           │
          │ genesis_hash    │  │ hash(hints_vk)  │  │                 │
          └────────┬────────┘  └────────┬────────┘  └────────┬────────┘
                   │                    │                    │
                   │                    │                    ▼
                   │                    │          ┌─────────────────┐
                   │                    │          │ Fold commitments│
                   │                    │          │ cmW_final, cmE  │
                   │                    │          └────────┬────────┘
                   │                    │                   │
                   │                    │                   ▼
                   │                    │          ┌─────────────────┐
                   │                    │          │ Nonnative encode│
                   │                    │          │ → 40 Fr inputs  │
                   │                    │          └────────┬────────┘
                   │                    │                   │
                   │                    │          ┌────────┴────────┐
                   │                    │          ▼                 ▼
                   │                    │  ┌──────────────┐ ┌──────────────┐
                   │                    │  │ Groth16      │ │ KZG verify   │
                   │                    │  │ 4 pairings   │ │ 2+2 pairings │
                   │                    │  └──────┬───────┘ └──────┬───────┘
                   │                    │         │                │
                   └────────────────────┴─────────┴────────────────┘
                                         │
                                         ▼
                                 ┌───────────────┐
                                 │ ALL checks &&  │
                                 │ → verified     │
                                 └───────────────┘
```

---

## 9. Existing Code to Reuse

| Existing | Location | Reuse for |
|----------|----------|-----------|
| ProofData deserialization | `src/deserializeWrapsProof.ts` | Parsing 704-byte proof (already working) |
| Fr/G1/G2 byte readers | `src/deserializeWrapsProof.ts` | Reading compressed elements |
| Poseidon hash (BN254 Fr) | `src/verifySchnorr.ts` | Computing `hash_hints_vk()` |
| BabyJubjub + Schnorr | `src/verifySchnorr.ts` | Already verified; not needed for WRAPS |
| TS types | `src/types.ts` | `WrapsProofData`, `WrapsEthProof`, etc. |
| Bootstrap extraction | `src/extractBootstrap.ts` | Getting ledger ID and hints VK |

**New code needed:**
1. ArkWorks G1/G2 compressed point decompression (x → curve point)
2. VerifierParam (1768-byte VK) deserialization
3. Nonnative limb encoding (`fpToLimbs`)
4. Groth16 verification (input preparation + pairing batch)
5. KZG verification (2 pairing checks)
6. Top-level `verifyWrapsProof()` orchestrating all steps

---

## 10. Risks and Open Questions

1. **Fp2 sqrt sign convention**: Must match ArkWorks lexicographic ordering exactly for G2 decompression.
   Needs careful testing against a known G2 point.

2. **gamma_abc_g1 length**: Inferred as 41 from byte budget arithmetic (1768 total).
   Should be validated by reading the Vec length prefix from a real VK fixture.

3. **Poseidon for hints VK**: The exact chunking (32-byte blocks) and `from_le_bytes_mod_order`
   reduction needs to match the Rust `hash_hints_vk()` implementation.
   The Poseidon config (t=5, fullRounds=8, partialRounds=60) is already proven in Schnorr verification.

4. **No test vector yet**: Without a confirmed WRAPS proof + expected intermediate values,
   we can only validate structural correctness. A test vector from the crypto team would
   allow step-by-step validation.

5. **Performance**: BN254 pairings in pure JS may take 5-15 seconds for the full verification.
   Acceptable for spike; production would need WASM acceleration.
