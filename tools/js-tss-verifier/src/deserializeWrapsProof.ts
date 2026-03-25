/**
 * Deserializes the 704-byte WRAPS compressed proof suffix into its constituent fields.
 *
 * The 704 bytes are a `ProofData` struct serialized with ArkWorks `CanonicalSerialize`
 * (compressed mode) on BN254. The layout is:
 *
 *   Offset   Size   Field
 *   0        32     i: Fr (IVC step counter)
 *   32       8      len(z_0): u64 LE (expect 2)
 *   40       64     z_0[0], z_0[1]: Fr × 2
 *   104      8      len(z_i): u64 LE (expect 2)
 *   112      64     z_i[0], z_i[1]: Fr × 2
 *   176      8      len(U_i_commitments): u64 LE (expect 2)
 *   184      64     U_i[0], U_i[1]: G1 compressed × 2
 *   248      8      len(u_i_commitments): u64 LE (expect 2)
 *   256      64     u_i[0], u_i[1]: G1 compressed × 2
 *   320      32     snark_proof.a: G1 compressed
 *   352      64     snark_proof.b: G2 compressed
 *   416      32     snark_proof.c: G1 compressed
 *   448      32     kzg_proofs[0].eval: Fr
 *   480      32     kzg_proofs[0].proof: G1 compressed
 *   512      32     kzg_proofs[1].eval: Fr
 *   544      32     kzg_proofs[1].proof: G1 compressed
 *   576      32     cmT: G1 compressed
 *   608      32     r: Fr
 *   640      32     kzg_challenges[0]: Fr
 *   672      32     kzg_challenges[1]: Fr
 *   Total:  704
 *
 * All field elements are BN254 Fr (32 bytes, little-endian).
 * Compressed G1 is 32 bytes: LE x-coordinate with sign flag in bit 255.
 * Compressed G2 is 64 bytes: LE Fp2 x-coordinate (c0: bytes[0..32], c1: bytes[32..64])
 *   with sign flag in bit 255 of the last byte.
 */

import type {
  WrapsProofData,
  WrapsEthProof,
  WrapsKzgEvalProof,
  WrapsDeserializationResult,
  BootstrapPublicationSummary,
} from "./types.js";

const BN254_FR_ORDER = 0x30644e72e131a029b85045b68181585d2833e84879b9709143e1f593f0000001n;
const EXPECTED_PROOF_LENGTH = 704;

/** Read a 32-byte little-endian unsigned integer from a buffer at the given offset. */
function readU256LE(buf: Buffer, offset: number): bigint {
  let value = 0n;
  for (let i = 31; i >= 0; i--) {
    value = (value << 8n) | BigInt(buf[offset + i]);
  }
  return value;
}

/** Read an 8-byte little-endian u64 from a buffer. */
function readU64LE(buf: Buffer, offset: number): number {
  // Safe for lengths that fit in JS number (up to 2^53)
  let value = 0n;
  for (let i = 7; i >= 0; i--) {
    value = (value << 8n) | BigInt(buf[offset + i]);
  }
  return Number(value);
}

/** Read a BN254 Fr element (32 bytes LE). Returns the raw value without the sign flag. */
function readFr(buf: Buffer, offset: number): bigint {
  return readU256LE(buf, offset);
}

/**
 * Read a compressed BN254 G1 point (32 bytes).
 * Returns the x-coordinate as bigint (sign flag stripped).
 * The sign flag for y is in bit 255 of the last byte.
 */
function readG1Compressed(buf: Buffer, offset: number): bigint {
  const raw = readU256LE(buf, offset);
  // Strip the sign flag (bit 255) to get the x-coordinate
  const mask = (1n << 255n) - 1n;
  return raw & mask;
}

/**
 * Read a compressed BN254 G2 point (64 bytes).
 * Returns the Fp2 x-coordinate as {c0, c1}.
 * c0 is in bytes [0..32), c1 is in bytes [32..64).
 * Sign flag for y is in bit 255 of the last byte (of c1).
 */
function readG2Compressed(buf: Buffer, offset: number): { c0: bigint; c1: bigint } {
  const c0 = readU256LE(buf, offset);
  const rawC1 = readU256LE(buf, offset + 32);
  const mask = (1n << 255n) - 1n;
  const c1 = rawC1 & mask;
  return { c0, c1 };
}

/**
 * Read a Vec<Fr> (8-byte length prefix + N × 32-byte Fr elements).
 * Returns the parsed elements and the next offset.
 */
function readVecFr(buf: Buffer, offset: number): { values: bigint[]; nextOffset: number } {
  const len = readU64LE(buf, offset);
  offset += 8;
  const values: bigint[] = [];
  for (let i = 0; i < len; i++) {
    values.push(readFr(buf, offset));
    offset += 32;
  }
  return { values, nextOffset: offset };
}

/**
 * Read a Vec<G1> (8-byte length prefix + N × 32-byte compressed G1 points).
 * Returns the x-coordinates and the next offset.
 */
function readVecG1(buf: Buffer, offset: number): { values: bigint[]; nextOffset: number } {
  const len = readU64LE(buf, offset);
  offset += 8;
  const values: bigint[] = [];
  for (let i = 0; i < len; i++) {
    values.push(readG1Compressed(buf, offset));
    offset += 32;
  }
  return { values, nextOffset: offset };
}

function deserializeProofData(buf: Buffer): WrapsProofData {
  let offset = 0;

  // i: Fr
  const i = readFr(buf, offset);
  offset += 32;

  // z_0: Vec<Fr>
  const z0 = readVecFr(buf, offset);
  offset = z0.nextOffset;

  // z_i: Vec<Fr>
  const zi = readVecFr(buf, offset);
  offset = zi.nextOffset;

  // U_i_commitments: Vec<G1>
  const Ui = readVecG1(buf, offset);
  offset = Ui.nextOffset;

  // u_i_commitments: Vec<G1>
  const ui = readVecG1(buf, offset);
  offset = ui.nextOffset;

  // EthProof fields (no Vec wrappers — fixed layout)
  const groth16A = readG1Compressed(buf, offset);
  offset += 32;

  const groth16B = readG2Compressed(buf, offset);
  offset += 64;

  const groth16C = readG1Compressed(buf, offset);
  offset += 32;

  // kzg_proofs: [KzgEvalProof; 2]
  const kzgProof0Eval = readFr(buf, offset);
  offset += 32;
  const kzgProof0Proof = readG1Compressed(buf, offset);
  offset += 32;
  const kzgProof1Eval = readFr(buf, offset);
  offset += 32;
  const kzgProof1Proof = readG1Compressed(buf, offset);
  offset += 32;

  const cmT = readG1Compressed(buf, offset);
  offset += 32;

  const r = readFr(buf, offset);
  offset += 32;

  const kzgChallenge0 = readFr(buf, offset);
  offset += 32;
  const kzgChallenge1 = readFr(buf, offset);
  offset += 32;

  if (offset !== EXPECTED_PROOF_LENGTH) {
    throw new Error(`Deserialized ${offset} bytes but expected ${EXPECTED_PROOF_LENGTH}`);
  }

  const kzgProofs: [WrapsKzgEvalProof, WrapsKzgEvalProof] = [
    { eval: kzgProof0Eval, proof: kzgProof0Proof },
    { eval: kzgProof1Eval, proof: kzgProof1Proof },
  ];

  const ethProof: WrapsEthProof = {
    groth16A,
    groth16B,
    groth16C,
    kzgProofs,
    cmT,
    r,
    kzgChallenges: [kzgChallenge0, kzgChallenge1],
  };

  return {
    i,
    z_0: z0.values,
    z_i: zi.values,
    U_i_commitments: Ui.values,
    u_i_commitments: ui.values,
    ethProof,
  };
}

function frToHex(v: bigint): string {
  return "0x" + v.toString(16).padStart(64, "0");
}

export function deserializeWrapsProofSuffix(
  suffixBytes: Buffer,
  bootstrap: BootstrapPublicationSummary | null,
): WrapsDeserializationResult {
  if (suffixBytes.length !== EXPECTED_PROOF_LENGTH) {
    return {
      ok: false,
      error: `Expected ${EXPECTED_PROOF_LENGTH} bytes, got ${suffixBytes.length}`,
    };
  }

  try {
    const proofData = deserializeProofData(suffixBytes);

    // Validate basic structural expectations
    if (proofData.z_0.length !== 2) {
      return { ok: false, error: `Expected z_0 length 2, got ${proofData.z_0.length}` };
    }
    if (proofData.z_i.length !== 2) {
      return { ok: false, error: `Expected z_i length 2, got ${proofData.z_i.length}` };
    }
    if (proofData.U_i_commitments.length !== 2) {
      return { ok: false, error: `Expected U_i length 2, got ${proofData.U_i_commitments.length}` };
    }
    if (proofData.u_i_commitments.length !== 2) {
      return { ok: false, error: `Expected u_i length 2, got ${proofData.u_i_commitments.length}` };
    }

    // Validate Fr elements are in range
    const frFields = [
      proofData.i,
      ...proofData.z_0,
      ...proofData.z_i,
      proofData.ethProof.r,
      ...proofData.ethProof.kzgChallenges,
      proofData.ethProof.kzgProofs[0].eval,
      proofData.ethProof.kzgProofs[1].eval,
    ];
    for (const fr of frFields) {
      if (fr >= BN254_FR_ORDER) {
        return { ok: false, error: `Fr element ${frToHex(fr)} exceeds field order` };
      }
    }

    // Check z_0[0] against bootstrap ledger ID if available
    let ledgerIdCheck: WrapsDeserializationResult["ledgerIdCheck"];
    if (bootstrap) {
      const ledgerIdBytes = Buffer.from(bootstrap.ledgerIdHex, "hex");
      // The ledger ID in the fixture is the raw 32-byte Poseidon hash.
      // z_0[0] is the same value as a BN254 Fr element (LE).
      if (ledgerIdBytes.length === 32) {
        let ledgerIdFr = 0n;
        for (let i = 31; i >= 0; i--) {
          ledgerIdFr = (ledgerIdFr << 8n) | BigInt(ledgerIdBytes[i]);
        }
        ledgerIdCheck = {
          expected: frToHex(ledgerIdFr),
          actual: frToHex(proofData.z_0[0]),
          match: ledgerIdFr === proofData.z_0[0],
        };
      }
    }

    return { ok: true, proofData, ledgerIdCheck };
  } catch (err) {
    return {
      ok: false,
      error: err instanceof Error ? err.message : String(err),
    };
  }
}
