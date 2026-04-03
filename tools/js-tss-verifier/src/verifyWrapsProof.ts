/**
 * WRAPS proof verification: Groth16 + KZG pairing checks on BN254.
 *
 * Implements the algorithm from docs/wraps-verification-spec.md:
 *   1. State consistency: z_0[0] == genesis_hash, z_i[1] == hash(hints_vk)
 *   2. Fold commitments: cmW_final, cmE_final
 *   3. Nonnative encoding: G1 affine → 55-bit limbs
 *   4. Groth16 pairing (4 pairings)
 *   5. KZG pairings (2 × 2 pairings)
 *
 * Source: hedera-cryptography verify_compressed_wraps_proof() and DeciderEth::verify()
 */

import { bn254 } from "@noble/curves/bn254.js";
import { readU256LE } from "./byteReaders.js";
import { decompressG1, decompressG2, type G1Point, type G2Point } from "./bn254Decompress.js";
import { deserializeVerifierParam, type VerifierParam } from "./deserializeVerifierParam.js";
import { deserializeWrapsProofSuffix } from "./deserializeWrapsProof.js";
import { hashHintsVk } from "./verifySchnorr.js";
import type {
  BootstrapPublicationSummary,
  ProofLayout,
  WrapsVerificationResult,
  WrapsVerificationChecks,
} from "./types.js";

const { Fp12 } = bn254.fields;
const G1 = bn254.G1.Point;

// ─── Nonnative limb encoding ────────────────────────────────────────────────
// Each BN254 Fp coordinate → 5 limbs of 55 bits.
// See nonnative/uint.rs:190 and nonnative/affine.rs:180-185.

const BITS_PER_LIMB = 55n;
const LIMB_MASK = (1n << BITS_PER_LIMB) - 1n;

function fpToLimbs(v: bigint): bigint[] {
  return [
    (v >> 0n) & LIMB_MASK,
    (v >> 55n) & LIMB_MASK,
    (v >> 110n) & LIMB_MASK,
    (v >> 165n) & LIMB_MASK,
    (v >> 220n) & LIMB_MASK,
  ];
}

function g1ToNonnative(point: G1Point): bigint[] {
  const aff = point.toAffine();
  return [...fpToLimbs(aff.x), ...fpToLimbs(aff.y)];
}

// ─── Groth16 verification ───────────────────────────────────────────────────

function verifyGroth16(
  publicInput: bigint[],
  proofA: G1Point,
  proofB: G2Point,
  proofC: G1Point,
  vp: VerifierParam,
): boolean {
  // Prepare inputs: I = gamma_abc_g1[0] + SUM(x_i * gamma_abc_g1[i+1])
  let I = vp.gammaAbcG1[0];
  for (let j = 0; j < publicInput.length; j++) {
    if (publicInput[j] !== 0n) {
      I = I.add(vp.gammaAbcG1[j + 1].multiply(publicInput[j]));
    }
  }

  // Pairing check: e(A,B) * e(I,-γG2) * e(C,-δG2) * e(-αG1,βG2) == 1
  const result = bn254.pairingBatch([
    { g1: proofA, g2: proofB },
    { g1: I, g2: vp.gammaG2.negate() },
    { g1: proofC, g2: vp.deltaG2.negate() },
    { g1: vp.alphaG1.negate(), g2: vp.betaG2 },
  ]);

  return Fp12.eql(result, Fp12.ONE);
}

// ─── KZG verification ───────────────────────────────────────────────────────

function verifyKzg(
  cm: G1Point,
  challenge: bigint,
  evalVal: bigint,
  witness: G1Point,
  vp: VerifierParam,
): boolean {
  const { g, h, betaH } = vp.kzgVk;

  // e(cm - eval*G, H) * e(-w, betaH - challenge*H) == 1
  const lhsG1 = cm.add(g.multiply(evalVal).negate());
  const rhsG2 = betaH.add(h.multiply(challenge).negate());

  const result = bn254.pairingBatch([
    { g1: lhsG1, g2: h },
    { g1: witness.negate(), g2: rhsG2 },
  ]);

  return Fp12.eql(result, Fp12.ONE);
}

// ─── Main verification entry point ──────────────────────────────────────────

export function verifyWrapsProof(
  proofLayout: ProofLayout,
  bootstrap: BootstrapPublicationSummary | null,
): WrapsVerificationResult {
  if (proofLayout.suffixKind !== "wraps-compressed-proof") {
    return { attempted: false, status: "skipped", reason: "Not a WRAPS-path proof." };
  }

  if (!bootstrap) {
    return {
      attempted: false,
      status: "skipped",
      reason: "No bootstrap publication available (need VK and ledger ID).",
    };
  }

  const startMs = performance.now();

  try {
    // ── Deserialize proof ──
    const deser = deserializeWrapsProofSuffix(proofLayout.suffixBytes, bootstrap);
    if (!deser.ok || !deser.proofData) {
      return { attempted: true, status: "error", reason: `Proof deserialization failed: ${deser.error}` };
    }
    const proof = deser.proofData;

    // ── Deserialize VK from bootstrap ──
    const vkHex = bootstrap.historyProofVerificationKeyHex;
    if (!vkHex || vkHex.length === 0) {
      return { attempted: false, status: "skipped", reason: "No WRAPS verification key in bootstrap." };
    }
    const vkBytes = Buffer.from(vkHex, "hex");
    if (vkBytes.length !== 1768) {
      return {
        attempted: true,
        status: "error",
        reason: `VK is ${vkBytes.length} bytes, expected 1768.`,
      };
    }
    let vp: VerifierParam;
    try {
      vp = deserializeVerifierParam(vkBytes);
    } catch (err) {
      return {
        attempted: true,
        status: "error",
        reason: `VK deserialization failed: ${err instanceof Error ? err.message : String(err)}`,
      };
    }

    // ── Step 1: State consistency checks ──

    // Ledger ID: z_0[0] must match genesis address book hash
    const ledgerIdBytes = Buffer.from(bootstrap.ledgerIdHex, "hex");
    const ledgerIdFr = readU256LE(ledgerIdBytes, 0);
    const ledgerIdMatch = proof.z_0[0] === ledgerIdFr;

    // Hints VK hash: z_i[1] must match Poseidon(hints_vk_bytes)
    const hintsVkHash = hashHintsVk(proofLayout.hintsVerificationKeyBytes);
    const hintsVkHashMatch = proof.z_i[1] === hintsVkHash;

    // Iteration guard: i > 1
    const iterationGuard = proof.i > 1n;

    // ── Step 2: Fold commitments ──
    const suffixBuf = proofLayout.suffixBytes;

    const U_cmW = decompressG1(suffixBuf, 184);
    const U_cmE = decompressG1(suffixBuf, 216);
    const u_cmW = decompressG1(suffixBuf, 256);
    const u_cmE = decompressG1(suffixBuf, 288);
    const cmT = decompressG1(suffixBuf, 576);

    // u_cmE must be the identity
    const uCmEIsZero = u_cmE.equals(G1.ZERO);

    const r = proof.ethProof.r;
    const cmWFinal = U_cmW.add(u_cmW.multiply(r));
    const cmEFinal = U_cmE.add(cmT.multiply(r));

    // ── Step 3: Nonnative encoding ──
    const cmWNonnative = g1ToNonnative(cmWFinal);
    const cmENonnative = g1ToNonnative(cmEFinal);
    const cmTNonnative = g1ToNonnative(cmT);

    // ── Step 4: Build public input vector (40 elements) ──
    const publicInput: bigint[] = [
      vp.ppHash,
      proof.i,
      proof.z_0[0],
      proof.z_0[1],
      proof.z_i[0],
      proof.z_i[1],
      ...cmWNonnative,
      ...cmENonnative,
      proof.ethProof.kzgChallenges[0],
      proof.ethProof.kzgChallenges[1],
      proof.ethProof.kzgProofs[0].eval,
      proof.ethProof.kzgProofs[1].eval,
      ...cmTNonnative,
    ];

    if (publicInput.length !== 40) {
      return {
        attempted: true,
        status: "error",
        reason: `Public input vector has ${publicInput.length} elements, expected 40.`,
      };
    }

    // ── Step 5: Groth16 verification ──
    const proofA = decompressG1(suffixBuf, 320);
    const proofB = decompressG2(suffixBuf, 352);
    const proofC = decompressG1(suffixBuf, 416);

    const groth16Valid = verifyGroth16(publicInput, proofA, proofB, proofC, vp);

    // ── Step 6: KZG verification ──
    const kzgWitness0 = decompressG1(suffixBuf, 480);
    const kzgWitness1 = decompressG1(suffixBuf, 544);

    const kzg0Valid = verifyKzg(
      cmWFinal,
      proof.ethProof.kzgChallenges[0],
      proof.ethProof.kzgProofs[0].eval,
      kzgWitness0,
      vp,
    );

    const kzg1Valid = verifyKzg(
      cmEFinal,
      proof.ethProof.kzgChallenges[1],
      proof.ethProof.kzgProofs[1].eval,
      kzgWitness1,
      vp,
    );

    // ── Aggregate result ──
    const checks: WrapsVerificationChecks = {
      ledgerIdMatch,
      hintsVkHashMatch,
      iterationGuard,
      uCmEIsZero,
      groth16Valid,
      kzg0Valid,
      kzg1Valid,
    };

    const allPassed = Object.values(checks).every(Boolean);
    const timingMs = Math.round(performance.now() - startMs);

    if (allPassed) {
      return {
        attempted: true,
        status: "verified",
        reason: `WRAPS proof verified successfully (${timingMs}ms).`,
        checks,
        timingMs,
      };
    }

    const failedChecks = Object.entries(checks)
      .filter(([, v]) => !v)
      .map(([k]) => k);
    return {
      attempted: true,
      status: "failed",
      reason: `WRAPS verification failed: ${failedChecks.join(", ")} (${timingMs}ms).`,
      checks,
      timingMs,
    };
  } catch (err) {
    const timingMs = Math.round(performance.now() - startMs);
    return {
      attempted: true,
      status: "error",
      reason: `${err instanceof Error ? err.message : String(err)} (${timingMs}ms)`,
      timingMs,
    };
  }
}
