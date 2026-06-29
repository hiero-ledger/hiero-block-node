/**
 * hinTS threshold aggregate signature verification on BLS12-381.
 *
 * Implements the algorithm from hedera-cryptography hints.rs:777-871:
 *   0. Threshold weight check
 *   1. BLS signature pairing check
 *   2. Fiat-Shamir challenge
 *   3. Merged KZG opening proof at r
 *   4. ParSum KZG opening proof at r/ω
 *   5. Polynomial identity: B·SK
 *   6. Four field-level polynomial identity checks
 *   7. Degree check
 */

import { bls12_381 } from "@noble/curves/bls12-381.js";
import { deserializeHintsVk, deserializeHintsSignature } from "./deserializeHintsData.js";
import { computeFiatShamirChallenge, nthRootOfUnity } from "./hintsFieldHash.js";
import type { Bls12G1Point } from "./bls12381Points.js";
import type {
  ProofLayout,
  BootstrapPublicationSummary,
  HintsVerificationResult,
  HintsVerificationChecks,
} from "./types.js";

const { Fp12, Fr } = bls12_381.fields;
const G1 = bls12_381.G1.Point;

const HASH_TO_G2_DST = "BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_POP_";

// Threshold fraction: Hedera uses 1/3 (numerator=1, denominator=3)
const THRESHOLD_NUMERATOR = 1n;
const THRESHOLD_DENOMINATOR = 3n;

export function verifyHintsSignature(
  proofLayout: ProofLayout,
  bootstrap: BootstrapPublicationSummary | null,
  blockRoot: Buffer,
): HintsVerificationResult {
  // hinTS verification applies to any proof layout that has hints VK + hints signature
  if (proofLayout.hintsVerificationKeyBytes.length === 0 ||
      proofLayout.hintsSignatureBytes.length === 0) {
    return { attempted: false, status: "skipped", reason: "No hints VK or signature present." };
  }

  if (proofLayout.hintsVerificationKeyBytes.length !== 1096) {
    return {
      attempted: true,
      status: "error",
      reason: `Hints VK is ${proofLayout.hintsVerificationKeyBytes.length} bytes, expected 1096.`,
    };
  }

  if (proofLayout.hintsSignatureBytes.length !== 1632) {
    return {
      attempted: true,
      status: "error",
      reason: `Hints signature is ${proofLayout.hintsSignatureBytes.length} bytes, expected 1632.`,
    };
  }

  const startMs = performance.now();

  try {
    // ── Deserialize ──
    const vk = deserializeHintsVk(proofLayout.hintsVerificationKeyBytes);
    const sig = deserializeHintsSignature(proofLayout.hintsSignatureBytes);

    // ── Step 0: Threshold weight check ──
    // denominator × agg_weight > numerator × total_weight
    const thresholdMet =
      THRESHOLD_DENOMINATOR * sig.aggWeight > THRESHOLD_NUMERATOR * vk.totalWeight;

    // ── Step 1: BLS signature check ──
    // e(agg_pk, H(msg)) == e(g_0, agg_sig)
    const hMsg = bls12_381.G2.hashToCurve(blockRoot, { DST: HASH_TO_G2_DST });
    const blsLhs = bls12_381.pairing(sig.aggPk, hMsg);
    const blsRhs = bls12_381.pairing(vk.g0, sig.aggSig);
    const blsSignatureValid = Fp12.eql(blsLhs, blsRhs);

    if (!blsSignatureValid) {
      const timingMs = Math.round(performance.now() - startMs);
      const checks: HintsVerificationChecks = {
        thresholdMet, blsSignatureValid,
        mergedKzgValid: false, parsumKzgValid: false,
        bSkIdentityValid: false, parsumAccumulationValid: false,
        parsumConstraintValid: false, bitmapWellFormednessValid: false,
        bitmapConstraintValid: false, degreeCheckValid: false,
      };
      return {
        attempted: true, status: "failed", timingMs, checks,
        reason: `hinTS verification failed: blsSignatureValid (${timingMs}ms).`,
      };
    }

    // ── Step 2: Fiat-Shamir challenge ──
    const r = computeFiatShamirChallenge(vk, sig);
    const omega = nthRootOfUnity(vk.n);

    // ── Step 3: Merged KZG opening proof at r ──
    // Adjust W(τ): W'(τ) = W(τ) - agg_weight × L_{n-1}(τ)
    const wAdjusted = vk.wOfTauCom.add(vk.lnMinus1OfTauCom.multiply(Fr.neg(sig.aggWeight)));

    // Build 7 arguments: [f(τ) - g₀·f(r)] for each polynomial
    const g0 = vk.g0;
    const mkArg = (com: Bls12G1Point, evalAtR: bigint) =>
      com.add(g0.multiply(evalAtR).negate());

    const arg0 = mkArg(sig.parsumOfTauCom, sig.parsumOfR);
    const arg1 = mkArg(wAdjusted, sig.wOfR);
    const arg2 = mkArg(sig.bOfTauCom, sig.bOfR);
    const arg3 = mkArg(sig.q1OfTauCom, sig.q1OfR);
    const arg4 = mkArg(sig.q3OfTauCom, sig.q3OfR);
    const arg5 = mkArg(sig.q2OfTauCom, sig.q2OfR);
    const arg6 = mkArg(sig.q4OfTauCom, sig.q4OfR);

    // Merge with powers of r: arg0 + r·arg1 + r²·arg2 + ... + r⁶·arg6
    let rPow = r;
    let merged = arg0;
    for (const arg of [arg1, arg2, arg3, arg4, arg5, arg6]) {
      merged = merged.add(arg.multiply(rPow));
      rPow = Fr.mul(rPow, r);
    }

    // Pairing check: e(merged, h₀) == e(opening_proof_r, h₁ - h₀·r)
    const kzgRhsG2 = vk.h1.add(vk.h0.multiply(r).negate());
    const mergedKzgResult = bls12_381.pairingBatch([
      { g1: merged, g2: vk.h0 },
      { g1: sig.openingProofR.negate(), g2: kzgRhsG2 },
    ]);
    const mergedKzgValid = Fp12.eql(mergedKzgResult, Fp12.ONE);

    // ── Step 4: ParSum KZG opening at r/ω ──
    const rDivOmega = Fr.div(r, omega);
    const parsumArg = sig.parsumOfTauCom.add(g0.multiply(sig.parsumOfRDivOmega).negate());
    const parsumRhsG2 = vk.h1.add(vk.h0.multiply(rDivOmega).negate());
    const parsumKzgResult = bls12_381.pairingBatch([
      { g1: parsumArg, g2: vk.h0 },
      { g1: sig.openingProofRDivOmega.negate(), g2: parsumRhsG2 },
    ]);
    const parsumKzgValid = Fp12.eql(parsumKzgResult, Fp12.ONE);

    // ── Step 5: Polynomial identity B·SK ──
    // e(B(τ), SK(τ)) == e(Qz(τ), Z(τ)) + e(Qx(τ), h₁) + e(agg_pk, h₀)
    // Rearranged: e(B(τ), SK(τ)) · e(-Qz(τ), Z(τ)) · e(-Qx(τ), h₁) · e(-agg_pk, h₀) == 1
    const bSkResult = bls12_381.pairingBatch([
      { g1: sig.bOfTauCom, g2: vk.skOfTauCom },
      { g1: sig.qzOfTauCom.negate(), g2: vk.zOfTauCom },
      { g1: sig.qxOfTauCom.negate(), g2: vk.h1 },
      { g1: sig.aggPk.negate(), g2: vk.h0 },
    ]);
    const bSkIdentityValid = Fp12.eql(bSkResult, Fp12.ONE);

    // ── Step 6: Field-level polynomial identity checks ──
    const n = BigInt(vk.n);
    const vanishingOfR = Fr.sub(Fr.pow(r, n), 1n);

    // L_{n-1}(r) = (ω^(n-1) / n) × (vanishingOfR / (r - ω^(n-1)))
    const omegaPowNMinus1 = Fr.pow(omega, n - 1n);
    const lnMinus1OfR = Fr.mul(
      Fr.div(omegaPowNMinus1, Fr.create(n)),
      Fr.div(vanishingOfR, Fr.sub(r, omegaPowNMinus1)),
    );

    // Check 1: parsum(r) - parsum(r/ω) - w(r)·b(r) == q1(r) × vanishing(r)
    const check1Lhs = Fr.sub(Fr.sub(sig.parsumOfR, sig.parsumOfRDivOmega), Fr.mul(sig.wOfR, sig.bOfR));
    const check1Rhs = Fr.mul(sig.q1OfR, vanishingOfR);
    const parsumAccumulationValid = Fr.eql(check1Lhs, check1Rhs);

    // Check 2: L_{n-1}(r) × parsum(r) == vanishing(r) × q3(r)
    const check2Lhs = Fr.mul(lnMinus1OfR, sig.parsumOfR);
    const check2Rhs = Fr.mul(vanishingOfR, sig.q3OfR);
    const parsumConstraintValid = Fr.eql(check2Lhs, check2Rhs);

    // Check 3: b(r)² - b(r) == q2(r) × vanishing(r)
    const check3Lhs = Fr.sub(Fr.mul(sig.bOfR, sig.bOfR), sig.bOfR);
    const check3Rhs = Fr.mul(sig.q2OfR, vanishingOfR);
    const bitmapWellFormednessValid = Fr.eql(check3Lhs, check3Rhs);

    // Check 4: L_{n-1}(r) × (b(r) - 1) == vanishing(r) × q4(r)
    const check4Lhs = Fr.mul(lnMinus1OfR, Fr.sub(sig.bOfR, 1n));
    const check4Rhs = Fr.mul(vanishingOfR, sig.q4OfR);
    const bitmapConstraintValid = Fr.eql(check4Lhs, check4Rhs);

    // ── Step 7: Degree check ──
    // e(Qx(τ), h₁) == e(Qx(τ)·τ, h₀)
    // Rearranged: e(Qx(τ), h₁) · e(-Qx(τ)·τ, h₀) == 1
    const degreeResult = bls12_381.pairingBatch([
      { g1: sig.qxOfTauCom, g2: vk.h1 },
      { g1: sig.qxOfTauMulTauCom.negate(), g2: vk.h0 },
    ]);
    const degreeCheckValid = Fp12.eql(degreeResult, Fp12.ONE);

    // ── Aggregate ──
    const checks: HintsVerificationChecks = {
      thresholdMet,
      blsSignatureValid,
      mergedKzgValid,
      parsumKzgValid,
      bSkIdentityValid,
      parsumAccumulationValid,
      parsumConstraintValid,
      bitmapWellFormednessValid,
      bitmapConstraintValid,
      degreeCheckValid,
    };

    const allPassed = Object.values(checks).every(Boolean);
    const timingMs = Math.round(performance.now() - startMs);

    if (allPassed) {
      return {
        attempted: true,
        status: "verified",
        reason: `hinTS signature verified successfully (${timingMs}ms).`,
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
      reason: `hinTS verification failed: ${failedChecks.join(", ")} (${timingMs}ms).`,
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
