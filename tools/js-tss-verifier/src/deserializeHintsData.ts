/**
 * Deserializes the hinTS VerificationKey (1096 bytes) and ThresholdSignature (1632 bytes).
 *
 * Both use ArkWorks uncompressed serialization: Fp elements are big-endian (48 bytes),
 * Fr elements are little-endian (32 bytes).
 *
 * VerificationKey layout:
 *   0:    n (u64 LE, 8 bytes)
 *   8:    total_weight (Fr, 32 bytes)
 *   40:   g_0 (G1, 96 bytes)
 *   136:  h_0 (G2, 192 bytes)
 *   328:  h_1 (G2, 192 bytes)
 *   520:  l_n_minus_1_of_tau_com (G1, 96 bytes)
 *   616:  w_of_tau_com (G1, 96 bytes)
 *   712:  sk_of_tau_com (G2, 192 bytes)
 *   904:  z_of_tau_com (G2, 192 bytes)
 *   Total: 1096
 *
 * ThresholdSignature layout:
 *   0:    agg_pk (G1, 96 bytes)
 *   96:   agg_weight (Fr, 32 bytes)
 *   128:  agg_sig (G2, 192 bytes)
 *   320:  b_of_tau_com (G1, 96)
 *   416:  qx_of_tau_com (G1, 96)
 *   512:  qx_of_tau_mul_tau_com (G1, 96)
 *   608:  qz_of_tau_com (G1, 96)
 *   704:  parsum_of_tau_com (G1, 96)
 *   800:  q1_of_tau_com (G1, 96)
 *   896:  q3_of_tau_com (G1, 96)
 *   992:  q2_of_tau_com (G1, 96)
 *   1088: q4_of_tau_com (G1, 96)
 *   1184: opening_proof_r (G1, 96)
 *   1280: opening_proof_r_div_ω (G1, 96)
 *   1376: parsum_of_r (Fr, 32)
 *   1408: parsum_of_r_div_ω (Fr, 32)
 *   1440: w_of_r (Fr, 32)
 *   1472: b_of_r (Fr, 32)
 *   1504: q1_of_r (Fr, 32)
 *   1536: q3_of_r (Fr, 32)
 *   1568: q2_of_r (Fr, 32)
 *   1600: q4_of_r (Fr, 32)
 *   Total: 1632
 */

import { readU64LE } from "./byteReaders.js";
import {
  readBlsFr,
  readG1Uncompressed,
  readG2Uncompressed,
  type Bls12G1Point,
  type Bls12G2Point,
} from "./bls12381Points.js";

export interface HintsVerificationKey {
  n: number;
  totalWeight: bigint;
  g0: Bls12G1Point;
  h0: Bls12G2Point;
  h1: Bls12G2Point;
  lnMinus1OfTauCom: Bls12G1Point;
  wOfTauCom: Bls12G1Point;
  skOfTauCom: Bls12G2Point;
  zOfTauCom: Bls12G2Point;
}

export interface HintsThresholdSignature {
  aggPk: Bls12G1Point;
  aggWeight: bigint;
  aggSig: Bls12G2Point;
  bOfTauCom: Bls12G1Point;
  qxOfTauCom: Bls12G1Point;
  qxOfTauMulTauCom: Bls12G1Point;
  qzOfTauCom: Bls12G1Point;
  parsumOfTauCom: Bls12G1Point;
  q1OfTauCom: Bls12G1Point;
  q3OfTauCom: Bls12G1Point;
  q2OfTauCom: Bls12G1Point;
  q4OfTauCom: Bls12G1Point;
  openingProofR: Bls12G1Point;
  openingProofRDivOmega: Bls12G1Point;
  parsumOfR: bigint;
  parsumOfRDivOmega: bigint;
  wOfR: bigint;
  bOfR: bigint;
  q1OfR: bigint;
  q3OfR: bigint;
  q2OfR: bigint;
  q4OfR: bigint;
}

const EXPECTED_VK_LENGTH = 1096;
const EXPECTED_SIG_LENGTH = 1632;

export function deserializeHintsVk(buf: Buffer): HintsVerificationKey {
  if (buf.length !== EXPECTED_VK_LENGTH) {
    throw new Error(`Expected ${EXPECTED_VK_LENGTH}-byte hints VK, got ${buf.length}`);
  }

  return {
    n: readU64LE(buf, 0),
    totalWeight: readBlsFr(buf, 8),
    g0: readG1Uncompressed(buf, 40),
    h0: readG2Uncompressed(buf, 136),
    h1: readG2Uncompressed(buf, 328),
    lnMinus1OfTauCom: readG1Uncompressed(buf, 520),
    wOfTauCom: readG1Uncompressed(buf, 616),
    skOfTauCom: readG2Uncompressed(buf, 712),
    zOfTauCom: readG2Uncompressed(buf, 904),
  };
}

export function deserializeHintsSignature(buf: Buffer): HintsThresholdSignature {
  if (buf.length !== EXPECTED_SIG_LENGTH) {
    throw new Error(`Expected ${EXPECTED_SIG_LENGTH}-byte hints signature, got ${buf.length}`);
  }

  return {
    aggPk: readG1Uncompressed(buf, 0),
    aggWeight: readBlsFr(buf, 96),
    aggSig: readG2Uncompressed(buf, 128),
    bOfTauCom: readG1Uncompressed(buf, 320),
    qxOfTauCom: readG1Uncompressed(buf, 416),
    qxOfTauMulTauCom: readG1Uncompressed(buf, 512),
    qzOfTauCom: readG1Uncompressed(buf, 608),
    parsumOfTauCom: readG1Uncompressed(buf, 704),
    q1OfTauCom: readG1Uncompressed(buf, 800),
    q3OfTauCom: readG1Uncompressed(buf, 896),
    q2OfTauCom: readG1Uncompressed(buf, 992),
    q4OfTauCom: readG1Uncompressed(buf, 1088),
    openingProofR: readG1Uncompressed(buf, 1184),
    openingProofRDivOmega: readG1Uncompressed(buf, 1280),
    parsumOfR: readBlsFr(buf, 1376),
    parsumOfRDivOmega: readBlsFr(buf, 1408),
    wOfR: readBlsFr(buf, 1440),
    bOfR: readBlsFr(buf, 1472),
    q1OfR: readBlsFr(buf, 1504),
    q3OfR: readBlsFr(buf, 1536),
    q2OfR: readBlsFr(buf, 1568),
    q4OfR: readBlsFr(buf, 1600),
  };
}
