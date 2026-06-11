/**
 * Deserializes the 1768-byte WRAPS VerifierParam (decider verification key).
 *
 * Layout (ArkWorks CanonicalSerialize compressed):
 *
 *   Offset   Size   Field
 *   0        32     pp_hash: Fr
 *   32       32     alpha_g1: G1 compressed
 *   64       64     beta_g2: G2 compressed
 *   128      64     gamma_g2: G2 compressed
 *   192      64     delta_g2: G2 compressed
 *   256      8      len(gamma_abc_g1): u64 LE (expect 41)
 *   264      1312   gamma_abc_g1[0..40]: G1 compressed × 41
 *   1576     32     kzg_vk.g: G1 compressed
 *   1608     32     kzg_vk.gamma_g: G1 compressed
 *   1640     64     kzg_vk.h: G2 compressed
 *   1704     64     kzg_vk.beta_h: G2 compressed
 *   Total:  1768
 */

import { readFr, readU64LE } from "./byteReaders.js";
import { decompressG1, decompressG2, type G1Point, type G2Point } from "./bn254Decompress.js";

export interface VerifierParam {
  ppHash: bigint;
  alphaG1: G1Point;
  betaG2: G2Point;
  gammaG2: G2Point;
  deltaG2: G2Point;
  gammaAbcG1: G1Point[]; // 41 entries: index 0 = base, 1..40 = public input coefficients
  kzgVk: {
    g: G1Point;
    gammaG: G1Point;
    h: G2Point;
    betaH: G2Point;
  };
}

const EXPECTED_VK_LENGTH = 1768;

export function deserializeVerifierParam(vkBytes: Buffer): VerifierParam {
  if (vkBytes.length !== EXPECTED_VK_LENGTH) {
    throw new Error(`Expected ${EXPECTED_VK_LENGTH}-byte VK, got ${vkBytes.length}`);
  }

  let offset = 0;

  // pp_hash: Fr (32 bytes)
  const ppHash = readFr(vkBytes, offset);
  offset += 32;

  // Groth16 VerifyingKey
  const alphaG1 = decompressG1(vkBytes, offset);
  offset += 32;

  const betaG2 = decompressG2(vkBytes, offset);
  offset += 64;

  const gammaG2 = decompressG2(vkBytes, offset);
  offset += 64;

  const deltaG2 = decompressG2(vkBytes, offset);
  offset += 64;

  // gamma_abc_g1: Vec<G1>
  const gammaAbcLen = readU64LE(vkBytes, offset);
  offset += 8;

  const gammaAbcG1: G1Point[] = [];
  for (let i = 0; i < gammaAbcLen; i++) {
    gammaAbcG1.push(decompressG1(vkBytes, offset));
    offset += 32;
  }

  // KZG VerifierKey
  const kzgG = decompressG1(vkBytes, offset);
  offset += 32;

  const kzgGammaG = decompressG1(vkBytes, offset);
  offset += 32;

  const kzgH = decompressG2(vkBytes, offset);
  offset += 64;

  const kzgBetaH = decompressG2(vkBytes, offset);
  offset += 64;

  if (offset !== EXPECTED_VK_LENGTH) {
    throw new Error(`Deserialized ${offset} bytes but expected ${EXPECTED_VK_LENGTH}`);
  }

  return {
    ppHash,
    alphaG1,
    betaG2,
    gammaG2,
    deltaG2,
    gammaAbcG1,
    kzgVk: {
      g: kzgG,
      gammaG: kzgGammaG,
      h: kzgH,
      betaH: kzgBetaH,
    },
  };
}
