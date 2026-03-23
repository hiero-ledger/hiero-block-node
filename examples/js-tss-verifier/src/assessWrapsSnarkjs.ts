import type { ProofLayout, SnarkjsAssessment } from "./types.js";

const STANDARD_GROTH16_COMPRESSED_LENGTHS = new Set([128, 192]);
const STANDARD_GROTH16_UNCOMPRESSED_LENGTHS = new Set([256, 384]);

export function assessSnarkjsTractability(proofLayout: ProofLayout): SnarkjsAssessment {
  if (proofLayout.kind !== "wraps") {
    return {
      attempted: false,
      status: "skipped",
      reason: "Not a WRAPS-path proof layout.",
    };
  }

  if (proofLayout.suffixBytes.length === 0) {
    return {
      attempted: false,
      status: "skipped",
      reason: "WRAPS suffix is missing.",
    };
  }

  if (
    !STANDARD_GROTH16_COMPRESSED_LENGTHS.has(proofLayout.suffixBytes.length) &&
    !STANDARD_GROTH16_UNCOMPRESSED_LENGTHS.has(proofLayout.suffixBytes.length)
  ) {
    return {
      attempted: false,
      status: "skipped",
      reason:
        "WRAPS suffix length does not match a standard compressed or uncompressed Groth16 proof shape, and no canonical ArkWorks mapping is available.",
    };
  }

  return {
    attempted: false,
    status: "skipped",
    reason:
      "Canonical WRAPS proof, verification key, and ordered public inputs are not available in this repo, so a trustworthy snarkjs check cannot be attempted yet.",
  };
}
