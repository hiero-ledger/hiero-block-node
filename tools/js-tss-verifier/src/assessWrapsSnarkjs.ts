import type { BootstrapPublicationSummary, ProofLayout, SnarkjsAssessment, WrapsDeserializationResult } from "./types.js";
import { deserializeWrapsProofSuffix } from "./deserializeWrapsProof.js";

export function assessSnarkjsTractability(proofLayout: ProofLayout): SnarkjsAssessment {
  if (proofLayout.suffixKind !== "wraps-compressed-proof") {
    return {
      attempted: false,
      status: "skipped",
      reason: "Not a WRAPS-path proof layout.",
    };
  }

  return {
    attempted: false,
    status: "skipped",
    reason:
      "The 704-byte WRAPS suffix is a Nova IVC ProofData bundle (not a bare Groth16 proof). " +
      "snarkjs cannot verify it — a Nova IVC verifier is required. " +
      "See deserializeWrapsProof for the full structural breakdown.",
  };
}

export function attemptWrapsDeserialization(
  proofLayout: ProofLayout,
  bootstrap: BootstrapPublicationSummary | null,
): WrapsDeserializationResult | undefined {
  if (proofLayout.suffixKind !== "wraps-compressed-proof") {
    return undefined;
  }
  return deserializeWrapsProofSuffix(proofLayout.suffixBytes, bootstrap);
}
