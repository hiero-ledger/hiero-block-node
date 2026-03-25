import { bls12_381 } from "@noble/curves/bls12-381.js";
import type { ChunkedPointCheck, NobleAttemptResult, PointCheck, ProofLayout } from "./types.js";

type PointDecoder = (bytes: Uint8Array) => unknown;

function decodeG1(bytes: Uint8Array): unknown {
  return bls12_381.G1.Point.fromHex(bytes));
}

function decodeG2(bytes: Uint8Array): unknown {
  return bls12_381.G2.Point.fromHex(Buffer.from(bytes).toString("hex"));
}

function tryWholeSlice(label: string, bytes: Buffer, decoder: PointDecoder): PointCheck {
  try {
    decoder(bytes);
    return {
      label,
      bytesLength: bytes.length,
      ok: true,
      details: "Decoded successfully as a standard noble point.",
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return {
      label,
      bytesLength: bytes.length,
      ok: false,
      details: message,
    };
  }
}

function tryChunked(label: string, bytes: Buffer, chunkSize: number, decoder: PointDecoder): ChunkedPointCheck {
  if (bytes.length === 0) {
    return {
      label,
      chunkSize,
      totalChunks: 0,
      validChunks: 0,
      details: "No bytes available for chunked parsing.",
    };
  }

  if (bytes.length % chunkSize !== 0) {
    return {
      label,
      chunkSize,
      totalChunks: 0,
      validChunks: 0,
      details: `Length ${bytes.length} is not divisible by ${chunkSize}.`,
    };
  }

  const totalChunks = bytes.length / chunkSize;
  let validChunks = 0;
  for (let i = 0; i < totalChunks; i += 1) {
    const chunk = bytes.subarray(i * chunkSize, (i + 1) * chunkSize);
    try {
      decoder(chunk);
      validChunks += 1;
    } catch {
      break;
    }
  }

  return {
    label,
    chunkSize,
    totalChunks,
    validChunks,
    details:
      validChunks === totalChunks
        ? "Every chunk decoded as a standard noble point."
        : `Decoded ${validChunks} of ${totalChunks} chunks before failure.`,
  };
}

function buildWholeSliceChecks(proofLayout: ProofLayout): PointCheck[] {
  return [
    tryWholeSlice("hintsVk-as-G1", proofLayout.hintsVerificationKeyBytes, decodeG1),
    tryWholeSlice("hintsVk-as-G2", proofLayout.hintsVerificationKeyBytes, decodeG2),
    tryWholeSlice("hintsSig-as-G1", proofLayout.hintsSignatureBytes, decodeG1),
    tryWholeSlice("hintsSig-as-G2", proofLayout.hintsSignatureBytes, decodeG2),
    tryWholeSlice("suffix-as-G1", proofLayout.suffixBytes, decodeG1),
    tryWholeSlice("suffix-as-G2", proofLayout.suffixBytes, decodeG2),
  ];
}

function buildChunkedChecks(proofLayout: ProofLayout): ChunkedPointCheck[] {
  return [
    tryChunked("hintsVk-in-48-byte-G1-compressed", proofLayout.hintsVerificationKeyBytes, 48, decodeG1),
    tryChunked("hintsVk-in-96-byte-G2-compressed", proofLayout.hintsVerificationKeyBytes, 96, decodeG2),
    tryChunked("hintsSig-in-48-byte-G1-compressed", proofLayout.hintsSignatureBytes, 48, decodeG1),
    tryChunked("hintsSig-in-96-byte-G2-compressed", proofLayout.hintsSignatureBytes, 96, decodeG2),
    tryChunked("suffix-in-48-byte-G1-compressed", proofLayout.suffixBytes, 48, decodeG1),
    tryChunked("suffix-in-96-byte-G2-compressed", proofLayout.suffixBytes, 96, decodeG2),
    tryChunked("suffix-in-192-byte-G2-uncompressed", proofLayout.suffixBytes, 192, decodeG2),
  ];
}

export function attemptNobleVerification(proofLayout: ProofLayout): NobleAttemptResult {
  const wholeSliceChecks = buildWholeSliceChecks(proofLayout);
  const chunkedChecks = buildChunkedChecks(proofLayout);

  const anyWholeSliceDecoded = wholeSliceChecks.some((check) => check.ok);
  const anyChunkSeriesFullyDecoded = chunkedChecks.some(
    (check) => check.totalChunks > 0 && check.validChunks === check.totalChunks,
  );

  if (proofLayout.hintsVerificationKeyBytes.length === 0 || proofLayout.hintsSignatureBytes.length === 0) {
    return {
      attempted: true,
      status: "decode-failed",
      reason: "Proof payload is too short to isolate the documented vk and blsSig slices.",
      wholeSliceChecks,
      chunkedChecks,
    };
  }

  if (!anyWholeSliceDecoded && !anyChunkSeriesFullyDecoded) {
    return {
      attempted: true,
      status: "unsupported-artifact-shape",
      reason:
        "The Hedera proof slices do not decode as standard noble BLS12-381 points or clean point sequences, so direct aggregate verification is not currently tractable with canonical noble inputs alone.",
      wholeSliceChecks,
      chunkedChecks,
    };
  }

  return {
    attempted: true,
    status: "unsupported-artifact-shape",
    reason:
      "Some slices resemble standard BLS12-381 points, but the full wrapped proof still does not map to a canonical single public key plus single signature shape required for direct noble verification.",
    wholeSliceChecks,
    chunkedChecks,
  };
}
