import type {
  BootstrapPublicationSummary,
  NobleAttemptResult,
  ParsedBlockFixture,
  SnarkjsAssessment,
  VerificationReport,
  WrapsDeserializationResult,
  SchnorrVerificationResult,
} from "./types.js";

function buildOracleNotes(parsedBlock: ParsedBlockFixture): string[] {
  const notes: string[] = [];

  if (parsedBlock.blockNumber === "0") {
    notes.push("Block 0 carries the LedgerIdPublicationTransactionBody bootstrap transaction.");
    notes.push("Expected proof length for block 0 is 2920 bytes (genesis-schnorr path).");
  }

  if (parsedBlock.proofLayout.kind === "unknown") {
    notes.push("Observed proof length does not match the documented 2920-byte or 3432-byte layouts.");
  } else {
    notes.push(`Observed proof layout classified as ${parsedBlock.proofLayout.kind}.`);
  }

  return notes;
}

export function buildVerificationReport(args: {
  parsedBlock: ParsedBlockFixture;
  blockRoot: Buffer;
  bootstrapPublication: BootstrapPublicationSummary | null;
  nobleAttempt: NobleAttemptResult;
  snarkjsAssessment: SnarkjsAssessment;
  wrapsDeserialization?: WrapsDeserializationResult;
  schnorrVerification?: SchnorrVerificationResult;
}): VerificationReport {
  const {
    parsedBlock,
    blockRoot,
    bootstrapPublication,
    nobleAttempt,
    snarkjsAssessment,
    wrapsDeserialization,
    schnorrVerification,
  } = args;
  return {
    fixturePath: parsedBlock.fixturePath,
    blockNumber: parsedBlock.blockNumber,
    blockRootHex: Buffer.from(blockRoot).toString("hex"),
    proofLayout: parsedBlock.proofLayout,
    bootstrapFound: bootstrapPublication !== null,
    bootstrapLedgerIdHex: bootstrapPublication?.ledgerIdHex,
    bootstrapContributionCount: bootstrapPublication?.nodeContributions.length,
    nobleAttempt,
    snarkjsAssessment,
    wrapsDeserialization,
    schnorrVerification,
    oracleNotes: buildOracleNotes(parsedBlock),
  };
}

function formatWrapsDeserialization(result: WrapsDeserializationResult | undefined): string[] {
  if (!result) return [];
  const lines: string[] = [];
  if (result.ok && result.proofData) {
    const p = result.proofData;
    lines.push(`WRAPS deserialization: SUCCESS`);
    lines.push(`  IVC step (i): ${p.i}`);
    lines.push(`  z_0: [${p.z_0.map((v) => "0x" + v.toString(16).slice(0, 12) + "...").join(", ")}]`);
    lines.push(`  z_i: [${p.z_i.map((v) => "0x" + v.toString(16).slice(0, 12) + "...").join(", ")}]`);
    lines.push(`  U_i commitments: ${p.U_i_commitments.length} G1 points`);
    lines.push(`  u_i commitments: ${p.u_i_commitments.length} G1 points`);
    lines.push(
      `  Groth16 proof: A(G1), B(G2), C(G1) — present`,
    );
    lines.push(`  KZG proofs: 2 eval+proof pairs`);
    if (result.ledgerIdCheck) {
      const c = result.ledgerIdCheck;
      lines.push(`  Ledger ID check (z_0[0] vs bootstrap): ${c.match ? "MATCH" : "MISMATCH"}`);
    }
  } else {
    lines.push(`WRAPS deserialization: FAILED — ${result.error}`);
  }
  return lines;
}

function formatSchnorrVerification(result: SchnorrVerificationResult | undefined): string[] {
  if (!result) return [];
  const lines: string[] = [];
  const statusLabel =
    result.status === "verified"
      ? "VERIFIED"
      : result.status === "failed"
        ? "FAILED"
        : result.status === "error"
          ? "ERROR"
          : "SKIPPED";
  lines.push(`Schnorr verification: ${statusLabel} — ${result.reason}`);
  return lines;
}

export function formatVerificationReports(reports: VerificationReport[]): string {
  const lines: string[] = [];

  for (const report of reports) {
    lines.push(`Fixture: ${report.fixturePath}`);
    lines.push(`Block: ${report.blockNumber}`);
    lines.push(`Block root: ${report.blockRootHex}`);
    lines.push(
      `Proof layout: ${report.proofLayout.kind} (total=${report.proofLayout.totalLength}, hintsVk=${report.proofLayout.hintsVerificationKeyBytes.length}, hintsSig=${report.proofLayout.hintsSignatureBytes.length}, suffix=${report.proofLayout.suffixBytes.length}, suffixKind=${report.proofLayout.suffixKind})`,
    );
    lines.push(
      `Bootstrap: ${report.bootstrapFound ? `found (${report.bootstrapContributionCount ?? 0} node contributions)` : "not found in this block"}`,
    );
    if (report.bootstrapLedgerIdHex) {
      lines.push(`Ledger ID: ${report.bootstrapLedgerIdHex}`);
    }

    // WRAPS deserialization (new)
    for (const line of formatWrapsDeserialization(report.wrapsDeserialization)) {
      lines.push(line);
    }

    // Schnorr verification (new)
    for (const line of formatSchnorrVerification(report.schnorrVerification)) {
      lines.push(line);
    }

    lines.push(`Noble attempt: ${report.nobleAttempt.status} - ${report.nobleAttempt.reason}`);
    for (const check of report.nobleAttempt.wholeSliceChecks) {
      lines.push(`  whole-slice ${check.label}: ${check.ok ? "ok" : "fail"} (${check.details})`);
    }
    for (const check of report.nobleAttempt.chunkedChecks) {
      lines.push(
        `  chunked ${check.label}: valid=${check.validChunks}/${check.totalChunks} chunkSize=${check.chunkSize} (${check.details})`,
      );
    }
    lines.push(`SnarkJS assessment: ${report.snarkjsAssessment.status} - ${report.snarkjsAssessment.reason}`);
    for (const note of report.oracleNotes) {
      lines.push(`Oracle note: ${note}`);
    }
    lines.push("");
  }

  return lines.join("\n").trimEnd();
}
