import type {
  BootstrapPublicationSummary,
  NobleAttemptResult,
  ParsedBlockFixture,
  SnarkjsAssessment,
  VerificationReport,
} from "./types.js";

function toHex(bytes: Buffer): string {
  return bytes.toString("hex");
}

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
}): VerificationReport {
  const { parsedBlock, blockRoot, bootstrapPublication, nobleAttempt, snarkjsAssessment } = args;
  return {
    fixturePath: parsedBlock.fixturePath,
    blockNumber: parsedBlock.blockNumber,
    blockRootHex: toHex(blockRoot),
    proofLayout: parsedBlock.proofLayout,
    bootstrapFound: bootstrapPublication !== null,
    bootstrapLedgerIdHex: bootstrapPublication?.ledgerIdHex,
    bootstrapContributionCount: bootstrapPublication?.nodeContributions.length,
    nobleAttempt,
    snarkjsAssessment,
    oracleNotes: buildOracleNotes(parsedBlock),
  };
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
