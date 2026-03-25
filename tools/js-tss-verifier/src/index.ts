import path from "node:path";
import { getRepoRoot } from "./proto.js";
import { parseBlockFixture } from "./parseBlock.js";
import { extractBootstrapPublication } from "./extractBootstrap.js";
import { computeBlockRoot } from "./computeBlockRoot.js";
import { attemptNobleVerification } from "./attemptNobleVerify.js";
import { assessSnarkjsTractability, attemptWrapsDeserialization } from "./assessWrapsSnarkjs.js";
import { verifySchnorrSignature } from "./verifySchnorr.js";
import { buildVerificationReport, formatVerificationReports } from "./report.js";
import type { BootstrapPublicationSummary, VerificationReport } from "./types.js";

const repoRoot = getRepoRoot();
const TSS_FIXTURES_DIR = path.join(
  repoRoot,
  "block-node",
  "app",
  "src",
  "testFixtures",
  "resources",
  "test-blocks",
  "tss",
  "TssWraps",
);

const defaultFixturePaths = [
  path.join(TSS_FIXTURES_DIR, "block-0.blk.zstd"),
  path.join(TSS_FIXTURES_DIR, "block-1000.blk.zstd"),
];

function parseArgs(argv: string[]): { fixturePaths: string[]; json: boolean } {
  const fixturePaths: string[] = [];
  let json = false;

  for (const arg of argv) {
    if (arg === "--json") {
      json = true;
    } else {
      fixturePaths.push(arg);
    }
  }

  return {
    fixturePaths: fixturePaths.length > 0 ? fixturePaths : defaultFixturePaths,
    json,
  };
}

async function buildReports(fixturePaths: string[]): Promise<VerificationReport[]> {
  let bootstrapContext: BootstrapPublicationSummary | null = null;
  const reports: VerificationReport[] = [];

  for (const fixturePath of fixturePaths) {
    const parsedBlock = await parseBlockFixture(fixturePath);
    const blockRoot = await computeBlockRoot(parsedBlock);
    const bootstrapPublication: BootstrapPublicationSummary | null =
      (await extractBootstrapPublication(parsedBlock)) ?? bootstrapContext;
    if (bootstrapPublication !== null) {
      bootstrapContext = bootstrapPublication;
    }

    reports.push(
      buildVerificationReport({
        parsedBlock,
        blockRoot,
        bootstrapPublication,
        nobleAttempt: attemptNobleVerification(parsedBlock.proofLayout),
        snarkjsAssessment: assessSnarkjsTractability(parsedBlock.proofLayout),
        wrapsDeserialization: attemptWrapsDeserialization(parsedBlock.proofLayout, bootstrapPublication),
        schnorrVerification: verifySchnorrSignature(parsedBlock.proofLayout, bootstrapPublication),
      }),
    );
  }

  return reports;
}

async function main(): Promise<void> {
  const { fixturePaths, json } = parseArgs(process.argv.slice(2));
  const reports = await buildReports(fixturePaths);
  if (json) {
    const replacer = (_key: string, value: unknown) =>
      typeof value === "bigint" ? "0x" + value.toString(16) : value;
    console.log(JSON.stringify(reports, replacer, 2));
    return;
  }
  console.log(formatVerificationReports(reports));
}

main().catch((error) => {
  const message =
    error instanceof Error ? `${error.name}: ${error.message}\n${error.stack ?? ""}` : String(error);
  console.error(message);
  process.exitCode = 1;
});
