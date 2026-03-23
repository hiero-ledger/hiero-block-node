export type BlockItemKind =
  | "blockHeader"
  | "eventHeader"
  | "roundHeader"
  | "signedTransaction"
  | "transactionResult"
  | "transactionOutput"
  | "stateChanges"
  | "filteredSingleItem"
  | "blockProof"
  | "recordFile"
  | "traceData"
  | "blockFooter"
  | "redactedItem";

export type ProofLayoutKind = "genesis-schnorr" | "wraps" | "unknown";
export type ProofSuffixKind = "aggregate-schnorr" | "wraps-compressed-proof" | "unknown";

export interface ParsedUnparsedItem {
  kind: BlockItemKind;
  serializedItemBytes: Buffer;
  rawValueBytes: Buffer;
}

export interface ProofLayout {
  kind: ProofLayoutKind;
  suffixKind: ProofSuffixKind;
  totalLength: number;
  hintsVerificationKeyBytes: Buffer;
  hintsSignatureBytes: Buffer;
  suffixBytes: Buffer;
}

export interface NodeContributionSummary {
  nodeId: string;
  weight: string;
  historyProofKeyHex: string;
}

export interface BootstrapPublicationSummary {
  ledgerIdHex: string;
  historyProofVerificationKeyHex: string;
  nodeContributions: NodeContributionSummary[];
  rawBytes: Buffer;
}

export interface ParsedBlockFixture {
  fixturePath: string;
  blockNumber: string;
  items: ParsedUnparsedItem[];
  blockHeader: Record<string, unknown>;
  blockFooter: Record<string, unknown>;
  blockProof: Record<string, unknown>;
  blockSignature: Buffer;
  proofLayout: ProofLayout;
}

export interface PointCheck {
  label: string;
  bytesLength: number;
  ok: boolean;
  details: string;
}

export interface ChunkedPointCheck {
  label: string;
  chunkSize: number;
  totalChunks: number;
  validChunks: number;
  details: string;
}

export interface NobleAttemptResult {
  attempted: true;
  status: "verified" | "unsupported-artifact-shape" | "decode-failed";
  reason: string;
  wholeSliceChecks: PointCheck[];
  chunkedChecks: ChunkedPointCheck[];
}

export interface SnarkjsAssessment {
  attempted: false;
  status: "skipped";
  reason: string;
}

export interface VerificationReport {
  fixturePath: string;
  blockNumber: string;
  blockRootHex: string;
  proofLayout: ProofLayout;
  bootstrapFound: boolean;
  bootstrapLedgerIdHex?: string;
  bootstrapContributionCount?: number;
  nobleAttempt: NobleAttemptResult;
  snarkjsAssessment: SnarkjsAssessment;
  oracleNotes: string[];
}
