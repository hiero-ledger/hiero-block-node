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

// --- WRAPS ProofData deserialization types (704-byte suffix) ---

export interface WrapsKzgEvalProof {
  eval: bigint;
  proof: bigint; // compressed G1 x-coordinate
}

export interface WrapsEthProof {
  groth16A: bigint; // compressed G1
  groth16B: { c0: bigint; c1: bigint }; // compressed G2 (Fp2 x-coordinate)
  groth16C: bigint; // compressed G1
  kzgProofs: [WrapsKzgEvalProof, WrapsKzgEvalProof];
  cmT: bigint; // compressed G1
  r: bigint; // Fr scalar
  kzgChallenges: [bigint, bigint]; // Fr scalars
}

export interface WrapsProofData {
  i: bigint;
  z_0: bigint[];
  z_i: bigint[];
  U_i_commitments: bigint[];
  u_i_commitments: bigint[];
  ethProof: WrapsEthProof;
}

export interface WrapsDeserializationResult {
  ok: boolean;
  proofData?: WrapsProofData;
  error?: string;
  /** Checks whether z_0[0] matches the bootstrap ledger ID */
  ledgerIdCheck?: { expected: string; actual: string; match: boolean };
}

// --- Schnorr verification types (192-byte suffix) ---

export interface ParsedSchnorrSuffix {
  bitvector: boolean[];
  proverResponse: bigint;
  verifierChallenge: bigint;
}

export interface ParsedNodeKey {
  pubkeyX: bigint;
  pubkeyY: bigint;
  pokCommitmentX: bigint;
  pokCommitmentY: bigint;
  pokChallenge: bigint;
  pokResponse: bigint;
}

export interface SchnorrVerificationResult {
  attempted: boolean;
  status: "verified" | "failed" | "skipped" | "error";
  reason: string;
  signerCount?: number;
  totalNodes?: number;
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
  wrapsDeserialization?: WrapsDeserializationResult;
  schnorrVerification?: SchnorrVerificationResult;
  oracleNotes: string[];
}
