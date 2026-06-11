import { loadProtoRoot } from "./proto.js";
import { asBuffer, bytesToHex, longToString } from "./utils.js";
import type { BootstrapPublicationSummary, ParsedBlockFixture } from "./types.js";

export async function extractBootstrapPublication(
  parsedBlock: ParsedBlockFixture,
): Promise<BootstrapPublicationSummary | null> {
  const root = await loadProtoRoot();
  const signedTransactionType = root.lookupType("proto.SignedTransaction");
  const transactionBodyType = root.lookupType("proto.TransactionBody");
  const publicationType = root.lookupType("com.hedera.hapi.node.tss.LedgerIdPublicationTransactionBody");

  for (const item of parsedBlock.items) {
    if (item.kind !== "signedTransaction") {
      continue;
    }

    const signedTransaction = signedTransactionType.decode(item.rawValueBytes) as unknown as Record<string, unknown>;
    const bodyBytes = asBuffer(signedTransaction.bodyBytes);
    const transactionBody = transactionBodyType.decode(bodyBytes) as unknown as Record<string, unknown>;
    const publication = transactionBody.ledgerIdPublication as Record<string, unknown> | undefined;
    if (!publication) {
      continue;
    }

    const contributions = Array.isArray(publication.nodeContributions)
      ? publication.nodeContributions.map((entry) => {
          const contribution = entry as Record<string, unknown>;
          return {
            nodeId: longToString(contribution.nodeId),
            weight: longToString(contribution.weight),
            historyProofKeyHex: bytesToHex(contribution.historyProofKey),
          };
        })
      : [];

    return {
      ledgerIdHex: bytesToHex(publication.ledgerId),
      historyProofVerificationKeyHex: bytesToHex(publication.historyProofVerificationKey),
      nodeContributions: contributions,
      rawBytes: Buffer.from(publicationType.encode(publication).finish()),
    };
  }

  return null;
}
