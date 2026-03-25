import path from "node:path";
import { readFileSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { decompress as zstdDecompress } from "fzstd";
import { loadProtoRoot } from "./proto.js";
import { asBuffer, longToString } from "./utils.js";
import type { BlockItemKind, ParsedBlockFixture, ParsedUnparsedItem, ProofLayout, ProofSuffixKind } from "./types.js";

const BLOCK_UNPARSED_ITEMS_FIELD_NUMBER = 1;

const ITEM_KIND_BY_FIELD_NUMBER: Record<number, BlockItemKind> = {
  1: "blockHeader",
  2: "eventHeader",
  3: "roundHeader",
  4: "signedTransaction",
  5: "transactionResult",
  6: "transactionOutput",
  7: "stateChanges",
  8: "filteredSingleItem",
  9: "blockProof",
  10: "recordFile",
  11: "traceData",
  12: "blockFooter",
  19: "redactedItem",
};


function readVarint(bytes: Buffer, startOffset: number): { value: number; nextOffset: number } {
  let offset = startOffset;
  let shift = 0;
  let value = 0;

  while (offset < bytes.length) {
    const current = bytes[offset];
    value |= (current & 0x7f) << shift;
    offset += 1;
    if ((current & 0x80) === 0) {
      return { value, nextOffset: offset };
    }
    shift += 7;
    if (shift > 35) {
      throw new Error("Varint too large for this shallow parser");
    }
  }

  throw new Error("Unexpected end of input while reading varint");
}

function readLengthDelimited(
  bytes: Buffer,
  startOffset: number,
): { valueBytes: Buffer; valueStart: number; nextOffset: number } {
  const { value: length, nextOffset } = readVarint(bytes, startOffset);
  const valueStart = nextOffset;
  const valueEnd = valueStart + length;
  if (valueEnd > bytes.length) {
    throw new Error("Length-delimited field exceeds available bytes");
  }
  return {
    valueBytes: bytes.subarray(valueStart, valueEnd),
    valueStart,
    nextOffset: valueEnd,
  };
}

function scanBlockItem(itemBytes: Buffer): ParsedUnparsedItem {
  let offset = 0;
  let parsedItem: ParsedUnparsedItem | null = null;

  while (offset < itemBytes.length) {
    const { value: tag, nextOffset: afterTag } = readVarint(itemBytes, offset);
    const fieldNumber = tag >>> 3;
    const wireType = tag & 0x07;
    if (wireType !== 2) {
      throw new Error(`Unsupported BlockItemUnparsed wire type ${wireType} for field ${fieldNumber}`);
    }

    const { valueBytes, nextOffset } = readLengthDelimited(itemBytes, afterTag);
    const kind = ITEM_KIND_BY_FIELD_NUMBER[fieldNumber];
    if (kind) {
      if (parsedItem !== null) {
        throw new Error("BlockItemUnparsed had multiple populated oneof fields");
      }
      parsedItem = {
        kind,
        serializedItemBytes: itemBytes,
        rawValueBytes: valueBytes,
      };
    }
    offset = nextOffset;
  }

  if (parsedItem === null) {
    throw new Error("Unable to determine BlockItemUnparsed kind from wire bytes");
  }

  return parsedItem;
}

function scanBlockItems(blockBytes: Buffer): ParsedUnparsedItem[] {
  const items: ParsedUnparsedItem[] = [];
  let offset = 0;

  while (offset < blockBytes.length) {
    const { value: tag, nextOffset: afterTag } = readVarint(blockBytes, offset);
    const fieldNumber = tag >>> 3;
    const wireType = tag & 0x07;
    if (wireType !== 2) {
      throw new Error(`Unsupported BlockUnparsed wire type ${wireType} for field ${fieldNumber}`);
    }

    const { valueBytes, nextOffset } = readLengthDelimited(blockBytes, afterTag);
    if (fieldNumber === BLOCK_UNPARSED_ITEMS_FIELD_NUMBER) {
      items.push(scanBlockItem(valueBytes));
    }
    offset = nextOffset;
  }

  return items;
}

// Packed blockSignature layout (confirmed by Rohit, matches Java constants in BlockVerificationUtils):
//   [0..1096)    hints_verification_key  — hinTS VK, opaque; not a standard G1/G2 point
//   [1096..2728) hints_signature         — hinTS sig over the committee (1632 bytes)
//   [2728..)     suffix — one of:
//                  192 bytes: aggregate_schnorr_signature (genesis/pre-settled path)
//                  704 bytes: wraps_proof, compressed Groth16 on BN254 (settled path)
//
// Total known lengths:
//   2920 = 1096 + 1632 + 192  (genesis-schnorr)
//   3432 = 1096 + 1632 + 704  (wraps)
const HINTS_VK_LENGTH = 1096;
const HINTS_SIG_LENGTH = 1632;
const FIXED_PREFIX_LENGTH = HINTS_VK_LENGTH + HINTS_SIG_LENGTH;
const GENESIS_SCHNORR_TOTAL = 2920;
const WRAPS_TOTAL = 3432;

function classifyProofLayout(blockSignature: Buffer): ProofLayout {
  const totalLength = blockSignature.length;
  const hasKnownPrefix = totalLength >= FIXED_PREFIX_LENGTH;
  const hintsVerificationKeyBytes = hasKnownPrefix ? blockSignature.subarray(0, HINTS_VK_LENGTH) : Buffer.alloc(0);
  const hintsSignatureBytes = hasKnownPrefix ? blockSignature.subarray(HINTS_VK_LENGTH, FIXED_PREFIX_LENGTH) : Buffer.alloc(0);
  const suffixBytes = hasKnownPrefix ? blockSignature.subarray(FIXED_PREFIX_LENGTH) : blockSignature;

  let kind: ProofLayout["kind"] = "unknown";
  let suffixKind: ProofSuffixKind = "unknown";
  if (totalLength === GENESIS_SCHNORR_TOTAL) {
    kind = "genesis-schnorr";
    suffixKind = "aggregate-schnorr";
  } else if (totalLength === WRAPS_TOTAL) {
    kind = "wraps";
    suffixKind = "wraps-compressed-proof";
  } else if (suffixBytes.length === 192) {
    suffixKind = "aggregate-schnorr";
  } else if (suffixBytes.length === 704) {
    suffixKind = "wraps-compressed-proof";
  }

  return {
    kind,
    suffixKind,
    totalLength,
    hintsVerificationKeyBytes,
    hintsSignatureBytes,
    suffixBytes,
  };
}

async function decodeMessage(typeName: string, bytes: Buffer): Promise<Record<string, unknown>> {
  const root = await loadProtoRoot();
  const messageType = root.lookupType(typeName);
  return messageType.decode(bytes) as unknown as Record<string, unknown>;
}

async function parseItems(items: ParsedUnparsedItem[]): Promise<{
  items: ParsedUnparsedItem[];
  blockHeader: Record<string, unknown>;
  blockFooter: Record<string, unknown>;
  blockProof: Record<string, unknown>;
  blockSignature: Buffer;
}> {
  let blockHeader: Record<string, unknown> | undefined;
  let blockFooter: Record<string, unknown> | undefined;
  let blockProof: Record<string, unknown> | undefined;
  let blockSignature: Buffer | undefined;

  for (const item of items) {
    if (item.kind === "blockHeader") {
      blockHeader = await decodeMessage("com.hedera.hapi.block.stream.output.BlockHeader", item.rawValueBytes);
    } else if (item.kind === "blockFooter") {
      blockFooter = await decodeMessage("com.hedera.hapi.block.stream.output.BlockFooter", item.rawValueBytes);
    } else if (item.kind === "blockProof") {
      const decodedProof = await decodeMessage("com.hedera.hapi.block.stream.BlockProof", item.rawValueBytes);
      if (decodedProof.signedBlockProof) {
        const signedBlockProof = decodedProof.signedBlockProof as Record<string, unknown>;
        blockSignature = asBuffer(signedBlockProof.blockSignature);
        blockProof = decodedProof;
      }
    }
  }

  if (!blockHeader) {
    throw new Error("Block header missing from fixture");
  }
  if (!blockFooter) {
    throw new Error("Block footer missing from fixture");
  }
  if (!blockProof || !blockSignature) {
    throw new Error("Signed block proof missing from fixture");
  }

  return { items, blockHeader, blockFooter, blockProof, blockSignature };
}

function decompressBlock(filePath: string, fileBytes: Buffer): Buffer {
  if (filePath.endsWith(".zstd") || filePath.endsWith(".zst")) {
    return Buffer.from(zstdDecompress(fileBytes));
  }
  return gunzipSync(fileBytes);
}

export async function parseBlockFixture(fixturePath: string): Promise<ParsedBlockFixture> {
  const absoluteFixturePath = path.resolve(fixturePath);
  const compressedBytes = readFileSync(absoluteFixturePath);
  const blockBytes = decompressBlock(absoluteFixturePath, compressedBytes);

  const items = scanBlockItems(blockBytes);
  const { blockHeader, blockFooter, blockProof, blockSignature } = await parseItems(items);

  return {
    fixturePath: absoluteFixturePath,
    blockNumber: longToString(blockHeader.number),
    items,
    blockHeader,
    blockFooter,
    blockProof,
    blockSignature,
    proofLayout: classifyProofLayout(blockSignature),
  };
}
