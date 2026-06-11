import { createHash } from "node:crypto";
import { loadProtoRoot } from "./proto.js";
import { asBuffer } from "./utils.js";
import type { ParsedBlockFixture, ParsedUnparsedItem } from "./types.js";

const HASH_LENGTH = 48;
const EMPTY_TREE_HASH = sha384(Buffer.from([0x00]));

function sha384(bytes: Buffer): Buffer {
  return createHash("sha384").update(bytes).digest();
}

function hashLeaf(leafBytes: Buffer): Buffer {
  return createHash("sha384").update(Buffer.from([0x00])).update(leafBytes).digest();
}

function hashInternalNode(leftHash: Buffer, rightHash: Buffer): Buffer {
  return createHash("sha384").update(Buffer.from([0x02])).update(leftHash).update(rightHash).digest();
}

function hashInternalNodeSingleChild(childHash: Buffer): Buffer {
  return createHash("sha384").update(Buffer.from([0x01])).update(childHash).digest();
}


class NaiveStreamingTreeHasher {
  private readonly hashList: Buffer[] = [];
  private leafCount = 0;

  addLeaf(hash: Buffer): void {
    if (hash.length !== HASH_LENGTH) {
      throw new Error(`Expected ${HASH_LENGTH}-byte leaf hash`);
    }

    this.hashList.push(hash);
    for (let n = this.leafCount; (n & 1) === 1; n >>= 1) {
      const right = this.hashList.pop();
      const left = this.hashList.pop();
      if (!left || !right) {
        throw new Error("Merkle fold-up underflow");
      }
      this.hashList.push(hashInternalNode(left, right));
    }
    this.leafCount += 1;
  }

  rootHash(): Buffer {
    if (this.hashList.length === 0) {
      return EMPTY_TREE_HASH;
    }

    let root = this.hashList[this.hashList.length - 1];
    for (let i = this.hashList.length - 2; i >= 0; i -= 1) {
      root = hashInternalNode(this.hashList[i], root);
    }
    return root;
  }
}

function addItemToHasher(item: ParsedUnparsedItem, hasher: NaiveStreamingTreeHasher): void {
  hasher.addLeaf(hashLeaf(item.serializedItemBytes));
}

export async function computeBlockRoot(parsedBlock: ParsedBlockFixture): Promise<Buffer> {
  const root = await loadProtoRoot();
  const timestampType = root.lookupType("proto.Timestamp");

  const inputTreeHasher = new NaiveStreamingTreeHasher();
  const outputTreeHasher = new NaiveStreamingTreeHasher();
  const consensusHeaderHasher = new NaiveStreamingTreeHasher();
  const stateChangesHasher = new NaiveStreamingTreeHasher();
  const traceDataHasher = new NaiveStreamingTreeHasher();

  for (const item of parsedBlock.items) {
    switch (item.kind) {
      case "blockHeader":
      case "transactionResult":
      case "transactionOutput":
        addItemToHasher(item, outputTreeHasher);
        break;
      case "eventHeader":
      case "roundHeader":
        addItemToHasher(item, consensusHeaderHasher);
        break;
      case "signedTransaction":
        addItemToHasher(item, inputTreeHasher);
        break;
      case "stateChanges":
        addItemToHasher(item, stateChangesHasher);
        break;
      case "traceData":
        addItemToHasher(item, traceDataHasher);
        break;
      case "blockFooter":
      case "blockProof":
      case "recordFile":
        break;
      case "filteredSingleItem":
      case "redactedItem":
        throw new Error(`Filtered or redacted items are not supported by this example: ${item.kind}`);
    }
  }

  const previousBlockHash = asBuffer(parsedBlock.blockFooter.previousBlockRootHash);
  const rootHashOfAllPreviousBlockHashes = asBuffer(parsedBlock.blockFooter.rootHashOfAllBlockHashesTree);
  const startOfBlockStateRootHash = asBuffer(parsedBlock.blockFooter.startOfBlockStateRootHash);
  const stateRootHash =
    startOfBlockStateRootHash.length === 0 ? Buffer.alloc(HASH_LENGTH) : startOfBlockStateRootHash;

  const depth5Node1 = hashInternalNode(previousBlockHash, rootHashOfAllPreviousBlockHashes);
  const depth5Node2 = hashInternalNode(stateRootHash, consensusHeaderHasher.rootHash());
  const depth5Node3 = hashInternalNode(inputTreeHasher.rootHash(), outputTreeHasher.rootHash());
  const depth5Node4 = hashInternalNode(stateChangesHasher.rootHash(), traceDataHasher.rootHash());
  const depth4Node1 = hashInternalNode(depth5Node1, depth5Node2);
  const depth4Node2 = hashInternalNode(depth5Node3, depth5Node4);
  const depth3Node1 = hashInternalNode(depth4Node1, depth4Node2);
  const fixedRootTree = hashInternalNodeSingleChild(depth3Node1);

  const timestampBytes = Buffer.from(
    timestampType.encode(parsedBlock.blockHeader.blockTimestamp as Record<string, unknown>).finish(),
  );
  const timestampLeaf = hashLeaf(timestampBytes);
  return hashInternalNode(timestampLeaf, fixedRootTree);
}
