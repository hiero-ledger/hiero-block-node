# Historical Block Stream

> This document discusses all the original considerations and trade-offs for the Block Stream History. Before we chose
> the final option of wrapping lossless converted record files with appendments.

How do we have cleanup the network history to have one single correct source of truth for the history of the network. Today we have the record stream but it is not complete or 100% correct. The record stream is what we define as the block chain for Hedera. It starts at **Block 0** a point we call **Stream-Start** which was on “***Friday 13th***” 9/13/2019. Three days before OA(Open Access) on 9/16/2019 when the network was opened up to the general public. Each record file is accepted as a block, the running at the end of the file is used as the block hash.

When we switch to from Record Streams to Block Streams we really want to take that opportunity to fix the history so it is complete. Today the mirror node and any customers that work on historical data have to collate and maintain a large set of amendments to the data that have no trust chain. There are several options to how we can do this with trade-offs. This document lays them out so we can debate and choose best path forward.

## Block Stream History Options

### **Option 1**: Don’t fix anything

We could just have the record stream as it is today and the first new Block Stream block, previous hash is the last running hash of the record stream before we switch. Mirror nodes and others will need to be able to parse both Record Files and Blocks forever.

### Option 2: Wrap Record Stream

Take Record Files, Side Car and Signature Files, wrap them in Block Stream Blocks. All trust is existing trust, all missing data and mistakes still are there. Anyone interested in the contents of those record files need to be able to understand record files.

### Option 3a: Wrap Record Stream with inline state proof amendments

Take Record Files, Side Car and Signature Files, wrap them in Block Stream Blocks. Add into those blocks extra amendment items for missing data and corrections. Those amendments are trusted based on state proof from original sources. They might be be very ugly and complicated state proofs with 1000s of sibling hashes.

### Option 3b: Wrap Record Stream with inline TSS signed amendments✅

Take Record Files, Side Car and Signature Files, wrap them in Block Stream Blocks. Add into those blocks extra amendment items for missing data and corrections. Those amendments are signed by TSS ledger ID.

Ahead of switch over Record → Block Stream we generate all amendments. Build a merkle tree of their hashes and then include the root hash in first new Block Stream Block. We can then in the background create Block proofs for all those amendments and insert them into the converted Block Stream blocks.

### Option 4: Fork History → Reformat and correct stream and sign it all again

Define a clean protobuf for historical data, slight extensions to Block Stream and Record File V6 protobufs. Convert all history to new format with all mistakes corrected and all missing data added. Then sign the new chain of files with TSS so ledger ID can be used to trust it all. The first new Block Stream block has two historical hashes one for new history and one for old record stream running hash. There is a performance challenge if this is even possible as 81M blocks to sign.

## Pros/Cons Grid

|                                                                                                       | **Option 1** | **Option 2**                | **Option 3a**            | **Option 3b**            | **Option 4**          |
|-------------------------------------------------------------------------------------------------------|--------------|-----------------------------|--------------------------|--------------------------|-----------------------|
| Block Nodes only need to handle Block Stream files and not Record Files, Sidecars and Signature Files | **No**       | **Yes**                     | **Yes**                  | **Yes**                  | **Yes**               |
| Mirror Nodes and Customers don’t have to be able to read old Record File formats                      | **No**       | **No**                      | **No**                   | **No**                   | **Yes**               |
| Mirror Nodes and Customers have 100% correct trusted data                                             | **No**       | **No**                      | **Yes**                  | **Yes**                  | **Yes**               |
| Mirror Nodes and Customers don’t have to have own set of corrections and missing data                 | **No**       | **No**                      | **Yes**                  | **Yes**                  | **Yes**               |
| We get deduplication of record file history. Not 30 nodes copies. About 20x data size reduction       | **No**       | **Yes**                     | **Yes**                  | **Yes**                  | **Yes**               |
| Doesn’t require Building State Proofs from old messy saved states                                     | **Yes**      | **Yes**                     | **No**                   | **Yes**                  | **Yes**               |
| There is no Forked History(where has two trusted paths back)                                          | **Yes**      | **Yes**                     | **Yes**                  | **Yes**                  | **No**                |
| Ledger ID can be used to trust all history                                                            | **No**       | **No**                      | **No**                   | **No**                   | **Yes**               |
| Amendments can be done after switch over Record Stream → Block Stream                                 | **No**       | Can do Options 3 or 4 later | Yes if you pick Option 2 | Yes if you pick Option 2 | Can do any time later |
