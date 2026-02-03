# Address Book Update Rules and Implementation Guide

> [!NOTE]
> This is AI generated from Mirror Node repository code and tests. It was helpful for AI to read when impementing address book update code. It has not been fully fact checked.

This document summarizes how Hedera node address books are managed in the Mirror Node today and distills those rules into a concrete, actionable spec for implementing an in-memory AddressBookRegistry.updateAddressBook(List<TransactionBody>) in another project.

It draws from the Mirror Node importer code and tests:
- Address book ingestion: hedera-mirror-importer AddressBookServiceImpl
- Migration and parsing tests: AddressBookServiceImplTest, MissingAddressBooksMigrationTest
- Node entity transactions: NodeCreateTransactionHandler, NodeUpdateTransactionHandler, NodeDeleteTransactionHandler

The key observations and rules below are grounded in that code and its tests.

## 1) Sources of truth for address book data

There are two historical files for the address book on Hedera:
- File 0.0.102 (current/primary)
- File 0.0.101 (legacy/service endpoints)

Mirror Node persists every complete address book parsed from either file but treats 0.0.102 as the authoritative “current” address book for serving clients. Updates to 0.0.101 do not change what getCurrent() returns; they’re stored as historical data.

Address books are not (today) recomputed from NodeCreate/NodeUpdate/NodeDelete transaction types inside Mirror Node; those transactions are recorded in the nodes table for administrative data. However, for a standalone registry intended to track the effective address book as the network evolves, you likely want to incorporate those transactions to incrementally update the in-memory book between 0.0.102 file refreshes. This document includes a recommended approach.

## 2) Transactions that can affect the effective address book

A) File transactions on 0.0.101 and 0.0.102
- Types: FileCreate, FileUpdate, FileAppend
- FileUpdate/FileCreate begin a new content stream; FileAppend continues it.
- There is no explicit “final append” flag. The Mirror Node simply attempts to parse the concatenated bytes into a NodeAddressBook protobuf, and if successful, considers that a complete address book update.
- Multiple FileAppend transactions may occur after a FileUpdate; you must accumulate bytes across them.
- Empty contents should be ignored.

B) Node lifecycle transactions (recommended for a registry)
- Types: NodeCreate, NodeUpdate, NodeDelete
- In the Mirror Node, these do not currently modify the persisted address_book tables. For a real-time registry, it is useful to apply them to the in-memory book:
- NodeCreate: add the new node to the current address book
- NodeUpdate: modify fields for an existing node
- NodeDelete: remove the node from the current book

## 3) Parsing and mapping rules (from 0.0.101/0.0.102 contents)

When a complete file payload is parsed as a NodeAddressBook:
- Each NodeAddress becomes an AddressBookEntry for its node id, with deduplication per node id.
- Node identity
- Preferred: nodeId (if present in the NodeAddress) is used as the key.
- Legacy: If nodeId is 0 and the nodeAccountId is a small number (< 20) and not equal to 3, infer nodeId as (nodeAccountId.accountNum - 3). This recovers node ids from early mainnet where nodeId was 0 in the address book.
- nodeAccountId is taken from NodeAddress.nodeAccountId when present; otherwise it is parsed from the memo (a UTF-8 string like "0.0.3").
- Service endpoints
- Deprecated fields: NodeAddress.ipAddress (string) and portno are still supported; if ipAddress is non-blank, include it as an endpoint.
- Preferred: NodeAddress.serviceEndpoint list of ServiceEndpoint entries. Each must have ipAddressV4 with exactly 4 bytes; otherwise the entire parse is rejected. The ip is the dotted-quad string from those 4 bytes; port is from ServiceEndpoint.port; domainName may be set or empty.
- All endpoints are deduplicated per node (by ip + port + domain) across multiple NodeAddress entries for the same node id.
- Other fields
- publicKey is taken from NodeAddress.RSAPubKey.
- stake is taken from NodeAddress.stake (but Mirror Node may later override effective consensus weighting from NodeStake records; this is outside the scope of the registry).
- Failure behavior
- If parsing the NodeAddressBook fails (e.g., invalid ipAddressV4), the address book update is discarded.

Timestamps (Mirror Node persistence)
- The persisted AddressBook startConsensusTimestamp is the transaction’s consensus timestamp + 1.
- When a new book is successfully saved, the previous book’s endConsensusTimestamp is set to the new file’s consensus timestamp if not already set.

A standalone registry can adopt the same convention for internal versioning (e.g., storing a logical start time of “txTime+1” for each computed version). If you don’t persist timestamps, just maintain order.

## 4) Applying NodeCreate/NodeUpdate/NodeDelete (recommended registry behavior)

Although Mirror Node doesn’t use these transactions to update its address books, a registry tracking the “effective” network state can:

- Identity matching for node changes
  - Prefer matching by nodeAccountId when present.
  - Else match by nodeId.
  - For very old nodes with nodeId=0, you may infer nodeId as (accountNum - 3) if accountNum < 20 and != 3.
- NodeCreate
  - If the transaction includes a NodeAddress or equivalent fields (public key, memo, nodeAccountId, service endpoints, etc.), construct a NodeAddress entry and add it to the current book.
  - If a node with the same identity already exists, replace it.
- NodeUpdate
  - If the transaction provides a full NodeAddress, replace the existing one.
  - If it provides partial fields, update only those fields on the existing node.
  - Endpoints:
    - Merge new endpoints with existing endpoints, deduplicating by ip+port+domain.
    - If the intent is to replace all endpoints (protocol-dependent), clear first then set.
- NodeDelete
  - Remove the node from the address book.

Every time you apply a NodeCreate/Update/Delete, treat it as producing a new version of the book and append it to the registry history.

## 5) File 0.0.101 vs 0.0.102 behavior nuances

- Both files may receive updates.
- A complete parse from either file should produce a new address book version.
- When a consumer asks for the “current” book, prefer the latest successful 0.0.102 version.
- You may still keep and expose the last-parsed 0.0.101 variant for diagnostics or if a consumer specifically asks.

## 6) Edge cases to handle

- Partial appends
  - Accumulate content across FileUpdate/FileCreate and subsequent FileAppend transactions until a valid NodeAddressBook parses.
  - Don’t clear the accumulator unless a new FileUpdate/FileCreate starts an entirely new content stream for that file id.
  - If the current block doesn’t complete a parse, carry the buffer forward to the next block.
- Empty contents
  - Ignore empty contents.
- Duplicate NodeAddress entries (same node) within a single book
  - Deduplicate by node id; union endpoints as a set.
- Invalid endpoint IP
  - For file-based updates, invalid ipAddressV4 (not exactly 4 bytes) should cause the parse to be rejected and no version to be created (as Mirror Node does).
  - For NodeUpdate transactions in a registry, consider either rejecting just the bad endpoint or the whole transaction depending on your correctness vs. resilience goals.
- Deprecated memo-based identity
  - If nodeAccountId is missing, memo may contain the “0.0.x” string and should be parsed to infer node identity.

## 7) Implementation outline: updateAddressBook(List<TransactionBody>)

Contract
- Input: a list of Hedera TransactionBody (PBJ) that are already filtered to those relevant for address book updates (FileCreate/Update/Append on 0.0.101/102 and NodeCreate/Update/Delete) or that you will filter.
- Output: The registry updates its internal state. For every material change (completed file parse or node lifecycle update), append a new NodeAddressBook to the registry’s history, and update getCurrentAddressBook() to return the most recent authoritative 0.0.102 book.

Internal state suggestions
- Maintain a map<FileId, ByteArrayOutputStream> for partial payloads per file id (0.0.101 and 0.0.102), not just a single buffer.
- Maintain a List<NodeAddressBook> history, and a pointer/index to the “current” 0.0.102 book.

Algorithm
1) (Optional) Filter input transactions to those that are address book related:
- FileAppend/FileUpdate/FileCreate where file id is 0.0.101 or 0.0.102
- NodeCreate/NodeUpdate/NodeDelete

2) For each transaction in order:
- If FileUpdate/FileCreate on 0.0.101/0.0.102:
- Initialize/replace the partial buffer for that file id with the provided contents (ignore empty).
- Attempt to parse NodeAddressBook from the buffer. If parse succeeds, add new version to history and, if 0.0.102, update “current”.
- If FileAppend on 0.0.101/0.0.102:
- Append the contents (ignore empty) to the buffer for that file id.
- Attempt to parse NodeAddressBook from the buffer. If parse succeeds, add new version to history and, if 0.0.102, update “current”.
- If NodeCreate/NodeUpdate/NodeDelete:
- Apply to a working copy of the current effective book (prefer 0.0.102; fall back to the last known book if none yet).
- Produce a new NodeAddressBook from the modified data and add it to history; update “current”.

3) Deduplication and merge rules (when building a book):
- Group NodeAddress entries by node id.
- For each group, union service endpoints across duplicates.
- Treat the deprecated ipAddress/port as a service endpoint too when present.
- Enforce ipAddressV4 to be exactly 4 bytes for service endpoints.

Notes
- A completed parse is the only signal that a file-based update has reached a full address book. Don’t assume a block boundary implies completion.
- If you need deterministic version timestamps, record the transaction consensus timestamp and set start = consensus + 1 for file-based versions; for node lifecycle versions, use the transaction’s consensus + 1.

## 8) Field mappings (PBJ vs classic proto)

Mirror Node tests use classic proto classes (com.hederahashgraph.api.proto.java). In PBJ (com.hedera.hapi.node.*):
- NodeAddressBook: com.hedera.hapi.node.base.NodeAddressBook
- NodeAddress: com.hedera.hapi.node.base.NodeAddress
- ServiceEndpoint: com.hedera.hapi.node.base.ServiceEndpoint
- FileAppendTransactionBody: com.hedera.hapi.node.file.FileAppendTransactionBody (and similar for FileUpdate, FileCreate)
- The field semantics are the same. Be careful to use ipAddressV4 as raw 4-byte IPv4, not a string.

## 9) Practical tips and pitfalls

- Parsing failures (especially malformed ipAddressV4) must reject the entire file-based update; otherwise you’ll diverge from Mirror Node behavior.
- Keep 0.0.101 and 0.0.102 partial buffers separate; appends for one must not be combined with the other.
- If you need to support legacy books where nodeId was 0, implement the (accountNum - 3) inference when accountNum < 20.
- If an agent wants to ignore domainName until HIP-869 rollout aligns, it can set domainName to empty for now.

## 10) Example skeleton for updateAddressBook (PBJ)

This is a sketch, not production code. It demonstrates the flow described above.

- Track buffers: Map<Long /*fileNum*/, ByteArrayOutputStream>
- Track history: List<NodeAddressBook>
- Track current index for 0.0.102

Pseudocode outline:

1) For each tx in addressBookTransactions:
- if tx.hasFileAppend() || tx.hasFileUpdate() || tx.hasFileCreate():
- if fileNum is 101 or 102:
- if update/create: buffers[fileNum] = new buffer with contents
- if append: buffers[fileNum].write(contents)
- try parse NodeAddressBook from buffers[fileNum]
- if ok: add to history; if fileNum == 102 set currentIndex
- else if tx.hasNodeCreate()/hasNodeUpdate()/hasNodeDelete():
- book = copy of getCurrentAddressBook()
- apply change; dedupe endpoints
- add new book to history; update currentIndex

2) Helper rules for applying node changes:
- Node identity match: prefer nodeAccountId else nodeId; legacy inference if needed
- Merge/replace semantics as noted above

This approach keeps your registry aligned with how Mirror Node computes address books from files while also letting you react to node lifecycle transactions in between file-based refreshes.
