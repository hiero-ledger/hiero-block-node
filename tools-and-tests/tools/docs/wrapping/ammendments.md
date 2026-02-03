# Amendments

The aim of amendments in the record file to the Block Stream conversion process is to add in any important data that was
missed in the original record stream files that is needed by mirror node or other historical data users. Data they
need to have a complete view of the network history and all account balances and all changes to account balances.

There are two main types of data that are missing from the record stream:
- All entities created before Stream-Start
- Incorrect Transaction Records
  - Missing Transactions
  - Failed Transfers in Record
  - Record Missing for Insufficient Fee Funding
  - Record Missing for FAIL_INVALID NFT transfers

Mirror Node documentation link where much of this information came from https://github.com/hiero-ledger/hiero-mirror-node/blob/main/docs/database/README.md#errata

## All entities created before Stream-Start

The network was started at Genisis on 2018-12-16 at 16:34:32.32 almost a year before Stream-Start. During that time
15,267 accounts and many other entities were created. None of those or their information are recorded in the record
stream. Historical data users need to have a complete view of the network history so need this data. One of the key
integrity checks is to sum all HBAR in all accounts, and it has to add up to 50 billion HBAR. That is impossible if you
do not know the initial balances of all accounts.

The only source of truth for initial balances of accounts is saved state dumps from the network. Unfortunately there was
not one recorded at the start of Block 0.

### All Historical Data Recorded from block 0 to 3

| Event                                                                                                 | Consensus Timestamp  | Consensus Time UTC          |
|-------------------------------------------------------------------------------------------------------|----------------------|-----------------------------|
| **Block 0**                                                                                           | 1568411631.396440000 | 2019-09-13T21:53:51.396440Z |
| └─ Txn [0.0.11337@1568411616.448357000](https://hashscan.io/mainnet/transaction/1568411631.396440000) | 1568411631.396440000 | 2019-09-13T21:53:51.396440Z |
| Saved State 33485415                                                                                  | 1568411631.916679000 | 2019-09-13T21:53:51.916679Z |
| **Block 1**                                                                                           | 1568411670.872035001 | 2019-09-13T21:54:30.872035Z |
| └─ Txn [0.0.11337@1568411656.265684000](https://hashscan.io/mainnet/transaction/1568411670.872035001) | 1568411670.872035001 | 2019-09-13T21:54:30.872035Z |
| **Block 2**                                                                                           | 1568411762.486929000 | 2019-09-13T21:56:02.486929Z |
| └─ Txn [0.0.11337@1568411747.660028000](https://hashscan.io/mainnet/transaction/1568411762.486929000) | 1568411762.486929000 | 2019-09-13T21:56:02.486900Z |
| Balances CSV                                                                                          | 1568412000.810000000 | 2019-09-13T22:00:00.000081Z |
| Saved State 33486127                                                                                  | 1568412000.810000000 | 2019-09-13T22:00:00.000081Z |
| **Block 3**                                                                                           | 1568412919.286477002 | 2019-09-13T22:15:19.286477Z |
| └─ Txn [0.0.14622@1568412908.640207071](https://hashscan.io/mainnet/transaction/1568412919.286477002) | 1568412919.286477002 | 2019-09-13T22:15:19.286477Z |

The closest we have is a saved state taken at round 33485415 which is the end of Block 0. So the result is the only way
to get a state at the beginning of block 0 is to take that snapshot and reverse the single transaction that happened in
block zero. Luckily, that transaction was only a simple crypto transfer, and it was recorded in the record stream. So
all that is needed is to read the 33485415 and reverse the transfers made in the single transaction in block zero.

### Saved State at the start of Block 0: Summary of Contents

|       Property       |                    Value                     |
|----------------------|----------------------------------------------|
| Round:               | 33,485,415                                   |
| Consensus Timestamp: | 2019-09-13T21:53:51.916679Z                  |
| Accounts:            | 15,368 (15,267 normal + 101 smart contracts) |
| Total Balance:       | 5,000,000,000,000,000,000 tinybars           |
| Files:               | 130                                          |
| Contract Bytecodes:  | 101                                          |
| Contract Storage:    | 95                                           |
| Binary Objects:      | 265                                          |
| Storage KV Pairs:    | 456                                          |


## Missing Transactions

There were 113 transactions have been missing from the record stream between Block Zero and the end of 2025. Mirror
Node keeps a copy of the missing transactions in its GIT repository. All of these will need to be added to the Block
Stream files as amendments.

https://github.com/hiero-ledger/hiero-mirror-node/tree/main/importer/src/main/resources/errata/mainnet/missingtransactions

## Failed Transfers in Record

Between 2019-09-14 and 2019-10-03 there were 1177 transactions that failed due to insufficient account balance. The
attempted transfers were nonetheless listed in the record. The bug that caused it was fixed in Consensus Node v0.4.0
late 2019. Block stream files will need amendments with corrected transfer lists for all affected transactions.

### Detection Rule

The logic is in ErrataMigration.java:182-204. The SQL query identifies transfers matching these criteria:

### Transaction Criteria

- Type 14 (CRYPTOTRANSFER)
- Failed result (result ≠ 22, which is SUCCESS)
- Timestamp before 1577836799000000000 (Oct 3, 2019 23:59:59 UTC)

### Transfer Criteria (for the credit side)

- Positive amount (credit to an account)
- Not account 0.0.98 (fee collector)
- Not system accounts 0.0.3 through 0.0.27
- Special case: two specific timestamps allow transfers where receiver = payer:
    - 1570118944399195000
    - 1570120372315307000

### Matching Logic

The query first finds spurious credits, then marks both the credit and its corresponding debit (same timestamp, negated amount) as errata = 'DELETE'.

### Key Code Locations

1. SQL Migration: hedera-mirror-importer/src/main/java/com/hedera/mirror/importer/migration/ErrataMigration.java:173-204
2. Runtime Detection: hedera-mirror-importer/src/main/java/com/hedera/mirror/importer/parser/record/entity/EntityRecordItemListener.java:302-324 - marks failed crypto transfers during parsing with:
   boolean failedTransfer = !recordItem.isSuccessful() && body.hasCryptoTransfer()
   && consensusTimestamp < 1577836799000000000L;


## Record Missing for Insufficient Fee Funding

Between 2019-09-14 and 2019-09-18 there were 31 transactions where the transaction over-bid the balance of its payer
account as a fee payment, its record was omitted from the stream. When a transaction’s payer account could not afford
the network fee, its record was omitted. The bug that caused it was fixed in Consensus Node v0.4.0 late 2019. Block
stream files will need amendments with missing transaction records for all affected transactions.

### Detection Rule

These transactions cannot be detected from the record stream since they are completely missing. The only way to know
about them is from external sources (state dumps, node logs, or manual reconstruction).

### Data Source

The missing transactions are stored as binary Protocol Buffer files at:
[missingtransactions/](https://github.com/hashgraph/hedera-mirror-node/tree/main/hedera-mirror-importer/src/main/resources/errata/mainnet/missingtransactions)

**31 files for this errata (2019-09-14 to 2019-09-18):**
```
2019-09-14T11_18_30.225708Z.bin
2019-09-14T11_18_30.651725Z.bin
2019-09-17T00_32_57.679941001Z.bin
2019-09-17T00_32_57.679941002Z.bin
2019-09-17T00_32_57.679941003Z.bin
2019-09-17T00_32_57.679941004Z.bin
2019-09-17T00_32_57.757350Z.bin
2019-09-17T00_32_57.786671Z.bin
2019-09-17T00_32_57.931682001Z.bin
2019-09-17T00_32_58.101593003Z.bin
2019-09-17T00_32_58.147060Z.bin
2019-09-17T00_32_58.147060001Z.bin
2019-09-17T00_32_58.239454Z.bin
2019-09-17T00_32_58.239454001Z.bin
2019-09-17T00_32_58.278240Z.bin
2019-09-17T00_32_58.284792001Z.bin
2019-09-17T00_32_58.454596Z.bin
2019-09-17T00_32_58.540615002Z.bin
2019-09-17T00_32_58.592212Z.bin
2019-09-17T00_32_58.592212001Z.bin
2019-09-17T00_32_58.617920Z.bin
2019-09-17T00_32_58.768393Z.bin
2019-09-17T00_32_58.804355Z.bin
2019-09-17T00_32_58.804355002Z.bin
2019-09-17T00_32_58.952731Z.bin
2019-09-17T00_32_58.998719Z.bin
2019-09-17T00_32_59.117911Z.bin
2019-09-17T00_32_59.117911001Z.bin
2019-09-17T00_32_59.191833Z.bin
2019-09-17T00_32_59.270289Z.bin
2019-09-18T19_04_30.584508001Z.bin
```

### Binary File Format

Each `.bin` file contains two length-prefixed Protocol Buffer messages in sequence:

```
[4-byte length][TransactionRecord protobuf bytes][4-byte length][Transaction protobuf bytes]
```

**Reading algorithm:**
1. Read 4 bytes as big-endian int32 → `recordLength`
2. Read `recordLength` bytes → parse as `TransactionRecord` protobuf
3. Read 4 bytes as big-endian int32 → `transactionLength`
4. Read `transactionLength` bytes → parse as `Transaction` protobuf

**Protobuf definitions:**
- `TransactionRecord`: [basic_types.proto](https://github.com/hashgraph/hedera-protobufs/blob/main/services/basic_types.proto)
- `Transaction`: [transaction.proto](https://github.com/hashgraph/hedera-protobufs/blob/main/services/transaction.proto)

### Amendment Action

For each missing transaction:
1. Read the `.bin` file and parse both protobuf messages
2. Insert the full transaction record into the block stream at its consensus timestamp
3. The consensus timestamp is extracted from the `TransactionRecord.consensusTimestamp` field
4. File names encode the timestamp as: `YYYY-MM-DDTHH_MM_SS.nnnnnnnnnZ.bin`

### Code References

- [ErrataMigration.java - missingTransactions()](https://github.com/hashgraph/hedera-mirror-node/blob/main/hedera-mirror-importer/src/main/java/com/hedera/mirror/importer/migration/ErrataMigration.java#L210-L260)
- [ValidatedDataInputStream.java - readLengthAndBytes()](https://github.com/hashgraph/hedera-mirror-node/blob/main/hedera-mirror-importer/src/main/java/com/hedera/mirror/importer/reader/ValidatedDataInputStream.java)


## Record Missing for FAIL_INVALID NFT Transfers (2022)

Between 2022-07-31 and 2022-08-09 there were 60 transactions where the NFT transfers were not recorded properly. Any
ledger that will grow to billions of entities must have an efficient way to remove expired entities. In the Hedera
network, this means keeping a list of NFTs owned by an account so that when an account expires, we can return its NFTs
to their respective treasury accounts. Under certain conditions in the 0.27.5 release, a bug in the logic maintaining
these lists could cause NFT transfers to fail without refunding fees. This would manifest itself as a `FAIL_INVALID`
transaction that does not get written to the record stream.

. The bug that caused it was fixed in Consensus Node v0.27.7 on August 9th 2022. Block
stream files will need amendments with correct transaction records for all affected transactions.

### Detection Rule

These transactions cannot be detected from the record stream since they are completely missing. The missing transactions
were reconstructed from node state and logs.

### Data Source

The missing transactions are stored as binary Protocol Buffer files at:
[missingtransactions/](https://github.com/hashgraph/hedera-mirror-node/tree/main/hedera-mirror-importer/src/main/resources/errata/mainnet/missingtransactions)

**70 files for this errata (2022-07-31 to 2022-08-09):**
- Files matching pattern: `2022-07-*.bin` and `2022-08-0[1-9]*.bin`
- Date range: `2022-07-31T08_03_36.811335554Z.bin` to `2022-08-07T23_03_20.760386695Z.bin`

### Binary File Format

Same format as "Record Missing for Insufficient Fee Funding" above:
```
[4-byte length][TransactionRecord protobuf bytes][4-byte length][Transaction protobuf bytes]
```

### Amendment Action

For each missing transaction:
1. Read the `.bin` file and parse both protobuf messages
2. Insert the full transaction record into the block stream at its consensus timestamp
3. These transactions have result code `FAIL_INVALID` and include NFT transfer operations

### Code References

- [ErrataMigration.java - missingTransactions()](https://github.com/hashgraph/hedera-mirror-node/blob/main/hedera-mirror-importer/src/main/java/com/hedera/mirror/importer/migration/ErrataMigration.java#L210-L260)



## Record Missing for FAIL_INVALID NFT Transfers (2023)
Between 2023-02-10 and 2023-02-14 there were 12 transactions where a bug in bookkeeping for NFT TokenWipe and TokenBurn
operations with redundant serial numbers caused an account to appear to own fewer NFTs than it actually does. This could
subsequently prevent an NFT owner from being changed as part of an atomic operation. When the atomic operation fails,
a `FAIL_INVALID` transaction occurs but was not written to the record stream. The bug that caused it was fixed in Hedera
Services v0.34.2 late 2019. Block stream files will need amendments with missing transaction records for all affected
transactions.

### Detection Rule

These transactions cannot be detected from the record stream since they are completely missing. The missing transactions
were reconstructed from node state and logs.

### Data Source

The missing transactions are stored as binary Protocol Buffer files at:
[missingtransactions/](https://github.com/hashgraph/hedera-mirror-node/tree/main/hedera-mirror-importer/src/main/resources/errata/mainnet/missingtransactions)

**12 files for this errata (2023-02-10 to 2023-02-14):**
```
2023-02-10T02_16_18.649144003Z.bin
2023-02-11T23_33_52.126565693Z.bin
2023-02-14T09_30_00.113171959Z.bin
2023-02-14T10_10_10.078089177Z.bin
2023-02-14T10_16_41.978788203Z.bin
2023-02-14T10_20_33.581677003Z.bin
2023-02-14T10_37_35.637466567Z.bin
2023-02-14T10_47_32.316855602Z.bin
2023-02-14T10_51_52.172485776Z.bin
2023-02-14T11_00_27.192493193Z.bin
2023-02-14T11_03_05.105650927Z.bin
2023-02-14T16_37_52.762823785Z.bin
```

### Binary File Format

Same format as "Record Missing for Insufficient Fee Funding" above:
```
[4-byte length][TransactionRecord protobuf bytes][4-byte length][Transaction protobuf bytes]
```

### Amendment Action

For each missing transaction:
1. Read the `.bin` file and parse both protobuf messages
2. Insert the full transaction record into the block stream at its consensus timestamp
3. These transactions have result code `FAIL_INVALID` and relate to NFT TokenWipe/TokenBurn operations

### Special Handling: Missing Token Transfers

For the 2023 errata, some transactions may already exist in the record stream but are missing their token transfer
records. The amendment should also ensure all `TokenTransfer` records from the `TransactionRecord` are properly inserted.

### Code References

- [ErrataMigration.java - missingTransactions()](https://github.com/hashgraph/hedera-mirror-node/blob/main/hedera-mirror-importer/src/main/java/com/hedera/mirror/importer/migration/ErrataMigration.java#L210-L260)
- [ErrataMigration.java - missingTokenTransfers()](https://github.com/hashgraph/hedera-mirror-node/blob/main/hedera-mirror-importer/src/main/java/com/hedera/mirror/importer/migration/ErrataMigration.java#L263-L290)


## Summary of All Missing Transaction Files

The complete list of 113 missing transaction files by category:

| Category | Period | Count | Description |
|----------|--------|-------|-------------|
| Insufficient Fee Funding | 2019-09-14 to 2019-09-18 | 31 | Payer couldn't afford fees |
| FAIL_INVALID NFT (2022) | 2022-07-31 to 2022-08-09 | 70 | NFT ownership list bug |
| FAIL_INVALID NFT (2023) | 2023-02-10 to 2023-02-14 | 12 | NFT wipe/burn bookkeeping bug |
| **Total** | | **113** | |

All files use the same binary format and are stored in the same directory.
