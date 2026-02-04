# Historical Record File Wrapping

This is the process of converting the blockchain history of the Hedera network which is stored in record files into the
new standardized Block Stream format. The aim is to make it much easier for users of the blockchain history to read all
the data in a trusted way. The original history of the network in record files was stored in multiple custom binary
formats. There were little bits of data missing from the original record files, as the network was new and bugs
happened. That missing data was mostly minor but caused problems in validation of data integrity. This project is about
converting the history to a common easy-to-use format that is fully lossless and has all the missing data added in.

After investigation, it was found all the original formats were basically containers of a set of protobuf `Transaction`
and `TransactionRecord` protobuf messages. Record file V6 the most recent version of the format has a nice protobuf
container for these called `RecordStreamFile`. So by converting all the record files to this format along with sidecar
data we can have a single unified format that is lossless and fully reversible. The `SignedRecordFileProof` contains
what the version of the original file record format was. That can be used to compute a hash for the record file based
on the encodinge format and rules for that version from the contents of the `RecordFileItem`. Once you have computed the
hash you can verify the signatures in the `SignedRecordFileProof` to ensure the data is valid. You will need to know the
addressbook of nodes that made up the network at the point in time when the file was created and signed.

For each historical record file block we will produce a new block stream block. It will contain exactly
four block items in this order:

1. **`BlockHeader`** - Contains metadata for the block:
   - `hapi_proto_version`: The HAPI protobuf version from the original record file
   - `software_version`: Set to null (not available in historical record files)
   - `number`: The block number (sequential, starting from 0)
   - `block_timestamp`: The timestamp of the first transaction in the block
   - `hash_algorithm`: SHA2_384

2. **`StateChanges`** - only in block zero there will be state changes representing the initial state of the network at
      stream-start. All other wrapped blocks will not have a `StateChanges` item.

3. **`RecordFileItem`** - Contains the original record file data:
   - `creation_time`: The consensus time the record file was produced for (from the record file name)
   - `record_file_contents`: The record file contents converted to V6 `RecordStreamFile` format
   - `sidecar_file_contents`: Zero or more sidecar files associated with the record file

4. **`BlockFooter`** - Contains hashes for blockchain integrity:
   - `previous_block_root_hash`: The block hash of the previous wrapped block (or `sha384(0x0)` for block 0)
   - `root_hash_of_all_block_hashes_tree`: The merkle root of all block hashes from block 0 to the previous block
   - `start_of_block_state_root_hash`: Set to empty hash (state hashes were not included in record files)

5. **`BlockProof`** - Contains a `SignedRecordFileProof` with:
   - `version`: The original record file format version (2, 5, or 6)
   - `record_file_signatures`: RSA signatures from consensus nodes that signed the record file

## Appending TSS Block Proofs to the wrapped Block Stream
As well as maintaining a blockchain of all blocks through the `BlockFooter.previous_block_root_hash` chain, we also
maintain references to previous blocks though the `BlockFooter.root_hash_of_all_block_hashes_tree` merkle tree. The
reason for doing this is it allows for concise merkle proofs to be constructed for any block in the chain from any
future block. Once the first new block stream block is produced after the last wrapped record file block, and it is
signed with a [TSS](https://hips.hedera.com/hip/hip-1200) signature. We can use that TSS signature block proof and the
block hash merkle tree to produce a `StateProof` type of `BlockProof` for every historical wrapped block. Then append
each of those `BlockProof` items to the end of each wrapped block file. This means every historical block will have a
two block proofs, the original SignedRecordFileProof proof for trusting the original record data at the point it was
created and the new TSS signature block proof for trusting the new wrapped block with any amendments to the data. TSS
proofs only need a single well-known piece of data the Ledger ID to be able to verify and trust any block. They do not
need to know anything about address books or changes to address books over time, which greatly simplifies the
verification. This means as long as you trust the network today and its Ledger ID, you can verify and trust any block
new or old using a single verification method. This should make it much easier for users to read the blockchain history
as they have a single unified format and unified signature verification method. There should be no need to use the
SignedRecordFileProof if there is a TSS proof unless you tried the network historically and not today (which seems very
unlikely).

### RecordFileItem
```proto
/**
 * A Block Item for record files.
 *
 * A `RecordFileItem` contains data produced before the innovation of the
 * Block Stream, when data was stored in files and validated by individual
 * signature files.<br/>
 *
 * This item enables a single format, the Block Stream, to carry both
 * historical and current data; eliminating the need to search two sources for
 * block and block chain data.<br/>
 *
 * There are 3 versions of the binary file format that record files were
 * produced in: v2, v5 and v6. Version 6 was protobuf already in the format of
 * `proto.RecordStreamFile`. Version 5 was almost identical in content but a
 * custom binary encoding rather than protobuf. Version 2 was a different
 * custom binary format. All versions can be converted to a block stream block
 * with RecordFileItem in a fully reversible lossless way.<br/>
 *
 * The following pseudocode describes how to reconstruct the original record
 * file from the contents of this item. All numbers are big-endian.
 * <code><pre>{@code
 * recordFormatVersion = BlockProof.SignedRecordFileProof.version
 * PROCEDURE reconstructRecordFile(recordFormatVersion,
 *                                  hapiProtoVersion, previousBlockHash,
 *                                  recordStreamFile):
 *     WRITE version number (4 bytes, int)
 *     IF version == 2:
 *         WRITE HAPI major version (4 bytes int)
 *         WRITE previous file hash marker (1 byte = 0x01)
 *         WRITE previous block hash (48 bytes SHA-384)
 *         FOR EACH item IN recordStreamFile.recordStreamItems:
 *             WRITE record marker (1 byte = 0x02)
 *             WRITE transaction length (4 bytes int)
 *             WRITE transaction (protobuf bytes)
 *             WRITE transaction record length (4 bytes int)
 *             WRITE transaction record (protobuf bytes)
 *         END FOR
 *     ELSE IF version == 5:
 *         WRITE HAPI major version (4 bytes int)
 *         WRITE HAPI minor version (4 bytes int)
 *         WRITE HAPI patch version (4 bytes int)
 *         WRITE object stream version (4 bytes int = 1)
 *         WRITE start object running hash (V5 HashObject format)
 *         FOR EACH item IN recordStreamFile.recordStreamItems:
 *             WRITE class ID (8 bytes long = 0xe370929ba5429d8b)
 *             WRITE class version (4 bytes int = 1)
 *             WRITE transaction record length (4 bytes int)
 *             WRITE transaction record (protobuf bytes)
 *             WRITE transaction length (4 bytes int)
 *             WRITE transaction (protobuf bytes)
 *         END FOR
 *         WRITE end object running hash (V5 HashObject format)
 *     ELSE IF version == 6:
 *         WRITE RecordStreamFile (protobuf encoded)
 *     ELSE:
 *         THROW UnsupportedOperationException
 * END PROCEDURE
 * }</pre></code>
 *
 * Any block containing this item requires special handling.
 * - The block SHALL have a `BlockHeader`.
 *    - Some fields in the `BlockHeader` may be interpreted differently, and
 *      may depend on when the original record file was created.
 * - The block SHALL contain _exactly one_ `RecordFileItem`.
 * - The block SHALL have a `BlockFooter`.
 * - The block SHALL have a `SignedRecordFileProof` type `BlockProof`.
 * - The block SHALL NOT contain any content item other than a block header,
 *   single `RecordFileItem`, block footer and one or more block proofs.
 */
message RecordFileItem {
    /**
     * The consensus time the record file was produced for.<br/>
     * This comes from the record file name.
     */
    proto.Timestamp creation_time = 1;

    /**
     * The contents of a record file.<br/>
     * Record files were originally stored in different binary formats,
     * this is the content converted to V6 format RecordStreamFile. That
     * conversion is lossless and reversible. It will need to be reversed to
     * validate the block with a `SignedRecordFileProof`.
     */
    proto.RecordStreamFile record_file_contents = 2;

    /**
     * The contents of sidecar files for this block.<br/>
     * Each block can have zero or more sidecar files.
     */
    repeated proto.SidecarFile sidecar_file_contents = 3;

    /**
     * Amendments to the record file data.<br/>
     * Over the years there were various bugs that caused data to be missed,
     * incorrect or added erroneously.  Amendments are used to fix those
     * mistakes with the aim that all data in the block stream is correct and
     * complete. This field carries the record items that amend the original
     * record file contents. Each item may contain a Transaction and/or
     * TransactionRecord that are intended to replace the original data. They
     * can be matched by the transaction ID. If you want the original raw data
     * then you can ignore the contents of this field.
     */
    repeated proto.RecordStreamItem amendments = 4;
}
```

### SignedRecordFileProof
```proto
/**
 * A proof containing RSA signatures from consensus nodes for a block that was
 * originally recorded in the record file format. It contains a list of
 * signatures over the record file hash. How to compute the record file hash
 * depends on the record file format version it was originally created with. To
 * verify a block with a SignedRecordFileProof, the record file hash needs to be
 * computed according to the record file format version and then verified with
 * the signatures in this proof and the public keys of the consensus nodes at
 * the point the record file was created. The public keys can be obtained form
 * the network address book from a trusted source. See `RecordFileItem`
 * documentation for description of how to compute the record file hash.
 */
message SignedRecordFileProof {
    /**
     * The record file format version, this dictates how the hash that is signed
     * is computed. Valid versions are 2, 5 and 6.
     */
    uint32 version = 1;

    /**
     * A collection of RSA signatures from consensus nodes.<br/>
     * These signatures validate the record file hash.
     */
    repeated RecordFileSignature record_file_signatures = 2;
}
```
