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

For each historical record file block we will produce a new block stream block. It will contain:
 - A `BlockHeader` with the same consensus timestamp as the original record file block.
 - A `RecordFileItem` containing the original record file contents.
 - A `SignedRecordFileProof` containing the signatures from the nodes that signed the record file.
 - A `BlockFooter` with the same consensus timestamp as the original record file block.

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
