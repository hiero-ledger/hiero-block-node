// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.download;

import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import org.hiero.block.tools.records.model.parsed.ParsedRecordFile;
import org.hiero.block.tools.records.model.unparsed.InMemoryFile;

/**
 * Utility class for validating block hashes during download operations.
 *
 * <p>Provides methods for verifying block hash consistency between downloaded
 * record files and expected values from the mirror node.
 */
public final class DownloadDayUtil {

    /** Private constructor to prevent instantiation of this utility class. */
    private DownloadDayUtil() {
        // Utility class - do not instantiate
    }

    /**
     * Validate block hashes for the given block's record files.
     *
     * @param blockNum the block number
     * @param inMemoryFilesForWriting the list of in-memory record files for this block
     * @param prevRecordFileHash the previous record file hash to validate against (can be null)
     * @param blockHashFromMirrorNode the expected block hash from mirror node listing (can be null)
     * @return the computed block hash from this block's record file
     * @throws IllegalStateException if any hash validation fails
     */
    public static byte[] validateBlockHashes(
            final long blockNum,
            final List<InMemoryFile> inMemoryFilesForWriting,
            final byte[] prevRecordFileHash,
            final byte[] blockHashFromMirrorNode) {
        final InMemoryFile mostCommonRecordFileInMem = inMemoryFilesForWriting.getFirst();
        final ParsedRecordFile recordFileInfo = ParsedRecordFile.parse(mostCommonRecordFileInMem);
        byte[] readPreviousBlockHash = recordFileInfo.previousBlockHash();
        byte[] computedBlockHash = recordFileInfo.blockHash();
        if (blockHashFromMirrorNode != null && !Arrays.equals(blockHashFromMirrorNode, computedBlockHash)) {
            throw new IllegalStateException(
                    "Block[%d] hash mismatch with mirror node listing. Expected: %s, Found: %s\nContext mostCommonRecordFile:%s computedHash:%s"
                            .formatted(
                                    blockNum,
                                    HexFormat.of()
                                            .formatHex(blockHashFromMirrorNode)
                                            .substring(0, 8),
                                    HexFormat.of().formatHex(computedBlockHash).substring(0, 8),
                                    mostCommonRecordFileInMem.path(),
                                    HexFormat.of().formatHex(computedBlockHash).substring(0, 8)));
        }
        if (prevRecordFileHash != null && !Arrays.equals(prevRecordFileHash, readPreviousBlockHash)) {
            throw new IllegalStateException(
                    "Block[%d] previous block hash mismatch. Expected: %s, Found: %s\nContext mostCommonRecordFile:%s computedHash:%s"
                            .formatted(
                                    blockNum,
                                    HexFormat.of().formatHex(prevRecordFileHash).substring(0, 8),
                                    HexFormat.of()
                                            .formatHex(readPreviousBlockHash)
                                            .substring(0, 8),
                                    mostCommonRecordFileInMem.path(),
                                    HexFormat.of().formatHex(computedBlockHash).substring(0, 8)));
        }
        return computedBlockHash;
    }
}
