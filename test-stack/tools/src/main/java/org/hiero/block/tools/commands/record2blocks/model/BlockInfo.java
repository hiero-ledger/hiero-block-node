// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.record2blocks.model;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.hiero.block.tools.commands.record2blocks.model.ChainFile.Kind;
import picocli.CommandLine.Help.Ansi;

/**
 * BlockInfo represents a Hedera block with its associated record files, sidecar files and signature files.
 *
 * @param blockNum the block number
 * @param blockTime the block time
 * @param recordFiles the record files associated with the block
 * @param mostCommonRecordFile the record file with the most occurrences
 * @param sidecarFiles the sidecar files associated with the block
 * @param signatureFiles the signature files associated with the block
 */
@SuppressWarnings("unused")
public record BlockInfo(
        long blockNum,
        long blockTime,
        List<ChainFile> recordFiles,
        ChainFileAndCount mostCommonRecordFile,
        SortedMap<Integer, NumberedSidecarFile> sidecarFiles,
        List<ChainFile> signatureFiles) {

    /**
     * Create a new BlockInfo instance by passing in all files associated with the block. They are then divided into
     * record files, sidecar files and signature files.
     *
     * @param blockNum the block number
     * @param blockTime the block time
     * @param allBlockFiles all files associated with the block
     */
    public BlockInfo(long blockNum, long blockTime, List<ChainFile> allBlockFiles) {
        this(
                blockNum,
                blockTime,
                allBlockFiles.stream().filter(cf -> cf.kind() == Kind.RECORD).collect(Collectors.toList()),
                mostCommonRecordFileMd5EntryAndCount(allBlockFiles),
                collectSidecarFiles(allBlockFiles),
                allBlockFiles.stream().filter(cf -> cf.kind() == Kind.SIGNATURE).collect(Collectors.toList()));
    }

    /**
     * Find the record file with the most occurrences in the list of all block files. This works on the assumption that
     * the record file with the most occurrences is the one that is most likely to be the correct record file.
     *
     * @param allBlockFiles all files associated with the block
     * @return the record file with the most occurrences and the number of occurrences
     */
    private static ChainFileAndCount mostCommonRecordFileMd5EntryAndCount(List<ChainFile> allBlockFiles) {
        final Map<String, Long> md5Counts = allBlockFiles.stream()
                .filter(cf -> cf.kind() == Kind.RECORD)
                .collect(Collectors.groupingBy(ChainFile::md5, Collectors.counting()));
        final var maxCountentry =
                md5Counts.entrySet().stream().max(Map.Entry.comparingByValue()).orElse(null);
        if (maxCountentry == null) {
            throw new IllegalStateException("No record files found");
        }
        final ChainFile maxCountRecordFile = allBlockFiles.stream()
                .filter(cf -> cf.md5().equals(maxCountentry.getKey()))
                .findFirst()
                .orElse(null);
        return new ChainFileAndCount(
                maxCountRecordFile, maxCountentry.getValue().intValue());
    }

    /**
     * Collect sidecar files from all block files. There can be multiple sidecar files for a block, each with multiple
     * copies for from each node. This groups them by sidecar index and returns the most common sidecar file for each
     * index in a sorted map.
     *
     * @param allBlockFiles all files associated with the block
     * @return a sorted map of sidecar files, keyed by sidecar index
     */
    private static SortedMap<Integer, NumberedSidecarFile> collectSidecarFiles(List<ChainFile> allBlockFiles) {
        // group sidecar files by sidecar index
        final Map<Integer, List<ChainFile>> sidecarFiles = allBlockFiles.stream()
                .filter(cf -> cf.kind() == Kind.SIDECAR)
                .collect(Collectors.groupingBy(ChainFile::sidecarIndex));
        final TreeMap<Integer, NumberedSidecarFile> sortedSidecarFiles = new TreeMap<>();
        sidecarFiles.forEach((sidecarIndex, sidecarFileList) ->
                sortedSidecarFiles.put(sidecarIndex, new NumberedSidecarFile(sidecarFileList)));
        return sortedSidecarFiles;
    }

    /** Template used for rendering to string. */
    private static final String TEMPLATE = Ansi.AUTO.string(
            "@|bold,yellow BlockInfo{|@  @|yellow blockNum=|@$blockNum, @|yellow blockTime=|@$blockTime "
                    + "@|bold,yellow recordFiles|@ @|yellow total=|@$recordFileCount @|yellow "
                    + "matching=|@$recordFilesMatching @|cyan -> $recordFilePercent%|@ "
                    + "@|bold,yellow sidecarFiles=|@  $sidecarFiles"
                    + "@|bold,yellow signatureFiles|@ @|yellow total=|@$signatureFilesSize "
                    + "@|bold,yellow }|@");

    /**
     * Render the block info as a string in nice colored output for the console.
     *
     * @return the block info as a string
     */
    @Override
    public String toString() {
        // check
        return TEMPLATE.replace("$blockNum", String.valueOf(blockNum))
                .replace("$blockTime", String.valueOf(blockTime))
                .replace("$recordFileCount", String.valueOf(recordFiles.size()))
                .replace("$recordFilesMatching", String.valueOf(mostCommonRecordFile.count()))
                .replace(
                        "$recordFilePercent",
                        String.valueOf(((mostCommonRecordFile.count() / (double) recordFiles.size()) * 100)))
                .replace("$mostCommonRecordFile", mostCommonRecordFile.toString())
                .replace(
                        "$sidecarFiles",
                        sidecarFiles.isEmpty()
                                ? "none"
                                : sidecarFiles.values().stream()
                                        .map(NumberedSidecarFile::toString)
                                        .collect(Collectors.joining(", ")))
                .replace("$signatureFilesSize", String.valueOf(signatureFiles.size()));
    }
}
