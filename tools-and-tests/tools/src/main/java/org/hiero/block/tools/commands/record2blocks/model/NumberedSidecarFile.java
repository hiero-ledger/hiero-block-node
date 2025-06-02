// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.record2blocks.model;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;
import picocli.CommandLine.Help.Ansi;

/**
 * NumberedSidecarFile represents a set of sidecar files from all nodes for a single numbered sidecar file for a
 * record file.
 *
 * @param sidecarFileNum the numbered sidecar file
 * @param mostCommonSidecarFile the most common sidecar file by MD5 hash
 * @param sidecarFiles the list of sidecar files
 */
public record NumberedSidecarFile(
        int sidecarFileNum, List<ChainFile> sidecarFiles, ChainFileAndCount mostCommonSidecarFile) {

    /**
     * Create a NumberedSidecarFile from a list of sidecar files.
     *
     * @param sidecarFiles the list of sidecar files
     */
    public NumberedSidecarFile(List<ChainFile> sidecarFiles) {
        this(sidecarFiles.getFirst().sidecarIndex(), sidecarFiles, findMostCommonByMD5(sidecarFiles));
    }

    /**
     * Find the most common sidecar file by MD5 hash. If there is more than one with most common MD5 hash this just
     * picks any one.
     *
     * @param sidecarFiles the list of sidecar files
     * @return the most common sidecar file by MD5 hash as key and count as value
     */
    private static ChainFileAndCount findMostCommonByMD5(List<ChainFile> sidecarFiles) {
        final Entry<ChainFile, Long> result = sidecarFiles.stream()
                .filter(Objects::nonNull)
                .collect(Collectors.groupingBy(md5 -> md5, Collectors.counting()))
                .entrySet()
                .stream()
                .max(Map.Entry.comparingByValue())
                .orElseThrow();
        return new ChainFileAndCount(result.getKey(), result.getValue().intValue());
    }

    /** Template used for rendering to string. */
    private static final String TEMPLATE = Ansi.AUTO.string("@|bold,yellow NumberedSidecarFile{|@  "
            + "@|yellow sidecarFileNum=|@$sidecarFileNum, "
            + "@|yellow sidecarFilesCount=|@$sidecarFilesCount "
            + "@|yellow mostCommon=|@$mostCommonCount "
            + "@|bold,yellow }|@");

    @Override
    public String toString() {
        // check
        return TEMPLATE.replace("$sidecarFileNum", String.valueOf(sidecarFileNum))
                .replace("$sidecarFilesCount", String.valueOf(sidecarFiles.size()))
                .replace("$mostCommonCount", String.valueOf(mostCommonSidecarFile.count()));
    }
}
