// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.history.model;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Instant;
import java.util.List;

/**
 * In-memory representation of a set of record files for a single time slot. Typically, a 2-second period of consensus
 * time. The record file set includes the primary most common record file, signature files, and sidecar files.
 *
 * @param recordFileTime  the record file time. All files in this set should have this time.
 * @param primaryRecordFile       the main record file. This is the one most nodes agreed on.
 * @param otherRecordFiles list of other record files. All record files that did not match the primary record file.
 *                         Normally empty.
 * @param signatureFiles   list of signature files
 * @param primarySidecarFiles     list of sidecar files the most common sidecar file for each index that all nodes
 *                                agreed on. Only ever one per index. There can be no sidecar files at all.
 * @param otherSidecarFiles       list of other sidecar files. All sidecar files that did not match the primary sidecar
 *                                files. Normally empty.
 */
public record InMemoryRecordFileSet(
        Instant recordFileTime,
        InMemoryFile primaryRecordFile,
        List<InMemoryFile> otherRecordFiles,
        List<InMemoryFile> signatureFiles,
        List<InMemoryFile> primarySidecarFiles,
        List<InMemoryFile> otherSidecarFiles) {

    public InMemoryRecordFileSet {
        if (recordFileTime == null) {
            throw new IllegalArgumentException("recordFileTime cannot be null");
        }
        if (signatureFiles == null) {
            throw new IllegalArgumentException("signatureFiles cannot be null");
        }
        if (primarySidecarFiles == null) {
            throw new IllegalArgumentException("primarySidecarFiles cannot be null");
        }
        if (otherSidecarFiles == null) {
            throw new IllegalArgumentException("otherSidecarFiles cannot be null");
        }
        if (otherRecordFiles == null) {
            throw new IllegalArgumentException("otherRecordFiles cannot be null");
        }
    }

    @Override
    public @NonNull String toString() {
        return String.format(
                "-- RecordFileSet @ %-32s :: primary=%b, signatures=%2d%s%s",
                recordFileTime,
                primaryRecordFile != null,
                signatureFiles.size(),
                primarySidecarFiles.isEmpty() ? "" : ", primary sidecars=" + primarySidecarFiles.size(),
                otherRecordFiles.isEmpty() ? "" : ", other record files=" + otherRecordFiles.size());
    }
}
