// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records;

import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.hiero.block.tools.commands.days.listing.ListingRecordFile;

/**
 * Utility class for extracting consensus timestamps from Hedera record file names and paths.
 */
public class RecordFileUtils {
    private static final Pattern RECORD_FILE_NAME_PATTERN = Pattern.compile(
            "(\\d{4}-\\d{2}-\\d{2}T\\d{2}_\\d{2}_\\d{2}\\.\\d+Z)");

    /**
     * Extract the record file time from a record file path. Extracts the timestamp string from file paths like:
     * "recordstreams_record0.0.10_2022-02-01T17_30_10.014479000Z.rcd" or
     * "/path/to/2024-07-06T16_42_40.006863632Z.rcd.gz"
     *
     * @param path the record file path
     * @return the record file time as an Instant
     */
    public static String extractRecordFileTimeStrFromPath(Path path) {
        final String fileName = path.getFileName().toString();
        final Matcher matcher = RECORD_FILE_NAME_PATTERN.matcher(fileName);
        // return last match if multiple found
        String lastMatch = null;
        while (matcher.find()) {
            lastMatch = matcher.group(1);
        }
        if (lastMatch == null) {
            throw new IllegalArgumentException("No valid record file time found in path: " + path);
        }
        return lastMatch;
    }

    /**
     * Extract the record file time from a record file path.
     *
     * @param path the record file path, like "/path/to/2024-07-06T16_42_40.006863632Z.rcd.gz"
     * @return the record file time as an Instant
     */
    public static LocalDateTime extractRecordFileTimeFromPath(Path path) {
        return extractRecordFileTime(path.getFileName().toString());
    }

    /**
     * Extract the record file time from a record file path.
     *
     * @param path the record file path, like "/path/to/2024-07-06T16_42_40.006863632Z.rcd.gz"
     * @return the record file time as an Instant
     */
    public static LocalDateTime extractRecordFileTimeFromPath(String path) {
        String fileName = path.substring(path.lastIndexOf('/') + 1);
        return extractRecordFileTime(fileName);
    }

    /**
     * Extract the record file time from a record file name.
     *
     * @param recordOrSidecarFileName the record file name, like "2024-07-06T16_42_40.006863632Z.rcd.gz" or a sidecar
     *                                file name like "2024-07-06T16_42_40.006863632Z_02.rcd.gz"
     * @return the record file time as an Instant
     */
    public static LocalDateTime extractRecordFileTime(String recordOrSidecarFileName) {
        String dateString;
        // check if a sidecar file
        if (recordOrSidecarFileName.contains("Z_")) {
            dateString = recordOrSidecarFileName
                    .substring(0, recordOrSidecarFileName.lastIndexOf("_"))
                    .replace('_', ':');
        } else {
            dateString = recordOrSidecarFileName
                    .substring(0, recordOrSidecarFileName.indexOf(".rcd"))
                    .replace('_', ':');
        }
        try {
            return LocalDateTime.ofInstant(Instant.parse(dateString), ZoneOffset.UTC);
        } catch (DateTimeParseException e) {
            throw new RuntimeException(
                    "Invalid record file name: \"" + recordOrSidecarFileName + "\" - dateString=\"" + dateString + "\"",
                    e);
        }
    }

    /**
     * Find the most common RecordFile in the given list of files of the given type. If there is a tie, return the one
     * with the lowest MD5 hash.
     *
     * @param files the list of RecordFile objects to search
     * @param type the type of RecordFile to consider
     * @return the most common RecordFile of the given type, or null if none found
     */
    public static ListingRecordFile findMostCommonByType(List<ListingRecordFile> files, ListingRecordFile.Type type) {
        final Map<ListingRecordFile, Long> counts = new HashMap<>();
        ListingRecordFile best = null;
        long bestCount = 0;
        for (ListingRecordFile f : files) {
            if (f.type() != type) continue;
            long c = counts.merge(f, 1L, Long::sum);
            if (best == null || c > bestCount || (c == bestCount && f.md5Hex().compareTo(best.md5Hex()) < 0)) {
                best = f;
                bestCount = c;
            }
        }
        return best;
    }

    /**
     * Find the most common of each Sidecar index in the given list of files of the given type. Sidecar files are named
     * like "2023-04-25T17_48_38.002085562Z_01.rcd.gz" where the index is the number after the "Z_" and before the ".rcd".
     *
     * @param files the list of RecordFile objects to search
     * @return the array of the most common sidecar RecordFiles one of each index, or empty list if none found
     */
    public static ListingRecordFile[] findMostCommonSidecars(List<ListingRecordFile> files) {
        // find all sidecar indexes
        final Map<Integer, List<ListingRecordFile>> sidecarsByIndex = new HashMap<>();
        for (ListingRecordFile f : files) {
            if (f.type() != ListingRecordFile.Type.RECORD_SIDECAR) continue;
            String fileName = Path.of(f.path()).getFileName().toString();
            int idxStart = fileName.lastIndexOf("Z_");
            int idxEnd = fileName.indexOf(".rcd");
            if (idxStart < 0 || idxEnd < 0 || idxEnd <= idxStart + 2) {
                throw new IllegalArgumentException("Invalid sidecar file name: " + fileName);
            }
            String idxStr = fileName.substring(idxStart + 2, idxEnd);
            try {
                int idx = Integer.parseInt(idxStr);
                sidecarsByIndex
                        .computeIfAbsent(idx, k -> new java.util.ArrayList<>())
                        .add(f);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid sidecar file name: " + fileName, e);
            }
        }
        // create result array
        final ListingRecordFile[] result = new ListingRecordFile[sidecarsByIndex.size()];
        // for each index, find the most common sidecar
        for (Map.Entry<Integer, List<ListingRecordFile>> entry : sidecarsByIndex.entrySet()) {
            int idx = entry.getKey();
            List<ListingRecordFile> sidecars = entry.getValue();
            result[idx] = findMostCommonByType(sidecars, ListingRecordFile.Type.RECORD_SIDECAR);
        }
        return result;
    }
}
