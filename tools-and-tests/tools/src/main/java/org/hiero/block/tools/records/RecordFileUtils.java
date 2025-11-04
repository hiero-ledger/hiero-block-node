// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records;

import static java.time.ZoneOffset.UTC;

import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
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
    /** Pattern to match record file names with timestamp */
    private static final Pattern RECORD_FILE_NAME_PATTERN =
            Pattern.compile("(\\d{4}-\\d{2}-\\d{2}T\\d{2}_\\d{2}_\\d{2}\\.\\d+Z)");

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
            return LocalDateTime.ofInstant(Instant.parse(dateString), UTC);
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
     * Find the most common of each Sidecar index in the given list of files of the given type.
     * Sidecar files are named like "2023-04-25T17_48_38.002085562Z_01.rcd.gz" where the index is the
     * number after the "Z_" and before the ".rcd". Sidecar indexes are assumed to be sequential starting from 1.
     *
     * <p>The method returns a zero-based array where element 0 corresponds to the most common sidecar for
     * sidecar index 1, element 1 corresponds to sidecar index 2, and so on. The array length equals the maximum
     * sidecar index found among the provided files. If no sidecar files are found, an empty array is returned.
     *
     * <p>"Most common" is determined by counting occurrences of files (by ListingRecordFile equality) for each
     * index and selecting the one with the largest count. In case of a tie, the file with the lowest MD5 hex
     * string (lexicographically) is selected.
     *
     * <p>If there are missing indexes between 1 and the maximum index, the method will throw an
     * {@link IllegalArgumentException} listing the missing and found indexes. The rationale is that sidecar
     * indexes must start at 1 and be contiguous up to the maximum; gaps typically indicate incomplete or
     * inconsistent listings and should be handled by the caller.
     *
     * @param files the list of RecordFile objects to search
     * @return a zero-based array of the most common sidecar RecordFiles, where index 0 is for sidecar 1; empty
     *         array if none found
     * @throws IllegalArgumentException if a sidecar file name cannot be parsed or if there are missing indexes
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
        if (sidecarsByIndex.isEmpty()) {
            return new ListingRecordFile[0];
        }
        // Determine the maximum index so we can create a zero-based array where index N maps to array[N-1]
        int maxIndex = sidecarsByIndex.keySet().stream().mapToInt(Integer::intValue).max().orElse(0);
        // If there are missing indexes between 1 and maxIndex, that's an unexpected gap; fail fast with helpful message
        final List<Integer> missingIndexes = new java.util.ArrayList<>();
        for (int i = 1; i <= maxIndex; i++) {
            if (!sidecarsByIndex.containsKey(i)) {
                missingIndexes.add(i);
            }
        }
        if (!missingIndexes.isEmpty()) {
            throw new IllegalArgumentException(
                    "Missing sidecar indexes " + missingIndexes + "; found indexes " + sidecarsByIndex.keySet()
                            + ". Sidecar indexes must start at 1 and be contiguous up to the maximum index.");
        }
        final ListingRecordFile[] result = new ListingRecordFile[maxIndex];
        // for each index, find the most common sidecar and place it at position (index - 1)
        for (Map.Entry<Integer, List<ListingRecordFile>> entry : sidecarsByIndex.entrySet()) {
            int idx = entry.getKey();
            List<ListingRecordFile> sidecars = entry.getValue();
            ListingRecordFile mostCommon = findMostCommonByType(sidecars, ListingRecordFile.Type.RECORD_SIDECAR);
            if (mostCommon != null) {
                result[idx - 1] = mostCommon;
            }
        }
        return result;
    }
}
