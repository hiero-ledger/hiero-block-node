package org.hiero.block.tools.utils;

import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hiero.block.tools.commands.days.listing.ListingRecordFile;

/**
 * Utility class for extracting consensus timestamps from Hedera record file names and paths.
 */
public class RecordFileUtils {

    /**
     * Extract the record file time from a record file path.
     *
     * @param path the record file path, like "/path/to/2024-07-06T16_42_40.006863632Z.rcd.gz"
     * @return the record file time as an Instant
     */
    public static String extractRecordFileTimeStrFromPath(Path path) {
        final String fileName = path.getFileName().toString();
        return fileName.substring(0, fileName.indexOf("Z")+1);
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
            if (best == null
                    || c > bestCount
                    || (c == bestCount && f.md5Hex().compareTo(best.md5Hex()) < 0)) {
                best = f;
                bestCount = c;
            }
        }
        return best;
    }

}
