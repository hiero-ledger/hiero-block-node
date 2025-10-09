// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;

/**
 * Utility class to store help with record file dates.
 * <p>
 *     Record file names look like "2024-07-06T16_42_40.006863632Z.rcd.gz" the first part is a date time in ISO format
 *     in UTC time zone.
 * </p>
 * <p>
 *     Block times are longs in nanoseconds since the first block after OA. They allow very condensed storage of times
 *     in the Hedera blockchain history.
 * </p>
 */
@SuppressWarnings("unused")
public final class RecordFileDates {
    /** the record file time of the first block after OA */
    public static final String FIRST_BLOCK_TIME = "2019-09-13T21_53_51.396440Z";
    /** the record file time of the first block after OA as Instant */
    public static final Instant FIRST_BLOCK_TIME_INSTANT = Instant.parse(FIRST_BLOCK_TIME.replace('_', ':'));
    /** one hour in nanoseconds */
    public static final long ONE_HOUR = Duration.ofHours(1).toNanos();
    /** one day in nanoseconds */
    public static final long ONE_DAY = Duration.ofDays(1).toNanos();

    /**
     * Extract the record file time from a record file name.
     *
     * @param recordOrSidecarFileName the record file name, like "2024-07-06T16_42_40.006863632Z.rcd.gz" or a sidecar
     *                                file name like "2024-07-06T16_42_40.006863632Z_02.rcd.gz"
     * @return the record file time as an Instant
     */
    public static Instant extractRecordFileTime(String recordOrSidecarFileName) {
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
            return Instant.parse(dateString);
        } catch (DateTimeParseException e) {
            throw new RuntimeException(
                    "Invalid record file name: \"" + recordOrSidecarFileName + "\" - dateString=\"" + dateString + "\"",
                    e);
        }
    }

    /**
     * Convert a block time long to an instant.
     *
     * @param blockTime the block time in nanoseconds since the first block after OA
     * @return the block time instant
     */
    public static Instant blockTimeLongToInstant(long blockTime) {
        return FIRST_BLOCK_TIME_INSTANT.plusNanos(blockTime);
    }

    /**
     * Convert an instant to a block time long.
     *
     * @param instant the instant
     * @return the block time in nanoseconds since the first block after OA
     */
    public static long instantToBlockTimeLong(Instant instant) {
        return Duration.between(FIRST_BLOCK_TIME_INSTANT, instant).toNanos();
    }

    /**
     * Convert an instant in time to a block time long.
     *
     * @param dateTime the time instant
     * @return the block time in nanoseconds since the first block after OA
     */
    public static long blockTimeInstantToLong(Instant dateTime) {
        return Duration.between(FIRST_BLOCK_TIME_INSTANT, dateTime).toNanos();
    }

    /**
     * Convert a record file name to a block time long.
     *
     * @param recordFileName the record file name, like "2024-07-06T16_42_40.006863632Z.rcd.gz"
     * @return the block time in nanoseconds since the first block after OA
     */
    public static long recordFileNameToBlockTimeLong(String recordFileName) {
        return blockTimeInstantToLong(extractRecordFileTime(recordFileName));
    }

    /**
     * Convert a block time long to a record file prefix string.
     *
     * @param blockTime the block time in nanoseconds since the first block after OA
     * @return the record file prefix string, like "2019-09-13T21_53_51.39644"
     */
    public static String blockTimeLongToRecordFilePrefix(long blockTime) {
        String blockTimeString = blockTimeLongToInstant(blockTime).toString().replace(':', '_');
        // remove the 'Z' at the end
        blockTimeString = blockTimeString.substring(0, blockTimeString.length() - 1);
        return blockTimeString;
    }
}
