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
    /** when converting an Instant to a String, nano-of-second part always outputs this many digits */
    private static final int NANO_DIGITS_COUNT = 9;

    /**
     * Extract the record file time from a record file name.
     *
     * @param recordOrSidecarFileName the record file name, like "2024-07-06T16_42_40.006863632Z.rcd.gz" or a sidecar
     *                                file name like "2024-07-06T16_42_40.006863632Z_02.rcd.gz"
     * @return the record file time as an Instant
     */
    public static Instant extractRecordFileTime(String recordOrSidecarFileName) {
        // if a path remove all before last slash
        int lastSlash = recordOrSidecarFileName.lastIndexOf('/');
        if (lastSlash >= 0) {
            recordOrSidecarFileName = recordOrSidecarFileName.substring(lastSlash + 1);
        }
        // extract date prefix from file name, last chat is always 'Z'
        final String dateString = recordOrSidecarFileName
                .substring(0, recordOrSidecarFileName.indexOf('Z') + 1)
                .replaceAll("_", ":");
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

    /**
     * Convert an Instant to a record file name.
     *
     * @param instant the block time Instant
     * @return the record file name, like "2019-09-13T21_53_51.396440Z.rcd"
     */
    public static String instantToRecordFileName(Instant instant) {
        return convertInstantToStringWithPadding(instant) + ".rcd";
    }

    /**
     * A string representation of the Instant using ISO-8601 representation, with colons converted to underscores for
     * Windows compatibility. The nano-of-second always outputs nine digits with padding when necessary, to ensure same
     * length filenames and proper sorting. examples: input: 2020-10-19T21:35:39Z output:
     * "2020-10-19T21_35_39.000000000Z"
     * <p>
     * input: 2020-10-19T21:35:39.454265Z output: "2020-10-19T21:35:39.454265000Z"
     * <p>
     * Copied from com.swirlds.common.stream.LinkedObjectStreamUtilities#convertInstantToStringWithPadding(Instant)
     * </p>
     *
     * @param timestamp an Instant object
     * @return a string representation of the Instant, to be used as stream file name
     */
    public static String convertInstantToStringWithPadding(final Instant timestamp) {
        String string = timestamp.toString().replace(":", "_");
        StringBuilder stringBuilder = new StringBuilder(string);
        int nanoStartIdx = string.indexOf(".");
        int nanoEndIdx = string.indexOf("Z");
        int numOfNanoDigits = nanoStartIdx == -1 ? 0 : (nanoEndIdx - nanoStartIdx - 1);
        int numOfZeroPadding = NANO_DIGITS_COUNT - numOfNanoDigits;
        if (numOfZeroPadding == 0) {
            return string;
        }
        // remove the last 'Z'
        stringBuilder.setLength(stringBuilder.length() - 1);
        // if string doesn't have nano part, append '.'
        if (nanoStartIdx == -1) {
            stringBuilder.append('.');
        }
        // append numOfZeroPadding '0'(s)
        //noinspection StringRepeatCanBeUsed
        for (int i = 0; i < numOfZeroPadding; i++) {
            stringBuilder.append('0');
        }
        // append 'Z'
        stringBuilder.append('Z');
        return stringBuilder.toString();
    }

    /**
     * Extract the time string from a record file name.Examples:
     * <ul>
     *  <li>input: "recordstreams/record0.0.3/2021-02-04T19_13_04.033998000Z.rcd_sig"
     *  output: "2021-02-04T19_13_04.033998000Z"</li>
     *  <li>input: "recordstreams/record0.0.3/2021-02-04T19_13_04.033998000Z.rcd"
     *  output: "2021-02-04T19_13_04.033998000Z"</li>
     *  <li>input: "2021-02-04T19_13_04.033998000Z.rcd_sig"
     *  output: "2021-02-04T19_13_04.033998000Z"</li>
     *  <li>input: "2021-02-04T19_13_04.033998000Z.rcd"
     *  output: "2021-02-04T19_13_04.033998000Z"</li>
     * </ul>
     *
     * @param fileName the record file name, like "2024-07-06T16_42_40.006863632Z.rcd"
     * @return the time string, like "2024-07-06T16_42_40.006863632Z"
     */
    public static String timeStringFromFileNameOrPath(String fileName) {
        int lastSlash = fileName.lastIndexOf('/');
        if (lastSlash >= 0) {
            fileName = fileName.substring(lastSlash + 1);
        }
        return fileName.substring(0, fileName.indexOf(".rcd"));
    }
}
