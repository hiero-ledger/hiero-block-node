// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RecordFileDates}.
 */
public class RecordFileDatesTest {

    // ===== Constants Tests =====

    @Test
    void testFirstBlockTimeConstant() {
        assertEquals("2019-09-13T21_53_51.396440Z", RecordFileDates.FIRST_BLOCK_TIME);
    }

    @Test
    void testFirstBlockTimeInstantConstant() {
        Instant expected = Instant.parse("2019-09-13T21:53:51.396440Z");
        assertEquals(expected, RecordFileDates.FIRST_BLOCK_TIME_INSTANT);
    }

    @Test
    void testOneHourConstant() {
        assertEquals(Duration.ofHours(1).toNanos(), RecordFileDates.ONE_HOUR);
        assertEquals(3_600_000_000_000L, RecordFileDates.ONE_HOUR);
    }

    @Test
    void testOneDayConstant() {
        assertEquals(Duration.ofDays(1).toNanos(), RecordFileDates.ONE_DAY);
        assertEquals(86_400_000_000_000L, RecordFileDates.ONE_DAY);
    }

    // ===== extractRecordFileTime Tests =====

    @Test
    void testExtractRecordFileTimeFromStandardRecordFile() {
        String recordFileName = "2024-07-06T16_42_40.006863632Z.rcd.gz";
        Instant expected = Instant.parse("2024-07-06T16:42:40.006863632Z");
        assertEquals(expected, RecordFileDates.extractRecordFileTime(recordFileName));
    }

    @Test
    void testExtractRecordFileTimeFromUncompressedRecordFile() {
        String recordFileName = "2024-07-06T16_42_40.006863632Z.rcd";
        Instant expected = Instant.parse("2024-07-06T16:42:40.006863632Z");
        assertEquals(expected, RecordFileDates.extractRecordFileTime(recordFileName));
    }

    @Test
    void testExtractRecordFileTimeFromSidecarFile() {
        String sidecarFileName = "2024-07-06T16_42_40.006863632Z_02.rcd.gz";
        Instant expected = Instant.parse("2024-07-06T16:42:40.006863632Z");
        assertEquals(expected, RecordFileDates.extractRecordFileTime(sidecarFileName));
    }

    @Test
    void testExtractRecordFileTimeFromSidecarFileWithDifferentNumbers() {
        String sidecarFileName = "2021-02-04T19_13_04.033998000Z_15.rcd";
        Instant expected = Instant.parse("2021-02-04T19:13:04.033998000Z");
        assertEquals(expected, RecordFileDates.extractRecordFileTime(sidecarFileName));
    }

    @Test
    void testExtractRecordFileTimeFromFirstBlockTime() {
        String recordFileName = "2019-09-13T21_53_51.396440Z.rcd";
        assertEquals(RecordFileDates.FIRST_BLOCK_TIME_INSTANT, RecordFileDates.extractRecordFileTime(recordFileName));
    }

    @Test
    void testExtractRecordFileTimeInvalidFileName() {
        String invalidFileName = "invalid-file-name.rcd";
        RuntimeException exception =
                assertThrows(RuntimeException.class, () -> RecordFileDates.extractRecordFileTime(invalidFileName));
        assertTrue(exception.getMessage().contains("Invalid record file name"));
    }

    // ===== blockTimeLongToInstant Tests =====

    @Test
    void testBlockTimeLongToInstantZero() {
        assertEquals(RecordFileDates.FIRST_BLOCK_TIME_INSTANT, RecordFileDates.blockTimeLongToInstant(0));
    }

    @Test
    void testBlockTimeLongToInstantOneHour() {
        Instant expected = RecordFileDates.FIRST_BLOCK_TIME_INSTANT.plusNanos(RecordFileDates.ONE_HOUR);
        assertEquals(expected, RecordFileDates.blockTimeLongToInstant(RecordFileDates.ONE_HOUR));
    }

    @Test
    void testBlockTimeLongToInstantOneDay() {
        Instant expected = RecordFileDates.FIRST_BLOCK_TIME_INSTANT.plusNanos(RecordFileDates.ONE_DAY);
        assertEquals(expected, RecordFileDates.blockTimeLongToInstant(RecordFileDates.ONE_DAY));
    }

    @Test
    void testBlockTimeLongToInstantLargeValue() {
        // Roughly 5 years in nanoseconds
        long fiveYearsNanos = Duration.ofDays(365 * 5).toNanos();
        Instant result = RecordFileDates.blockTimeLongToInstant(fiveYearsNanos);
        Instant expected = RecordFileDates.FIRST_BLOCK_TIME_INSTANT.plusNanos(fiveYearsNanos);
        assertEquals(expected, result);
    }

    // ===== instantToBlockTimeLong Tests =====

    @Test
    void testInstantToBlockTimeLongAtFirstBlock() {
        assertEquals(0, RecordFileDates.instantToBlockTimeLong(RecordFileDates.FIRST_BLOCK_TIME_INSTANT));
    }

    @Test
    void testInstantToBlockTimeLongOneHourAfter() {
        Instant oneHourAfter = RecordFileDates.FIRST_BLOCK_TIME_INSTANT.plusNanos(RecordFileDates.ONE_HOUR);
        assertEquals(RecordFileDates.ONE_HOUR, RecordFileDates.instantToBlockTimeLong(oneHourAfter));
    }

    @Test
    void testInstantToBlockTimeLongOneDayAfter() {
        Instant oneDayAfter = RecordFileDates.FIRST_BLOCK_TIME_INSTANT.plusNanos(RecordFileDates.ONE_DAY);
        assertEquals(RecordFileDates.ONE_DAY, RecordFileDates.instantToBlockTimeLong(oneDayAfter));
    }

    // ===== blockTimeInstantToLong Tests =====

    @Test
    void testBlockTimeInstantToLongAtFirstBlock() {
        assertEquals(0, RecordFileDates.blockTimeInstantToLong(RecordFileDates.FIRST_BLOCK_TIME_INSTANT));
    }

    @Test
    void testBlockTimeInstantToLongOneHourAfter() {
        Instant oneHourAfter = RecordFileDates.FIRST_BLOCK_TIME_INSTANT.plusNanos(RecordFileDates.ONE_HOUR);
        assertEquals(RecordFileDates.ONE_HOUR, RecordFileDates.blockTimeInstantToLong(oneHourAfter));
    }

    @Test
    void testBlockTimeInstantToLongMatchesInstantToBlockTimeLong() {
        Instant testInstant = Instant.parse("2024-01-01T00:00:00Z");
        assertEquals(
                RecordFileDates.instantToBlockTimeLong(testInstant),
                RecordFileDates.blockTimeInstantToLong(testInstant));
    }

    // ===== recordFileNameToBlockTimeLong Tests =====

    @Test
    void testRecordFileNameToBlockTimeLongFirstBlock() {
        String recordFileName = "2019-09-13T21_53_51.396440Z.rcd";
        assertEquals(0, RecordFileDates.recordFileNameToBlockTimeLong(recordFileName));
    }

    @Test
    void testRecordFileNameToBlockTimeLongLaterBlock() {
        String recordFileName = "2024-07-06T16_42_40.006863632Z.rcd.gz";
        Instant fileTime = Instant.parse("2024-07-06T16:42:40.006863632Z");
        long expected = Duration.between(RecordFileDates.FIRST_BLOCK_TIME_INSTANT, fileTime)
                .toNanos();
        assertEquals(expected, RecordFileDates.recordFileNameToBlockTimeLong(recordFileName));
    }

    // ===== blockTimeLongToRecordFilePrefix Tests =====

    @Test
    void testBlockTimeLongToRecordFilePrefixZero() {
        String prefix = RecordFileDates.blockTimeLongToRecordFilePrefix(0);
        // The original FIRST_BLOCK_TIME is "2019-09-13T21_53_51.396440Z", minus the 'Z'
        assertEquals("2019-09-13T21_53_51.396440", prefix);
    }

    @Test
    void testBlockTimeLongToRecordFilePrefixOneHour() {
        String prefix = RecordFileDates.blockTimeLongToRecordFilePrefix(RecordFileDates.ONE_HOUR);
        Instant expectedInstant = RecordFileDates.FIRST_BLOCK_TIME_INSTANT.plusNanos(RecordFileDates.ONE_HOUR);
        String expectedString = expectedInstant.toString().replace(':', '_');
        // Remove the trailing 'Z'
        expectedString = expectedString.substring(0, expectedString.length() - 1);
        assertEquals(expectedString, prefix);
    }

    @Test
    void testBlockTimeLongToRecordFilePrefixDoesNotContainZ() {
        String prefix = RecordFileDates.blockTimeLongToRecordFilePrefix(RecordFileDates.ONE_DAY);
        assertFalse(prefix.endsWith("Z"), "Prefix should not end with Z");
    }

    // ===== instantToRecordFileName Tests =====

    @Test
    void testInstantToRecordFileNameWithNanos() {
        Instant instant = Instant.parse("2024-07-06T16:42:40.006863632Z");
        String expected = "2024-07-06T16_42_40.006863632Z.rcd";
        assertEquals(expected, RecordFileDates.instantToRecordFileName(instant));
    }

    @Test
    void testInstantToRecordFileNameWithoutNanos() {
        Instant instant = Instant.parse("2020-10-19T21:35:39Z");
        String expected = "2020-10-19T21_35_39.000000000Z.rcd";
        assertEquals(expected, RecordFileDates.instantToRecordFileName(instant));
    }

    @Test
    void testInstantToRecordFileNameWithPartialNanos() {
        Instant instant = Instant.parse("2020-10-19T21:35:39.454265Z");
        String expected = "2020-10-19T21_35_39.454265000Z.rcd";
        assertEquals(expected, RecordFileDates.instantToRecordFileName(instant));
    }

    // ===== convertInstantToStringWithPadding Tests =====

    @Test
    void testConvertInstantToStringWithPaddingNoNanos() {
        Instant instant = Instant.parse("2020-10-19T21:35:39Z");
        String expected = "2020-10-19T21_35_39.000000000Z";
        assertEquals(expected, RecordFileDates.convertInstantToStringWithPadding(instant));
    }

    @Test
    void testConvertInstantToStringWithPaddingPartialNanos() {
        Instant instant = Instant.parse("2020-10-19T21:35:39.454265Z");
        String expected = "2020-10-19T21_35_39.454265000Z";
        assertEquals(expected, RecordFileDates.convertInstantToStringWithPadding(instant));
    }

    @Test
    void testConvertInstantToStringWithPaddingFullNanos() {
        Instant instant = Instant.parse("2024-07-06T16:42:40.006863632Z");
        String expected = "2024-07-06T16_42_40.006863632Z";
        assertEquals(expected, RecordFileDates.convertInstantToStringWithPadding(instant));
    }

    @Test
    void testConvertInstantToStringWithPaddingColonsReplacedWithUnderscores() {
        Instant instant = Instant.parse("2020-10-19T21:35:39.123456789Z");
        String result = RecordFileDates.convertInstantToStringWithPadding(instant);
        assertFalse(result.contains(":"), "Result should not contain colons");
        assertTrue(result.contains("_"), "Result should contain underscores");
    }

    // ===== timeStringFromFileNameOrPath Tests =====

    @Test
    void testTimeStringFromFileNameWithPath() {
        String path = "recordstreams/record0.0.3/2021-02-04T19_13_04.033998000Z.rcd_sig";
        String expected = "2021-02-04T19_13_04.033998000Z";
        assertEquals(expected, RecordFileDates.timeStringFromFileNameOrPath(path));
    }

    @Test
    void testTimeStringFromFileNameWithPathRcd() {
        String path = "recordstreams/record0.0.3/2021-02-04T19_13_04.033998000Z.rcd";
        String expected = "2021-02-04T19_13_04.033998000Z";
        assertEquals(expected, RecordFileDates.timeStringFromFileNameOrPath(path));
    }

    @Test
    void testTimeStringFromFileNameOnly() {
        String fileName = "2021-02-04T19_13_04.033998000Z.rcd_sig";
        String expected = "2021-02-04T19_13_04.033998000Z";
        assertEquals(expected, RecordFileDates.timeStringFromFileNameOrPath(fileName));
    }

    @Test
    void testTimeStringFromFileNameOnlyRcd() {
        String fileName = "2021-02-04T19_13_04.033998000Z.rcd";
        String expected = "2021-02-04T19_13_04.033998000Z";
        assertEquals(expected, RecordFileDates.timeStringFromFileNameOrPath(fileName));
    }

    @Test
    void testTimeStringFromFileNamePreservesUnderscores() {
        String fileName = "2024-07-06T16_42_40.006863632Z.rcd";
        String result = RecordFileDates.timeStringFromFileNameOrPath(fileName);
        assertTrue(result.contains("_"), "Result should contain underscores");
        assertFalse(result.contains(":"), "Result should not contain colons");
        assertEquals("2024-07-06T16_42_40.006863632Z", result);
    }

    // ===== Round-trip Tests =====

    @Test
    void testRoundTripBlockTimeLongToInstantAndBack() {
        long originalBlockTime = 123456789012345L;
        Instant instant = RecordFileDates.blockTimeLongToInstant(originalBlockTime);
        long roundTripped = RecordFileDates.instantToBlockTimeLong(instant);
        assertEquals(originalBlockTime, roundTripped);
    }

    @Test
    void testRoundTripInstantToBlockTimeLongAndBack() {
        Instant originalInstant = Instant.parse("2024-01-15T12:30:45.123456789Z");
        long blockTime = RecordFileDates.instantToBlockTimeLong(originalInstant);
        Instant roundTripped = RecordFileDates.blockTimeLongToInstant(blockTime);
        assertEquals(originalInstant, roundTripped);
    }

    @Test
    void testRoundTripRecordFileNameExtractAndRecreate() {
        String originalFileName = "2024-07-06T16_42_40.006863632Z.rcd";
        Instant extracted = RecordFileDates.extractRecordFileTime(originalFileName);
        String recreated = RecordFileDates.instantToRecordFileName(extracted);
        assertEquals(originalFileName, recreated);
    }
}
