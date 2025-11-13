// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.tools.commands.days.listing.ListingRecordFile;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RecordFileUtils#findMostCommonSidecars(List)}.
 */
public class FindMostCommonSidecarsTest {

    private static final String MD5_A = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    private static final String MD5_B = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    private static final String MD5_C = "cccccccccccccccccccccccccccccccc";

    private ListingRecordFile makeSidecar(String path, int size, String md5) {
        final LocalDateTime ts = RecordFileUtils.extractRecordFileTimeFromPath(path);
        return new ListingRecordFile(path, ts, size, md5);
    }

    @Test
    void returnsEmptyForNoFiles() {
        final ListingRecordFile[] result = RecordFileUtils.findMostCommonSidecars(List.of());
        assertEquals(0, result.length, "Expected empty array when no sidecar files provided");
    }

    @Test
    void singleIndexSingleFile() {
        final String p = "some/sidecar/2025-01-01T00_00_00.000000000Z_01.rcd";
        final ListingRecordFile f = makeSidecar(p, 100, MD5_A);
        final ListingRecordFile[] result = RecordFileUtils.findMostCommonSidecars(List.of(f));
        assertEquals(1, result.length);
        assertEquals(MD5_A, result[0].md5Hex());
    }

    @Test
    void twoIndexesSingleFileEach() {
        final ListingRecordFile f1 = makeSidecar("some/sidecar/2025-01-01T00_00_00.000000000Z_01.rcd", 100, MD5_A);
        final ListingRecordFile f2 = makeSidecar("some/sidecar/2025-01-01T00_00_00.000000000Z_02.rcd", 120, MD5_B);
        final ListingRecordFile[] result = RecordFileUtils.findMostCommonSidecars(List.of(f1, f2));
        assertEquals(2, result.length);
        assertEquals(MD5_A, result[0].md5Hex());
        assertEquals(MD5_B, result[1].md5Hex());
    }

    @Test
    void majorityWithOutliers() {
        // index 1: three with MD5_A, two outliers MD5_B
        final List<ListingRecordFile> files = new ArrayList<>();
        files.add(makeSidecar("some/sidecar/2025-01-01T00_00_00.000000000Z_01.rcd", 100, MD5_A));
        files.add(makeSidecar("some/sidecar/2025-01-01T00_00_00.000000001Z_01.rcd", 101, MD5_A));
        files.add(makeSidecar("some/sidecar/2025-01-01T00_00_00.000000002Z_01.rcd", 102, MD5_A));
        files.add(makeSidecar("some/sidecar/2025-01-01T00_00_00.000000003Z_01.rcd", 103, MD5_B));
        files.add(makeSidecar("some/sidecar/2025-01-01T00_00_00.000000004Z_01.rcd", 104, MD5_B));

        // index 2: two with MD5_B, one outlier MD5_C
        files.add(makeSidecar("some/sidecar/2025-01-01T00_00_00.000000000Z_02.rcd", 200, MD5_B));
        files.add(makeSidecar("some/sidecar/2025-01-01T00_00_00.000000001Z_02.rcd", 201, MD5_B));
        files.add(makeSidecar("some/sidecar/2025-01-01T00_00_00.000000002Z_02.rcd", 202, MD5_C));

        final ListingRecordFile[] result = RecordFileUtils.findMostCommonSidecars(files);
        assertEquals(2, result.length, "Should have entries for indexes 1 and 2");
        assertEquals(MD5_A, result[0].md5Hex(), "Index 1 should pick majority MD5_A");
        assertEquals(MD5_B, result[1].md5Hex(), "Index 2 should pick majority MD5_B");
    }

    @Test
    void missingLowerIndexProducesException() {
        // only index 2 present
        final ListingRecordFile onlyIndex2 =
                makeSidecar("some/sidecar/2025-01-01T00_00_00.000000000Z_02.rcd", 123, MD5_C);
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> RecordFileUtils.findMostCommonSidecars(List.of(onlyIndex2)),
                "Expected IllegalArgumentException when sidecar indexes are missing");
        assertTrue(ex.getMessage().contains("Missing sidecar indexes"));
    }
}
