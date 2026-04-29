// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.archive;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/// Unit tests for [ArchiveKey].
class ArchiveKeyTest {

    static Stream<Arguments> formatCases() {
        return Stream.of(
                Arguments.of(0L, 1, "0000/0000/0000/0000/0.tar"),
                Arguments.of(10L, 1, "0000/0000/0000/0000/1.tar"),
                Arguments.of(1230L, 1, "0000/0000/0000/0001/23.tar"),
                Arguments.of(100L, 2, "0000/0000/0000/0000/1.tar"),
                Arguments.of(12300L, 2, "0000/0000/0000/0012/3.tar"),
                Arguments.of(1000L, 3, "0000/0000/0000/1.tar"),
                Arguments.of(123000L, 3, "0000/0000/0000/123.tar"),
                Arguments.of(9999000L, 3, "0000/0000/0000/9999.tar"),
                Arguments.of(56789000L, 3, "0000/0000/0005/6789.tar"),
                Arguments.of(10000L, 4, "0000/0000/0000/1.tar"),
                Arguments.of(400000L, 4, "0000/0000/0000/40.tar"),
                Arguments.of(12340000L, 4, "0000/0000/0001/234.tar"),
                Arguments.of(154000000L, 4, "0000/0000/0015/400.tar"),
                Arguments.of(100000L, 5, "0000/0000/0000/1.tar"),
                Arguments.of(3400000L, 5, "0000/0000/0000/34.tar"),
                Arguments.of(1234500000L, 5, "0000/0000/0123/45.tar"),
                Arguments.of(1000000L, 6, "0000/0000/0000/1.tar"),
                Arguments.of(56000000L, 6, "0000/0000/0005/6.tar"),
                Arguments.of(1234000000L, 6, "0000/0000/0123/4.tar"));
    }

    @ParameterizedTest(name = "groupStart={0}, level={1}")
    @MethodSource("formatCases")
    @DisplayName("format produces expected S3 key")
    void formatProducesExpectedKey(long groupStart, int groupingLevel, String expectedKey) {
        assertThat(ArchiveKey.format(groupStart, groupingLevel)).isEqualTo(expectedKey);
    }

    static Stream<Arguments> parseCases() {
        return Stream.of(
                Arguments.of("0000/0000/0000/0000/0.tar", 1, 0L),
                Arguments.of("0000/0000/0000/0000/1.tar", 1, 10L),
                Arguments.of("0000/0000/0000/0001/23.tar", 1, 1230L),
                Arguments.of("0000/0000/0000/0000/1.tar", 2, 100L),
                Arguments.of("0000/0000/0000/0012/3.tar", 2, 12300L),
                Arguments.of("0000/0000/0000/1.tar", 3, 1000L),
                Arguments.of("0000/0000/0000/123.tar", 3, 123000L),
                Arguments.of("0000/0000/0000/9999.tar", 3, 9999000L),
                Arguments.of("0000/0000/0005/6789.tar", 3, 56789000L),
                Arguments.of("0000/0000/0000/1.tar", 4, 10000L),
                Arguments.of("0000/0000/0000/40.tar", 4, 400000L),
                Arguments.of("0000/0000/0001/234.tar", 4, 12340000L),
                Arguments.of("0000/0000/0015/400.tar", 4, 154000000L),
                Arguments.of("0000/0000/0000/1.tar", 5, 100000L),
                Arguments.of("0000/0000/0000/34.tar", 5, 3400000L),
                Arguments.of("0000/0000/0123/45.tar", 5, 1234500000L),
                Arguments.of("0000/0000/0000/1.tar", 6, 1000000L),
                Arguments.of("0000/0000/0005/6.tar", 6, 56000000L),
                Arguments.of("0000/0000/0123/4.tar", 6, 1234000000L));
    }

    @ParameterizedTest(name = "key={0}, level={1}")
    @MethodSource("parseCases")
    @DisplayName("parse recovers group start from S3 key")
    void parseRecoversGroupStart(String key, int groupingLevel, long expectedGroupStart) {
        assertThat(ArchiveKey.parse(key, groupingLevel)).isEqualTo(expectedGroupStart);
    }
}
