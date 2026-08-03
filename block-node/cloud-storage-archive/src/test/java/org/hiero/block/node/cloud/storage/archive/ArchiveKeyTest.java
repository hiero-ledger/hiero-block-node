// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.archive;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/// Unit tests for [ArchiveKey].
class ArchiveKeyTest {

    static Stream<Arguments> formatCases() {
        return Stream.of(
                Arguments.of(0L, 1, "", "0000/0000/0000/0000/0.tar"),
                Arguments.of(10L, 1, "", "0000/0000/0000/0000/1.tar"),
                Arguments.of(1230L, 1, "", "0000/0000/0000/0001/23.tar"),
                Arguments.of(100L, 2, "", "0000/0000/0000/0000/1.tar"),
                Arguments.of(12300L, 2, "", "0000/0000/0000/0012/3.tar"),
                Arguments.of(1000L, 3, "", "0000/0000/0000/1.tar"),
                Arguments.of(123000L, 3, "", "0000/0000/0000/123.tar"),
                Arguments.of(9999000L, 3, "", "0000/0000/0000/9999.tar"),
                Arguments.of(56789000L, 3, "", "0000/0000/0005/6789.tar"),
                Arguments.of(10000L, 4, "", "0000/0000/0000/1.tar"),
                Arguments.of(400000L, 4, "", "0000/0000/0000/40.tar"),
                Arguments.of(12340000L, 4, "", "0000/0000/0001/234.tar"),
                Arguments.of(154000000L, 4, "", "0000/0000/0015/400.tar"),
                Arguments.of(100000L, 5, "", "0000/0000/0000/1.tar"),
                Arguments.of(3400000L, 5, "", "0000/0000/0000/34.tar"),
                Arguments.of(1234500000L, 5, "", "0000/0000/0123/45.tar"),
                Arguments.of(1000000L, 6, "", "0000/0000/0000/1.tar"),
                Arguments.of(56000000L, 6, "", "0000/0000/0005/6.tar"),
                Arguments.of(1234000000L, 6, "", "0000/0000/0123/4.tar"),
                Arguments.of(0L, 1, "myblocks", "myblocks/0000/0000/0000/0000/0.tar"),
                Arguments.of(1230L, 1, "hiero/mainnet", "hiero/mainnet/0000/0000/0000/0001/23.tar"));
    }

    @ParameterizedTest(name = "groupStart={0}, level={1}, prefix=\"{2}\"")
    @MethodSource("formatCases")
    @DisplayName("format produces expected S3 key")
    void formatProducesExpectedKey(long groupStart, int groupingLevel, String prefix, String expectedKey) {
        assertThat(ArchiveKey.format(groupStart, groupingLevel, prefix)).isEqualTo(expectedKey);
    }

    static Stream<Arguments> parseCases() {
        return Stream.of(
                Arguments.of("0000/0000/0000/0000/0.tar", 1, "", 0L),
                Arguments.of("0000/0000/0000/0000/1.tar", 1, "", 10L),
                Arguments.of("0000/0000/0000/0001/23.tar", 1, "", 1230L),
                Arguments.of("0000/0000/0000/0000/1.tar", 2, "", 100L),
                Arguments.of("0000/0000/0000/0012/3.tar", 2, "", 12300L),
                Arguments.of("0000/0000/0000/1.tar", 3, "", 1000L),
                Arguments.of("0000/0000/0000/123.tar", 3, "", 123000L),
                Arguments.of("0000/0000/0000/9999.tar", 3, "", 9999000L),
                Arguments.of("0000/0000/0005/6789.tar", 3, "", 56789000L),
                Arguments.of("0000/0000/0000/1.tar", 4, "", 10000L),
                Arguments.of("0000/0000/0000/40.tar", 4, "", 400000L),
                Arguments.of("0000/0000/0001/234.tar", 4, "", 12340000L),
                Arguments.of("0000/0000/0015/400.tar", 4, "", 154000000L),
                Arguments.of("0000/0000/0000/1.tar", 5, "", 100000L),
                Arguments.of("0000/0000/0000/34.tar", 5, "", 3400000L),
                Arguments.of("0000/0000/0123/45.tar", 5, "", 1234500000L),
                Arguments.of("0000/0000/0000/1.tar", 6, "", 1000000L),
                Arguments.of("0000/0000/0005/6.tar", 6, "", 56000000L),
                Arguments.of("0000/0000/0123/4.tar", 6, "", 1234000000L),
                Arguments.of("myblocks/0000/0000/0000/0000/0.tar", 1, "myblocks", 0L),
                Arguments.of("hiero/mainnet/0000/0000/0000/0001/23.tar", 1, "hiero/mainnet", 1230L));
    }

    @ParameterizedTest(name = "key={0}, level={1}, prefix=\"{2}\"")
    @MethodSource("parseCases")
    @DisplayName("parse recovers group start from S3 key")
    void parseRecoversGroupStart(String key, int groupingLevel, String prefix, long expectedGroupStart) {
        assertThat(ArchiveKey.parse(key, groupingLevel, prefix)).isEqualTo(expectedGroupStart);
    }

    /// Every prefix shape combined with every grouping level by the parameterized sources below.
    private static final List<String> PREFIXES = List.of("", "myblocks", "hiero/mainnet");

    /// Malformed block paths: each violates exactly one aspect of the key format, while the
    /// grouping level and prefix arguments stay valid. Combined with every grouping level and
    /// every prefix shape by [#unparseableKeys].
    private static final List<String> MALFORMED_BLOCK_PATHS = List.of(
            // The malformed key from issue 3282, an object created in the bucket by another tool.
            "2026-05-01_21-32-38_0000000000000116700-0000000000000116799",
            "2026-05-01_21-32-38_0000000000000116700-0000000000000116799.tar",
            // Valid directory segments with a foreign object name in the leaf directory.
            "0000/0000/0000/0000/2026-05-01_21-32-38_0000000000000116700.tar",
            // Non numeric first, middle and last segment (all other segments valid).
            "abcd/0000/0000/0001/23.tar",
            "0000/abcd/0000/0001/23.tar",
            "0000/0000/0000/0001/xyz.tar",
            // Whitespace and decimal in an otherwise valid last segment.
            "0000/0000/0000/0001/2 3.tar",
            "0000/0000/0000/0001/2.5.tar",
            // Negative numbers, in a structured key and in a bare key.
            "0000/0000/0000/0001/-23.tar",
            "-5.tar",
            // Overflows a long.
            "99999999999999999999.tar",
            // Valid digits but the wrong digit count for any grouping level.
            "123.tar",
            "0000/0000/0000/0000/0000/1.tar",
            // Empty key and suffix only.
            "",
            ".tar");

    static Stream<Arguments> unparseableKeys() {
        return PREFIXES.stream()
                .flatMap(prefix -> IntStream.rangeClosed(1, 6).boxed().flatMap(level -> MALFORMED_BLOCK_PATHS.stream()
                        .map(path -> Arguments.of(prefix.isEmpty() ? path : prefix + "/" + path, level, prefix))));
    }

    /// The bucket may contain foreign objects created by other tools. Parsing their keys must
    /// not throw (issue #3282); the sentinel tells the caller to skip the key. Every malformed
    /// path is exercised under every grouping level and every prefix shape.
    @ParameterizedTest(name = "key={0}, level={1}, prefix=\"{2}\"")
    @MethodSource("unparseableKeys")
    @DisplayName("parse returns UNPARSEABLE for keys that do not conform to the archive key format")
    void parseReturnsUnparseableForForeignKeys(String key, int groupingLevel, String prefix) {
        assertThat(ArchiveKey.parse(key, groupingLevel, prefix)).isEqualTo(ArchiveKey.UNPARSEABLE);
    }

    static Stream<Arguments> prefixMismatchKeys() {
        return Stream.of(
                // A different prefix entirely.
                Arguments.of("other/0000/0000/0000/0000/0.tar", 1, "hiero/mainnet"),
                // No prefix at all while one is configured.
                Arguments.of("0000/0000/0000/0000/0.tar", 1, "myblocks"),
                // The configured prefix as a proper string prefix, but not on a `/` boundary.
                Arguments.of("myblocksextra/0000/0000/0000/0000/0.tar", 1, "myblocks"),
                // Only the first part of a nested prefix.
                Arguments.of("hiero/0000/0000/0000/0000/0.tar", 1, "hiero/mainnet"),
                // The bare prefix with no key below it.
                Arguments.of("hiero/mainnet", 1, "hiero/mainnet"));
    }

    /// A well formed key under the wrong prefix must not throw either: it is simply not one of
    /// this node's archive keys (issue #3282). The key and grouping level stay valid; only the
    /// prefix relationship is broken.
    @ParameterizedTest(name = "key={0}, level={1}, prefix=\"{2}\"")
    @MethodSource("prefixMismatchKeys")
    @DisplayName("parse returns UNPARSEABLE when the key does not start with the configured prefix")
    void parseReturnsUnparseableWhenKeyDoesNotStartWithPrefix(String key, int groupingLevel, String prefix) {
        assertThat(ArchiveKey.parse(key, groupingLevel, prefix)).isEqualTo(ArchiveKey.UNPARSEABLE);
    }

    /// Group start multipliers exercised by the round trip test; each is multiplied by the
    /// group size of the level under test so the group boundary invariant holds.
    private static final List<Long> GROUP_MULTIPLIERS = List.of(0L, 1L, 9L, 10L, 123L, 9_999L, 1_234_567L);

    static Stream<Arguments> roundTripCases() {
        return PREFIXES.stream()
                .flatMap(prefix -> IntStream.rangeClosed(1, 6).boxed().flatMap(level -> GROUP_MULTIPLIERS.stream()
                        .map(multiplier -> Arguments.of(multiplier * Math.powExact(10, level), level, prefix))));
    }

    /// Exhaustive combination of every grouping level, every prefix shape and a spread of group
    /// starts: whatever [ArchiveKey#format] produces, [ArchiveKey#parse] must recover exactly.
    @ParameterizedTest(name = "groupStart={0}, level={1}, prefix=\"{2}\"")
    @MethodSource("roundTripCases")
    @DisplayName("parse(format(groupStart)) round trips for every level and prefix combination")
    void formatParseRoundTrip(long groupStart, int level, String prefix) {
        assertThat(ArchiveKey.parse(ArchiveKey.format(groupStart, level, prefix), level, prefix))
                .isEqualTo(groupStart);
    }
}
