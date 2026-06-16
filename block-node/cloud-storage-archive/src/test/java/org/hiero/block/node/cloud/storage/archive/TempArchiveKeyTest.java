// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.archive;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/// Unit tests for [TempArchiveKey] key formatting, parsing, and classification.
@DisplayName("TempArchiveKey Tests")
class TempArchiveKeyTest {

    @Nested
    @DisplayName("tmpPrefix")
    final class TmpPrefixTests {

        @Test
        @DisplayName("Empty prefix yields 'tmp/'")
        void emptyPrefixYieldsTmpSlash() {
            assertThat(TempArchiveKey.tmpPrefix("")).isEqualTo("tmp/");
        }

        @Test
        @DisplayName("Simple prefix yields '{prefix}/tmp/'")
        void simplePrefixYieldsPrefixSlashTmpSlash() {
            assertThat(TempArchiveKey.tmpPrefix("myprefix")).isEqualTo("myprefix/tmp/");
        }

        @Test
        @DisplayName("Multi-segment prefix yields '{prefix}/tmp/'")
        void multiSegmentPrefixYieldsPrefixSlashTmpSlash() {
            assertThat(TempArchiveKey.tmpPrefix("hiero/mainnet")).isEqualTo("hiero/mainnet/tmp/");
        }
    }

    @Nested
    @DisplayName("formatTar / parseFirstBlockFromTar")
    final class TarKeyTests {

        @Test
        @DisplayName("Round-trip for firstBlock=0 with empty prefix")
        void tarRoundTripBlockZeroEmptyPrefix() {
            final String key = TempArchiveKey.formatTar(0L, "");
            assertThat(TempArchiveKey.parseFirstBlockFromTar(key, "")).isEqualTo(0L);
        }

        @Test
        @DisplayName("Round-trip for firstBlock=42 with empty prefix")
        void tarRoundTripBlock42EmptyPrefix() {
            final String key = TempArchiveKey.formatTar(42L, "");
            assertThat(TempArchiveKey.parseFirstBlockFromTar(key, "")).isEqualTo(42L);
        }

        @Test
        @DisplayName("Round-trip for large firstBlock with non-empty prefix")
        void tarRoundTripLargeBlockWithPrefix() {
            final long firstBlock = 1_000_000_000L;
            final String key = TempArchiveKey.formatTar(firstBlock, "hiero/mainnet");
            assertThat(TempArchiveKey.parseFirstBlockFromTar(key, "hiero/mainnet"))
                    .isEqualTo(firstBlock);
        }

        @Test
        @DisplayName("formatTar key ends with '.tmp'")
        void tarKeyEndsWithDotTmp() {
            assertThat(TempArchiveKey.formatTar(0L, "")).endsWith(".tmp");
        }

        @Test
        @DisplayName("formatTar key contains 19-digit zero-padded block number")
        void tarKeyContainsPaddedBlockNumber() {
            assertThat(TempArchiveKey.formatTar(42L, "")).contains("0000000000000000042");
        }

        @Test
        @DisplayName("formatTar key lives under tmpPrefix(prefix)")
        void tarKeyLivesUnderTmpPrefix() {
            final String prefix = "hiero";
            final String key = TempArchiveKey.formatTar(100L, prefix);
            assertThat(key).startsWith(TempArchiveKey.tmpPrefix(prefix));
        }
    }

    @Nested
    @DisplayName("formatMeta / parseFirstBlockFromMeta")
    final class MetaKeyTests {

        @Test
        @DisplayName("Round-trip for firstBlock=0 with empty prefix")
        void metaRoundTripBlockZeroEmptyPrefix() {
            final String key = TempArchiveKey.formatMeta(0L, "");
            assertThat(TempArchiveKey.parseFirstBlockFromMeta(key, "")).isEqualTo(0L);
        }

        @Test
        @DisplayName("Round-trip for arbitrary firstBlock with non-empty prefix")
        void metaRoundTripArbitraryBlockWithPrefix() {
            final long firstBlock = 500L;
            final String key = TempArchiveKey.formatMeta(firstBlock, "myprefix");
            assertThat(TempArchiveKey.parseFirstBlockFromMeta(key, "myprefix")).isEqualTo(firstBlock);
        }

        @Test
        @DisplayName("formatMeta key ends with '.meta'")
        void metaKeyEndsWithDotMeta() {
            assertThat(TempArchiveKey.formatMeta(0L, "")).endsWith(".meta");
        }

        @Test
        @DisplayName("formatMeta key lives under tmpPrefix(prefix)")
        void metaKeyLivesUnderTmpPrefix() {
            final String prefix = "test";
            assertThat(TempArchiveKey.formatMeta(77L, prefix)).startsWith(TempArchiveKey.tmpPrefix(prefix));
        }

        @Test
        @DisplayName("Tar and meta keys for the same firstBlock share the same stem (differ only in extension)")
        void tarAndMetaShareSameStem() {
            final long firstBlock = 77L;
            final String prefix = "test";
            final String tar = TempArchiveKey.formatTar(firstBlock, prefix);
            final String meta = TempArchiveKey.formatMeta(firstBlock, prefix);
            assertThat(tar.replace(".tmp", "")).isEqualTo(meta.replace(".meta", ""));
        }
    }

    @Nested
    @DisplayName("isTempTarKey")
    final class IsTempTarKeyTests {

        @Test
        @DisplayName("Returns true for a valid tar key with empty prefix")
        void trueForValidTarKeyEmptyPrefix() {
            assertThat(TempArchiveKey.isTempTarKey(TempArchiveKey.formatTar(0L, ""), ""))
                    .isTrue();
        }

        @Test
        @DisplayName("Returns true for a valid tar key with non-empty prefix")
        void trueForValidTarKeyWithPrefix() {
            assertThat(TempArchiveKey.isTempTarKey(TempArchiveKey.formatTar(10L, "p"), "p"))
                    .isTrue();
        }

        @Test
        @DisplayName("Returns false for a meta key")
        void falseForMetaKey() {
            assertThat(TempArchiveKey.isTempTarKey(TempArchiveKey.formatMeta(0L, ""), ""))
                    .isFalse();
        }

        @Test
        @DisplayName("Returns false for a regular archive key")
        void falseForRegularArchiveKey() {
            assertThat(TempArchiveKey.isTempTarKey(ArchiveKey.format(0L, 1, ""), ""))
                    .isFalse();
        }

        @Test
        @DisplayName("Returns false when objectKeyPrefix does not match the key's prefix")
        void falseWhenPrefixDoesNotMatch() {
            final String key = TempArchiveKey.formatTar(0L, "other");
            assertThat(TempArchiveKey.isTempTarKey(key, "myprefix")).isFalse();
        }
    }

    @Nested
    @DisplayName("isTempMetaKey")
    final class IsTempMetaKeyTests {

        @Test
        @DisplayName("Returns true for a valid meta key with empty prefix")
        void trueForValidMetaKeyEmptyPrefix() {
            assertThat(TempArchiveKey.isTempMetaKey(TempArchiveKey.formatMeta(0L, ""), ""))
                    .isTrue();
        }

        @Test
        @DisplayName("Returns true for a valid meta key with non-empty prefix")
        void trueForValidMetaKeyWithPrefix() {
            assertThat(TempArchiveKey.isTempMetaKey(TempArchiveKey.formatMeta(10L, "p"), "p"))
                    .isTrue();
        }

        @Test
        @DisplayName("Returns false for a tar key")
        void falseForTarKey() {
            assertThat(TempArchiveKey.isTempMetaKey(TempArchiveKey.formatTar(0L, ""), ""))
                    .isFalse();
        }

        @Test
        @DisplayName("Returns false for a regular archive key")
        void falseForRegularArchiveKey() {
            assertThat(TempArchiveKey.isTempMetaKey(ArchiveKey.format(0L, 1, ""), ""))
                    .isFalse();
        }

        @Test
        @DisplayName("Returns false when objectKeyPrefix does not match the key's prefix")
        void falseWhenPrefixDoesNotMatch() {
            final String key = TempArchiveKey.formatMeta(0L, "other");
            assertThat(TempArchiveKey.isTempMetaKey(key, "myprefix")).isFalse();
        }
    }
}
