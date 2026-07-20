// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records.model.parsed;

import static org.hiero.block.tools.utils.Sha384.sha384Digest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.node.base.Timestamp;
import com.hedera.hapi.streams.ContractAction;
import com.hedera.hapi.streams.ContractActions;
import com.hedera.hapi.streams.HashAlgorithm;
import com.hedera.hapi.streams.HashObject;
import com.hedera.hapi.streams.SidecarFile;
import com.hedera.hapi.streams.SidecarMetadata;
import com.hedera.hapi.streams.TransactionSidecarRecord;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Direct-invocation tests for {@link ParsedRecordBlock#filterOrphanSidecars(List, List)}.
 *
 * <p>Kept in a dedicated file so the state-sharing {@link ParsedRecordBlockTest} lifecycle
 * isn't disturbed. The filter is invoked by name (package-private) rather than exercised
 * through {@link ParsedRecordBlock#parse} so we can control inputs precisely without
 * standing up full record files.
 */
class ParsedRecordBlockFilterTest {

    @Nested
    @DisplayName("filterOrphanSidecars")
    class FilterTests {

        @Test
        @DisplayName("empty sidecar list returns the same empty list")
        void emptyList_returnsSameList() {
            final List<String> warnings = new ArrayList<>();
            final List<SidecarFile> in = List.of();
            final List<SidecarFile> out = ParsedRecordBlock.filterOrphanSidecars(in, List.of(), warnings::add);
            assertSame(in, out, "empty input short-circuits without allocation");
            assertEquals(0, warnings.size(), "no warnings should be emitted for empty input");
        }

        @Test
        @DisplayName("all sidecars with matching signed hashes are retained")
        void allMatch_allRetained() {
            final SidecarFile s0 = sidecarWith("a");
            final SidecarFile s1 = sidecarWith("b");
            final List<SidecarFile> in = List.of(s0, s1);
            final List<SidecarMetadata> metas = List.of(metadataFor(s0), metadataFor(s1));
            final List<String> warnings = new ArrayList<>();

            final List<SidecarFile> out = ParsedRecordBlock.filterOrphanSidecars(in, metas, warnings::add);

            assertEquals(2, out.size());
            assertSame(s0, out.get(0));
            assertSame(s1, out.get(1));
            assertEquals(0, warnings.size(), "no warnings when every candidate matches");
        }

        @Test
        @DisplayName("orphan sidecar (no matching signed hash) is dropped and warning emitted")
        void orphanDropped_warningLogged() {
            final SidecarFile signed = sidecarWith("signed");
            final SidecarFile orphan = sidecarWith("orphan");
            final List<SidecarFile> in = List.of(signed, orphan);
            final List<SidecarMetadata> metas = List.of(metadataFor(signed));
            final List<String> warnings = new ArrayList<>();

            final List<SidecarFile> out = ParsedRecordBlock.filterOrphanSidecars(in, metas, warnings::add);

            assertEquals(1, out.size(), "orphan should be dropped");
            assertSame(signed, out.get(0), "only the signed sidecar remains");
            assertEquals(1, warnings.size(), "exactly one warning for the one dropped sidecar");
            assertTrue(warnings.get(0).contains("Dropping orphan sidecar"), "warning shape");
            assertTrue(warnings.get(0).contains("#1"), "warning identifies the dropped index");
        }

        @Test
        @DisplayName("all sidecars orphaned - all dropped, empty list returned")
        void allOrphans_allDropped() {
            final SidecarFile s0 = sidecarWith("a");
            final SidecarFile s1 = sidecarWith("b");
            final List<SidecarFile> in = List.of(s0, s1);
            final List<SidecarMetadata> metas = List.of(); // signed list empty
            final List<String> warnings = new ArrayList<>();

            final List<SidecarFile> out = ParsedRecordBlock.filterOrphanSidecars(in, metas, warnings::add);

            assertEquals(0, out.size(), "with empty signed list, every candidate is an orphan");
            assertEquals(2, warnings.size(), "one warning per dropped sidecar");
            assertTrue(warnings.get(0).contains("#0"));
            assertTrue(warnings.get(1).contains("#1"));
        }

        @Test
        @DisplayName("metadata entry without a hash field does not vouch for any sidecar")
        void metadataWithoutHash_treatedAsUnsigned() {
            final SidecarFile s = sidecarWith("x");
            final List<SidecarFile> in = List.of(s);
            final List<SidecarMetadata> metas = List.of(SidecarMetadata.DEFAULT); // no hash set
            final List<String> warnings = new ArrayList<>();

            final List<SidecarFile> out = ParsedRecordBlock.filterOrphanSidecars(in, metas, warnings::add);

            assertEquals(0, out.size(), "metadata with no hash cannot back any sidecar");
            assertEquals(1, warnings.size());
            assertTrue(warnings.get(0).contains("Dropping orphan sidecar"));
        }

        @Test
        @DisplayName(
                "signed hash present but no matching candidate - candidate kept only if its hash is in signed list")
        void extraSignedHash_doesNotAffectRetentionDecision() {
            // Two signed hashes, only one candidate that matches the first.
            // The extra signed hash without a candidate is the MISSING case, which is a separate concern
            // (handled elsewhere). The filter itself should still retain the one candidate that matches.
            final SidecarFile matches = sidecarWith("here");
            final SidecarFile ghost = sidecarWith("gone"); // never presented as candidate
            final List<SidecarFile> in = List.of(matches);
            final List<SidecarMetadata> metas = List.of(metadataFor(matches), metadataFor(ghost));
            final List<String> warnings = new ArrayList<>();

            final List<SidecarFile> out = ParsedRecordBlock.filterOrphanSidecars(in, metas, warnings::add);
            assertEquals(1, out.size());
            assertSame(matches, out.get(0));
            assertEquals(0, warnings.size(), "no warnings when the candidate matches");
        }

        @Test
        @DisplayName("relative order of retained sidecars is preserved")
        void retainedOrderPreserved() {
            final SidecarFile a = sidecarWith("a");
            final SidecarFile b = sidecarWith("b");
            final SidecarFile c = sidecarWith("c");
            // in list: [a, b, c]; b is the orphan; expect [a, c] out.
            final List<SidecarFile> in = List.of(a, b, c);
            final List<SidecarMetadata> metas = List.of(metadataFor(a), metadataFor(c));
            final List<String> warnings = new ArrayList<>();

            final List<SidecarFile> out = ParsedRecordBlock.filterOrphanSidecars(in, metas, warnings::add);
            assertEquals(2, out.size());
            assertSame(a, out.get(0));
            assertSame(c, out.get(1));
            assertEquals(1, warnings.size(), "one warning for the one dropped sidecar");
            assertTrue(warnings.get(0).contains("#1"));
        }
    }

    // --- helpers ---

    /**
     * Build a small distinct {@link SidecarFile} whose serialized bytes hash to a stable
     * per-tag SHA-384. A single {@link TransactionSidecarRecord} with the given tag encoded
     * into the record's inner {@code ContractActions} payload is enough to differentiate
     * one sidecar from another.
     */
    private static SidecarFile sidecarWith(final String tag) {
        final Timestamp ts = new Timestamp(1, tag.hashCode() & 0x7fffffff);
        final ContractAction action =
                ContractAction.newBuilder().input(Bytes.wrap(tag.getBytes())).build();
        final ContractActions actions =
                ContractActions.newBuilder().contractActions(action).build();
        final TransactionSidecarRecord record = TransactionSidecarRecord.newBuilder()
                .consensusTimestamp(ts)
                .actions(actions)
                .build();
        return SidecarFile.newBuilder().sidecarRecords(record).build();
    }

    /** Build a {@link SidecarMetadata} entry whose hash equals SHA-384 of the given sidecar's serialized bytes. */
    private static SidecarMetadata metadataFor(final SidecarFile sf) {
        final MessageDigest digest = sha384Digest();
        SidecarFile.PROTOBUF.toBytes(sf).writeTo(digest);
        final byte[] hash = digest.digest();
        final HashObject ho = HashObject.newBuilder()
                .algorithm(HashAlgorithm.SHA_384)
                .length(hash.length)
                .hash(Bytes.wrap(hash))
                .build();
        return SidecarMetadata.newBuilder().hash(ho).build();
    }
}
