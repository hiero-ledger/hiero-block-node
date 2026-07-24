// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification;

import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.tss.LedgerIdNodeContribution;
import com.hedera.hapi.node.tss.LedgerIdPublicationTransactionBody;
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.api.RosterEntry;
import org.hiero.block.api.TssData;
import org.hiero.block.api.TssData.Builder;
import org.hiero.block.api.TssRoster;

/// Verification helper.
/// A utility class with static helpers used across the verification module.
public final class VerificationHelper {
    /// [SemanticVersion] for `0.73.0`.
    public static final SemanticVersion V_0_73_0 = semanticVersion(0, 73, 0);

    /// Private constructor to prevent instantiation.
    private VerificationHelper() {}

    /// Helper to build a [SemanticVersion] constant.
    ///
    /// @param major the major version
    /// @param minor the minor version
    /// @param patch the patch version
    /// @return a new [SemanticVersion] built from the given parts
    private static SemanticVersion semanticVersion(final int major, final int minor, final int patch) {
        return SemanticVersion.newBuilder()
                .major(major)
                .minor(minor)
                .patch(patch)
                .build();
    }

    /// Helper to check if a [SemanticVersion] is greater than or equal to another,
    /// comparing lexicographically by major, minor, patch.
    ///
    /// @param toCompare the version to compare
    /// @param base the version to compare against
    /// @return `true` if `toCompare >= base`
    public static boolean isVersionGreaterOrEqualTo(final SemanticVersion toCompare, final SemanticVersion base) {
        if (toCompare.major() != base.major()) return toCompare.major() > base.major();
        if (toCompare.minor() != base.minor()) return toCompare.minor() > base.minor();
        return toCompare.patch() >= base.patch();
    }

    /// Helper to extract, map, and build [TssData] from [LedgerIdPublicationTransactionBody].
    ///
    /// @param publication the publication body to extract from, must not be null
    /// @param blockNumber the block number the extracted data is valid from
    /// @return a new [TssData] built from the publication
    /// @throws NullPointerException if `publication` is `null`
    public static TssData extractTssData(final LedgerIdPublicationTransactionBody publication, final long blockNumber) {
        final Builder builder = TssData.newBuilder();
        builder.ledgerId(publication.ledgerId());
        builder.wrapsVerificationKey(publication.historyProofVerificationKey());
        builder.currentRoster(extractContributions(publication.nodeContributions()));
        builder.validFromBlock(blockNumber);
        return builder.build();
    }

    /// Helper to extract, map, and build [TssRoster] from a list of [LedgerIdNodeContribution].
    ///
    /// @param contributions the node contributions to map into roster entries, must not be null
    /// @return a new [TssRoster] built from the contributions
    /// @throws NullPointerException if `contributions` is `null`
    public static TssRoster extractContributions(final List<LedgerIdNodeContribution> contributions) {
        final TssRoster.Builder builder = TssRoster.newBuilder();
        final List<RosterEntry> rosterEntries = new ArrayList<>();
        for (final LedgerIdNodeContribution contribution : contributions) {
            final RosterEntry.Builder entryBuilder = RosterEntry.newBuilder();
            entryBuilder.nodeId(contribution.nodeId());
            entryBuilder.weight(contribution.weight());
            entryBuilder.schnorrPublicKey(contribution.historyProofKey());
            rosterEntries.add(entryBuilder.build());
        }
        builder.rosterEntries(rosterEntries);
        return builder.build();
    }
}
