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
public final class VerificationHelper {
    /// [SemanticVersion] for `0.73.0`.
    public static final SemanticVersion V_0_73_0 = semanticVersion(0, 73, 0);

    private VerificationHelper() {}

    /// Helper to build a SemanticVersion constant.
    private static SemanticVersion semanticVersion(final int major, final int minor, final int patch) {
        return SemanticVersion.newBuilder()
                .major(major)
                .minor(minor)
                .patch(patch)
                .build();
    }

    /// Helper to check if a [SemanticVersion] is greater than or equal to another.
    /// ```toCompare >= base ? (lexicographic by major, minor, patch).```
    public static boolean isVersionGreaterOrEqualTo(final SemanticVersion toCompare, final SemanticVersion base) {
        if (toCompare.major() != base.major()) return toCompare.major() > base.major();
        if (toCompare.minor() != base.minor()) return toCompare.minor() > base.minor();
        return toCompare.patch() >= base.patch();
    }

    /// Helper to extract, map, and build [TssData] from [LedgerIdPublicationTransactionBody].
    /// @throws NullPointerException if `publication` is `null`.
    public static TssData extractTssData(final LedgerIdPublicationTransactionBody publication) {
        final Builder builder = TssData.newBuilder();
        builder.ledgerId(publication.ledgerId());
        builder.wrapsVerificationKey(publication.historyProofVerificationKey());
        builder.currentRoster(extractContributions(publication.nodeContributions()));
        // todo(2528) what about valid from value? builder.validFromBlock();
        return builder.build();
    }

    /// Helper to extract, map, and build [TssRoster] from a list of [LedgerIdNodeContribution].
    /// @throws NullPointerException if `contributions` is `null`.
    public static TssRoster extractContributions(final List<LedgerIdNodeContribution> contributions) {
        // todo(2528) is this correct? Does RosterEntry map to LedgerIdNodeContribution?
        final TssRoster.Builder builder = TssRoster.newBuilder();
        final ArrayList<RosterEntry> rosterEntries = new ArrayList<>();
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
