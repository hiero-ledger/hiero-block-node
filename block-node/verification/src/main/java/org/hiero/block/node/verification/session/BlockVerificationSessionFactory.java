// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session;

import com.hedera.hapi.node.base.SemanticVersion;
import java.util.Objects;
import java.util.function.BiFunction;
import org.hiero.block.common.utils.Preconditions;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.verification.session.impl.BlockVerificationSessionAt0640;
import org.hiero.block.node.verification.session.impl.BlockVerificationSessionAt0680;

/**
 * Factory for creating {@link BlockVerificationSession} instances based on the requested HAPI version.
 *
 * <p>Entries are listed newest → oldest. The first whose {@code since} is {@code <=} the requested
 * version is selected; it remains valid until a newer one is added above it.</p>
 */
public final class BlockVerificationSessionFactory {

    /** Newest first; each covers [since, nextSince). Add new entries at the TOP. */
    private static final Entry[] ENTRIES = new Entry[] {
        new Entry(semanticVersion(0, 68, 0), BlockVerificationSessionAt0680::new),
        new Entry(semanticVersion(0, 64, 0), BlockVerificationSessionAt0640::new),
    };

    private BlockVerificationSessionFactory() {}

    /**
     * Create a {@link BlockVerificationSession} for the given HAPI version.
     *
     * @throws NullPointerException if blockSource or hapiVersion is null
     * @throws IllegalArgumentException if blockNumber less than 0 or version unsupported
     */
    public static BlockVerificationSession createSession(
            final long blockNumber, final BlockSource blockSource, final SemanticVersion hapiVersion) {
        // Validate arguments
        Objects.requireNonNull(blockSource, "blockSource");
        Objects.requireNonNull(hapiVersion, "hapiVersion");
        Preconditions.requireWhole(blockNumber, "blockNumber must be >= 0");

        // Newest → Oldest (first match wins)
        for (final Entry e : ENTRIES) {
            if (gte(hapiVersion, e.since)) {
                return e.ctor.apply(blockNumber, blockSource);
            }
        }

        // No match
        throw new IllegalArgumentException("Unsupported HAPI version: " + toShortString(hapiVersion)
                + " (supported from " + toShortString(ENTRIES[ENTRIES.length - 1].since) + " and up)");
    }

    /** v >= w ? (lexicographic by major, minor, patch). */
    private static boolean gte(final SemanticVersion v, final SemanticVersion w) {
        if (v.major() != w.major()) return v.major() > w.major();
        if (v.minor() != w.minor()) return v.minor() > w.minor();
        return v.patch() >= w.patch();
    }

    /** Tiny helper to build a SemanticVersion constant. Adjust if your API differs. */
    private static SemanticVersion semanticVersion(final int major, final int minor, final int patch) {
        return SemanticVersion.newBuilder()
                .major(major)
                .minor(minor)
                .patch(patch)
                .build();
    }

    private static String toShortString(final SemanticVersion v) {
        return v.major() + "." + v.minor() + "." + v.patch();
    }

    /** Minimal registry entry: since-version (inclusive) and the constructor. */
    private record Entry(SemanticVersion since, BiFunction<Long, BlockSource, BlockVerificationSession> ctor) {
        Entry {
            Objects.requireNonNull(since, "since");
            Objects.requireNonNull(ctor, "ctor");
        }
    }
}
