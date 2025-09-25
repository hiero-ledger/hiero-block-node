// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session;

import com.hedera.hapi.node.base.SemanticVersion;
import java.util.Objects;
import org.hiero.block.common.utils.Preconditions;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.verification.session.impl.BlockVerificationSessionAt0640;
import org.hiero.block.node.verification.session.impl.BlockVerificationSessionAt0680;

/**
 * Factory for creating {@link BlockVerificationSession} instances based on the requested HAPI version.
 */
public final class BlockVerificationSessionFactory {

    private BlockVerificationSessionFactory() {}

    /**
     * Create a {@link BlockVerificationSession} for the given HAPI version.
     *
     * @param blockNumber the block number (>= 0)
     * @param blockSource the source of blocks
     * @param hapiVersion the semantic HAPI version
     *
     * @throws NullPointerException if blockSource or hapiVersion is null
     * @throws IllegalArgumentException if blockNumber less than 0 or version unsupported
     */
    public static BlockVerificationSession createSession(
            final long blockNumber, final BlockSource blockSource, final SemanticVersion hapiVersion) {

        Objects.requireNonNull(blockSource, "blockSource");
        Objects.requireNonNull(hapiVersion, "hapiVersion");
        Preconditions.requireWhole(blockNumber, "blockNumber must be >= 0");

        if (gte(hapiVersion, semanticVersion(0, 68, 0))) {
            return new BlockVerificationSessionAt0680(blockNumber, blockSource, "extraBytesPlaceholder");
        } else if (gte(hapiVersion, semanticVersion(0, 64, 0))) {
            return new BlockVerificationSessionAt0640(blockNumber, blockSource);
        } else {
            throw new IllegalArgumentException(
                    "Unsupported HAPI version: " + toShortString(hapiVersion) + " (supported from 0.64.0 and up)");
        }
    }

    /** v >= w ? (lexicographic by major, minor, patch) */
    private static boolean gte(final SemanticVersion v, final SemanticVersion w) {
        if (v.major() != w.major()) return v.major() > w.major();
        if (v.minor() != w.minor()) return v.minor() > w.minor();
        return v.patch() >= w.patch();
    }

    /** Tiny helper to build a SemanticVersion constant */
    private static SemanticVersion semanticVersion(final int major, final int minor, final int patch) {
        return SemanticVersion.newBuilder()
                .major(major)
                .minor(minor)
                .patch(patch)
                .build();
    }
    /** Convert to a short string "major.minor.patch" */
    private static String toShortString(final SemanticVersion v) {
        return v.major() + "." + v.minor() + "." + v.patch();
    }
}
