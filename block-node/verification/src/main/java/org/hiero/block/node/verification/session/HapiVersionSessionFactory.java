// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session;

import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.Objects;
import org.hiero.block.common.utils.Preconditions;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.verification.session.impl.DummyVerificationSession;
import org.hiero.block.node.verification.session.impl.ExtendedMerkleTreeSession;

/**
 * Factory for creating {@link VerificationSession} instances based on the requested HAPI version.
 */
public final class HapiVersionSessionFactory {
    private static final SemanticVersion V_0_70_0 = semanticVersion(0, 70, 0);
    private static final SemanticVersion V_0_69_0 = semanticVersion(0, 69, 0);
    private static final SemanticVersion V_0_64_0 = semanticVersion(0, 64, 0);

    private HapiVersionSessionFactory() {}

    /**
     * Create a {@link VerificationSession} for the given HAPI version.
     *
     * @param blockNumber the block number (>= 0)
     * @param blockSource the source of blocks
     * @param hapiVersion the semantic HAPI version
     * @param previousBlockHash the previous block hash, may be null
     * @param allPreviousBlocksRootHash the all blocks root hash, may be null
     *
     * @throws NullPointerException if blockSource or hapiVersion is null
     * @throws IllegalArgumentException if blockNumber less than 0 or version unsupported
     */
    public static VerificationSession createSession(
            final long blockNumber,
            final BlockSource blockSource,
            final SemanticVersion hapiVersion,
            final Bytes previousBlockHash,
            final Bytes allPreviousBlocksRootHash) {
        Objects.requireNonNull(blockSource, "blockSource cannot be null");
        Objects.requireNonNull(hapiVersion, "hapiVersion cannot be null");
        Preconditions.requireWhole(blockNumber, "blockNumber must be >= 0");

        // TODO, before going live we should remove the Dummy Implementation.
        if (isGreaterThanOrEqual(hapiVersion, V_0_70_0)) {
            return new DummyVerificationSession(blockNumber, blockSource);
        }

        if (isGreaterThanOrEqual(hapiVersion, V_0_69_0)) {
            return new ExtendedMerkleTreeSession(
                    blockNumber, blockSource, previousBlockHash, allPreviousBlocksRootHash);
        }

        // TODO, before going live we should remove the Dummy Implementation.
        if (isGreaterThanOrEqual(hapiVersion, V_0_64_0)) {
            return new DummyVerificationSession(blockNumber, blockSource);
        }

        throw new IllegalArgumentException(
                "Unsupported HAPI version: %s (supported from 0.64.0 and up)".formatted(toShortString(hapiVersion)));
    }

    /** v >= w ? (lexicographic by major, minor, patch) */
    private static boolean isGreaterThanOrEqual(final SemanticVersion v, final SemanticVersion w) {
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
        return "%02d.%03d.%04d".formatted(v.major(), v.minor(), v.patch());
    }
}
