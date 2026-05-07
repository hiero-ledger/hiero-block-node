// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session;

import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.security.PublicKey;
import java.util.Map;
import java.util.Objects;
import org.hiero.block.common.utils.Preconditions;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.verification.session.impl.DummyVerificationSession;
import org.hiero.block.node.verification.session.impl.ExtendedMerkleTreeSession;
import org.hiero.metrics.LongCounter;

/**
 * Factory for creating `VerificationSession` instances based on the requested HAPI version.
 */
public final class HapiVersionSessionFactory {
    private static final SemanticVersion V_0_72_0 = semanticVersion(0, 72, 0);
    private static final SemanticVersion V_0_64_0 = semanticVersion(0, 64, 0);

    private HapiVersionSessionFactory() {}

    /**
     * Creates a `VerificationSession` for the given HAPI version.
     *
     * <p>Delegates to `createSession` with an empty RSA key map and no RSA metrics. Use
     * `createSession(...)` with the RSA parameters when a `NodeAddressBook` is available.
     *
     * @param blockNumber the block number (>= 0)
     * @param blockSource the source of blocks
     * @param hapiVersion the semantic HAPI version
     * @param previousBlockHash the previous block hash, may be null
     * @param allPreviousBlocksRootHash the all-blocks root hash, may be null
     * @param ledgerId the trusted ledger ID for TSS verification, may be null
     * @throws NullPointerException if `blockSource` or `hapiVersion` is null
     * @throws IllegalArgumentException if `blockNumber` is less than 0 or the version is unsupported
     */
    public static VerificationSession createSession(
            final long blockNumber,
            final BlockSource blockSource,
            final SemanticVersion hapiVersion,
            final Bytes previousBlockHash,
            final Bytes allPreviousBlocksRootHash,
            @Nullable final Bytes ledgerId) {
        return createSession(
                blockNumber,
                blockSource,
                hapiVersion,
                previousBlockHash,
                allPreviousBlocksRootHash,
                ledgerId,
                Map.of(),
                null,
                null,
                null);
    }

    /**
     * Creates a `VerificationSession` for the given HAPI version, with RSA WRB support.
     *
     * <p>When `rsaKeyByNodeId` is non-empty and the block carries a `SignedRecordFileProof`,
     * the returned session will attempt RSA verification using the provided key map and
     * will report results via the supplied metric counters (when non-null).
     *
     * @param blockNumber the block number (>= 0)
     * @param blockSource the source of blocks
     * @param hapiVersion the semantic HAPI version
     * @param previousBlockHash the previous block hash, may be null
     * @param allPreviousBlocksRootHash the all-blocks root hash, may be null
     * @param ledgerId the trusted ledger ID for TSS verification, may be null
     * @param rsaKeyByNodeId map from `node_id` to RSA `PublicKey`; use `Map.of()` when unavailable
     * @param rsaVerificationSuccessTotal metric for successful RSA verifications, may be null
     * @param rsaVerificationFailureTotal metric for failed RSA verifications, may be null
     * @param rsaRosterMismatchTotal metric for sigs from unknown nodes, may be null
     * @throws NullPointerException if `blockSource`, `hapiVersion`, or `rsaKeyByNodeId` is null
     * @throws IllegalArgumentException if `blockNumber` is less than 0 or the version is unsupported
     */
    public static VerificationSession createSession(
            final long blockNumber,
            final BlockSource blockSource,
            final SemanticVersion hapiVersion,
            final Bytes previousBlockHash,
            final Bytes allPreviousBlocksRootHash,
            @Nullable final Bytes ledgerId,
            final Map<Long, PublicKey> rsaKeyByNodeId,
            @Nullable final LongCounter.Measurement rsaVerificationSuccessTotal,
            @Nullable final LongCounter.Measurement rsaVerificationFailureTotal,
            @Nullable final LongCounter.Measurement rsaRosterMismatchTotal) {
        Objects.requireNonNull(blockSource, "blockSource cannot be null");
        Objects.requireNonNull(hapiVersion, "hapiVersion cannot be null");
        Objects.requireNonNull(rsaKeyByNodeId, "rsaKeyByNodeId cannot be null");
        Preconditions.requireWhole(blockNumber, "blockNumber must be >= 0");

        if (isGreaterThanOrEqual(hapiVersion, V_0_72_0)) {
            return new ExtendedMerkleTreeSession(
                    blockNumber,
                    blockSource,
                    previousBlockHash,
                    allPreviousBlocksRootHash,
                    ledgerId,
                    rsaKeyByNodeId,
                    rsaVerificationSuccessTotal,
                    rsaVerificationFailureTotal,
                    rsaRosterMismatchTotal);
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
