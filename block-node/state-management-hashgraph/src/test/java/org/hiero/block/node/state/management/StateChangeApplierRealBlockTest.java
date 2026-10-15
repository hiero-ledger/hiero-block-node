// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.base.time.Time;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.merkledb.config.MerkleDbConfig;
import com.swirlds.state.merkle.VirtualMapState;
import com.swirlds.state.merkle.VirtualMapStateLifecycleManager;
import com.swirlds.virtualmap.config.VirtualMapConfig;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;
import org.hiero.base.file.FileSystemManager;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.consensus.config.PathsConfig;
import org.hiero.consensus.metrics.noop.NoOpMetrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/// Runs {@link StateChangeApplier} against a real {@code VirtualMapStateImpl} (via the real
/// {@code VirtualMapStateLifecycleManager}, not {@link InMemoryBinaryState}) and a real
/// consensus-node block pair, asserting the computed root hash matches the next block's footer.
/// This is the one test shape that would have caught the oneof-wrapper-bytes encoding bug: the
/// in-memory test double doesn't apply {@code VirtualMapStateImpl}'s key/value wrapping, so unit
/// tests against it passed despite the bug.
class StateChangeApplierRealBlockTest {

    @Test
    void appliedBlockRootHashMatchesNextBlocksFooter(@TempDir final Path tmp) throws Exception {
        final BlockUnparsed block0 = loadBlock("CN_11_12_TSS_SCHNORR/0.blk.gz");
        final BlockUnparsed block1 = loadBlock("CN_11_12_TSS_SCHNORR/1.blk.gz");
        final Bytes block1StartHash = StateChangeApplier.extractStartOfBlockStateRootHash(block1);
        assertThat(block1StartHash).isNotNull();

        final var config = ConfigurationBuilder.create()
                .withConfigDataType(MerkleDbConfig.class)
                .withConfigDataType(VirtualMapConfig.class)
                .withConfigDataType(PathsConfig.class)
                .build();
        final VirtualMapStateLifecycleManager lifecycleManager = new VirtualMapStateLifecycleManager(
                new NoOpMetrics(), Time.getCurrent(), config, new FileSystemManager(tmp));
        lifecycleManager.copyMutableState(); // seal the genesis empty state

        final StateChangeApplier applier = new StateChangeApplier();
        final VirtualMapState mutable = lifecycleManager.getMutableState();
        final var result = applier.applyBlock(mutable, block0);
        assertThat(result.blockNumber()).isEqualTo(0L);

        lifecycleManager.copyMutableState(); // seal post-block-0 state
        final VirtualMapState postBlock0 = lifecycleManager.getLatestImmutableState();
        final Bytes computedHash = Bytes.wrap(postBlock0.getRoot().getHash().copyToByteArray());

        assertThat(computedHash)
                .as("root hash after applying block 0's state_changes must match block 1's footer")
                .isEqualTo(block1StartHash);
    }

    @NonNull
    private static BlockUnparsed loadBlock(@NonNull final String resourceName) throws IOException {
        try (final InputStream stream = TestUtils.class.getModule().getResourceAsStream("test-blocks/" + resourceName);
                final GZIPInputStream gzipInputStream = new GZIPInputStream(stream)) {
            return BlockUnparsed.PROTOBUF.parse(Bytes.wrap(gzipInputStream.readAllBytes()));
        } catch (final Exception e) {
            throw new IOException("Failed to load/parse test block resource " + resourceName, e);
        }
    }
}
