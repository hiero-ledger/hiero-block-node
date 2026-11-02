// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator.itemhandler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hedera.hapi.block.stream.output.protoc.BlockFooter;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import java.util.Arrays;
import org.hiero.block.simulator.Constants;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/// Tests for the [BlockFooterHandler] class, the producer of the footer item of a crafted block.
@DisplayName("Block Footer Handler Tests")
class BlockFooterHandlerTest {
    /// The digest size of the crafted chain.
    private static final int HASH_SIZE = Constants.BLOCK_HASH_ALGORITHM.hashSize();

    /// Tests for the footer item the handler builds.
    @Nested
    @DisplayName("Footer Item Tests")
    class FooterItemTests {
        /// This test aims to assert that the footer item carries the given previous block hash and
        /// root of the all previous block hashes tree unchanged, and a zero filled digest as the
        /// start of block state root, because crafted blocks carry no state.
        @Test
        @DisplayName("getItem() carries the given footer hashes")
        void testFooterCarriesGivenHashes() {
            final byte[] previousBlockHash = filledDigest((byte) 0x11);
            final byte[] hashOfAllBlockHashesTree = filledDigest((byte) 0x22);
            final BlockItem item = new BlockFooterHandler(previousBlockHash, hashOfAllBlockHashesTree).getItem();
            assertThat(item.hasBlockFooter()).isTrue();
            final BlockFooter footer = item.getBlockFooter();
            assertThat(footer.getPreviousBlockRootHash().toByteArray()).isEqualTo(previousBlockHash);
            assertThat(footer.getRootHashOfAllBlockHashesTree().toByteArray()).isEqualTo(hashOfAllBlockHashesTree);
            assertThat(footer.getStartOfBlockStateRootHash().toByteArray()).isEqualTo(new byte[HASH_SIZE]);
        }

        /// This test aims to assert that a null root of the all previous block hashes tree does not
        /// fail the handler but defaults to one zero filled digest, so the footer still carries a
        /// value of the digest size of the crafted chain.
        @Test
        @DisplayName("getItem() defaults a null all blocks tree root to a zero filled digest")
        void testNullAllBlocksTreeRootDefaultsToZeroDigest() {
            final byte[] previousBlockHash = filledDigest((byte) 0x11);
            final BlockItem item = new BlockFooterHandler(previousBlockHash, null).getItem();
            final BlockFooter footer = item.getBlockFooter();
            assertThat(footer.getRootHashOfAllBlockHashesTree().toByteArray()).isEqualTo(new byte[HASH_SIZE]);
            assertThat(footer.getPreviousBlockRootHash().toByteArray()).isEqualTo(previousBlockHash);
        }

        /// This test aims to assert that the handler builds the footer item once and returns the
        /// same instance on every further call.
        @Test
        @DisplayName("getItem() returns the cached item")
        void testGetItemIsCached() {
            final BlockFooterHandler handler =
                    new BlockFooterHandler(filledDigest((byte) 0x11), filledDigest((byte) 0x22));
            assertThat(handler.getItem()).isSameAs(handler.getItem());
        }
    }

    /// Tests for the constructor arguments.
    @Nested
    @DisplayName("Constructor Tests")
    class ConstructorTests {
        /// This test aims to assert that a null previous block hash is still refused, since only
        /// the root of the all previous block hashes tree has a default.
        @Test
        @DisplayName("constructor refuses a null previous block hash")
        void testNullPreviousBlockHashRefused() {
            final byte[] hashOfAllBlockHashesTree = filledDigest((byte) 0x22);
            assertThatThrownBy(() -> new BlockFooterHandler(null, hashOfAllBlockHashesTree))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    private static byte[] filledDigest(final byte value) {
        final byte[] digest = new byte[HASH_SIZE];
        Arrays.fill(digest, value);
        return digest;
    }
}
