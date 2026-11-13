// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.download;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BlockKeysTest {

    @Test
    @DisplayName("Block key splits the block number into the expanded folder layout under the node's block folder")
    void blockKeyLayout() {
        assertThat(BlockKeys.blockKey("testnet-202611011200", 3, 108_273_182L))
                .isEqualTo("testnet-202611011200/3/block/0000/0000/0010/8273/182.blk.zstd");
        assertThat(BlockKeys.blockKey("mainnet", 3, 1L)).isEqualTo("mainnet/3/block/0000/0000/0000/0000/001.blk.zstd");
        assertThat(BlockKeys.blockKey("mainnet", 3, 1_234_567L))
                .isEqualTo("mainnet/3/block/0000/0000/0000/1234/567.blk.zstd");
    }

    @Test
    @DisplayName("Block key has no leading slash when the prefix is empty or null")
    void blockKeyWithoutPrefix() {
        assertThat(BlockKeys.blockKey("", 3, 108_273_182L)).isEqualTo("3/block/0000/0000/0010/8273/182.blk.zstd");
        assertThat(BlockKeys.blockKey(null, 3, 108_273_182L)).isEqualTo("3/block/0000/0000/0010/8273/182.blk.zstd");
    }
}
