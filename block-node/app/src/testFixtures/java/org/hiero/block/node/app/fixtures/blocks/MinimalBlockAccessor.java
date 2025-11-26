// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.blocks;

import com.github.luben.zstd.Zstd;
import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;

public final class MinimalBlockAccessor implements BlockAccessor {
    private final long blockNumber;
    private final Block block;
    private boolean isClosed = false;

    public MinimalBlockAccessor(final long blockNumber, final Block block) {
        this.blockNumber = blockNumber;
        this.block = block;
    }

    @Override
    public long blockNumber() {
        return blockNumber;
    }

    @Override
    public Bytes blockBytes(final Format format) {
        return switch (format) {
            case JSON -> Block.JSON.toBytes(block);
            case PROTOBUF -> Block.PROTOBUF.toBytes(block);
            case ZSTD_PROTOBUF -> zstdCompressBytes(Block.PROTOBUF.toBytes(block));
        };
    }

    private Bytes zstdCompressBytes(final Bytes bytes) {
        return Bytes.wrap(Zstd.compress(bytes.toByteArray()));
    }

    @Override
    public void close() {
        isClosed = true;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }
}
