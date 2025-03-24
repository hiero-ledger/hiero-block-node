package org.hiero.block.node.spi.blockmessaging;

import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import java.util.List;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.hapi.block.node.BlockItemUnparsed;

/**
 * A record that holds a list of block items and the block number if items start with block header of a new block.
 * This is used to send block items throughout the server.The parsed block number for the start of a new block, is
 * included to avoid every consumer having to parse the block number from the block items.
 *
 * @param blockItems     the immutable list of block items to handle
 * @param newBlockNumber if these items include the start of a new block, this is the block number. If not, this is
 *                       {@link BlockNodePlugin#UNKNOWN_BLOCK_NUMBER}.
 */
public record BlockItems(
        List<BlockItemUnparsed> blockItems, long newBlockNumber
) {
}
