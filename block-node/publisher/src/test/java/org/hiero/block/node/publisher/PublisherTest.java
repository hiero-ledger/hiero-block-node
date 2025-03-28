// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.publisher;

import static org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder.sampleBlockHeader;
import static org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder.sampleBlockProof;
import static org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder.sampleRoundHeader;
import static org.hiero.block.node.publisher.PublisherServicePlugin.BlockStreamPublisherServiceMethod.publishBlockStream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.hedera.hapi.block.BlockItemSet;
import com.hedera.hapi.block.PublishStreamRequest;
import com.hedera.hapi.block.PublishStreamRequest.RequestOneOfType;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.grpc.ServiceInterface.Method;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import org.hiero.block.node.app.fixtures.plugintest.GrpcTestAppBase;
import org.hiero.block.node.app.fixtures.plugintest.NoBlocksHistoricalBlockFacility;
import org.junit.jupiter.api.Test;

/**
 * Tests for the PublisherServicePlugin. It mocks out the rest of the block node so we can simply test just this plugin.
 */
@SuppressWarnings({"FieldCanBeLocal", "MismatchedQueryAndUpdateOfCollection", "SameParameterValue"})
public class PublisherTest extends GrpcTestAppBase {

    public PublisherTest() {
        super(new PublisherServicePlugin(), publishBlockStream, new NoBlocksHistoricalBlockFacility());
    }

    @Test
    void testServiceInterfaceBasics() {
        // check we have a service interface
        assertNotNull(serviceInterface);
        // check the methods from service interface
        List<Method> methods = serviceInterface.methods();
        assertNotNull(methods);
        assertEquals(1, methods.size());
        assertEquals(publishBlockStream, methods.getFirst());
    }

    @Test
    void testPublisher() {
        // enable debug System.logger logging
        Logger rootLogger = LogManager.getLogManager().getLogger("");
        rootLogger.setLevel(Level.ALL);
        for (var handler : rootLogger.getHandlers()) {
            handler.setLevel(Level.ALL);
        }
        // create some sample data to send to plugin
        final BlockItem blockHeader1 = sampleBlockHeader(0);
        final Bytes publishBlockHeader1StreamRequest = blockItemsToPublishStreamRequest(blockHeader1);
        final BlockItem roundHeader1 = sampleRoundHeader(2);
        final Bytes publishRoundHeader1StreamRequest = blockItemsToPublishStreamRequest(roundHeader1);
        final BlockItem blockProof1 = sampleBlockProof(0);
        final Bytes publishBlockProof1StreamRequest = blockItemsToPublishStreamRequest(blockProof1);
        // send the data to the plugin
        toPluginPipe.onNext(publishBlockHeader1StreamRequest);
        toPluginPipe.onNext(publishRoundHeader1StreamRequest);
        toPluginPipe.onNext(publishBlockProof1StreamRequest);
        System.out.println("PublisherTest.testPublisher: sent publishBlockHeader1StreamRequest");
    }

    /**
     * Helper method to convert a block items to a publish stream request bytes for sending to the plugin.
     * @param blockItems the block items to wrap
     * @return the bytes of PublishStreamRequest to the plugin
     */
    public static Bytes blockItemsToPublishStreamRequest(BlockItem... blockItems) {
        return PublishStreamRequest.PROTOBUF.toBytes(new PublishStreamRequest(
                new OneOf<>(RequestOneOfType.BLOCK_ITEMS, new BlockItemSet(List.of(blockItems)))));
    }
}
