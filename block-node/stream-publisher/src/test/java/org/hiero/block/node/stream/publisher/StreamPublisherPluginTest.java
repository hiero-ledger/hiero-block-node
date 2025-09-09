// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hiero.block.node.app.fixtures.TestUtils.enableDebugLogging;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.UncheckedParseException;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.hiero.block.api.BlockItemSet;
import org.hiero.block.api.PublishStreamRequest;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.api.PublishStreamResponse.EndOfStream.Code;
import org.hiero.block.api.PublishStreamResponse.ResponseOneOfType;
import org.hiero.block.internal.BlockItemSetUnparsed;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.PublishStreamRequestUnparsed;
import org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder;
import org.hiero.block.node.app.fixtures.plugintest.GrpcPluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests for the {@link StreamPublisherPlugin}.
 */
@DisplayName("StreamPublisherPlugin Tests")
class StreamPublisherPluginTest {
    /**
     * Enable debug logging for each test.
     */
    @BeforeEach
    void setup() {
        enableDebugLogging();
    }

    /**
     * Test for the {@link StreamPublisherPlugin} plugin.
     */
    @Nested
    @DisplayName("Plugin Tests")
    class PluginTest extends GrpcPluginTestBase<StreamPublisherPlugin, ExecutorService> {
        // ASSERTION MAPPERS
        private final Function<Bytes, PublishStreamResponse> bytesToPublishStreamResponseMapper = bytes -> {
            try {
                return PublishStreamResponse.PROTOBUF.parse(bytes);
            } catch (final ParseException e) {
                throw new UncheckedParseException(e);
            }
        };
        // ASSERTION EXTRACTORS
        private final Function<PublishStreamResponse, ResponseOneOfType> responseKindExtractor =
                response -> response.response().kind();
        private final Function<PublishStreamResponse, Code> endStreamResponseCodeExtractor =
                response -> Objects.requireNonNull(response.endStream()).status();

        /**
         * Constructor for the plugin tests.
         */
        PluginTest() {
            super(Executors.newSingleThreadExecutor());
            final StreamPublisherPlugin toTest = new StreamPublisherPlugin();
            final SimpleInMemoryHistoricalBlockFacility historicalBlockFacility =
                    new SimpleInMemoryHistoricalBlockFacility();
            start(toTest, toTest.methods().getFirst(), historicalBlockFacility);
        }

        /**
         * Verifies that the service interface correctly registers and exposes
         * the server status method.
         */
        @Test
        @DisplayName("Test verify correct method/s registered for StreamPublisherPlugin in test base")
        void testVerifyCorrectMethodRegistered() {
            assertThat(serviceInterface)
                    .isNotNull()
                    .extracting(ServiceInterface::methods)
                    .asInstanceOf(InstanceOfAssertFactories.LIST)
                    .hasSize(1)
                    .containsExactly(plugin.methods().getFirst())
                    .actual()
                    .forEach(m -> System.out.println("Methods registered for plugin tests: " + m));
        }

        /**
         * This test aims to verify that when null block items are published to
         * the pipeline, an
         * {@link PublishStreamResponse.EndOfStream}
         * response is returned with code {@link Code#INVALID_REQUEST}.
         */
        @Test
        @DisplayName("Test publish null block items")
        void testPublishNullItems() {
            // Build a PublishStreamRequest with null block items
            final PublishStreamRequest request = PublishStreamRequest.newBuilder()
                    .blockItems(BlockItemSet.newBuilder()
                            .blockItems((List<BlockItem>) null)
                            .build())
                    .build();
            // Send the request to the pipeline
            toPluginPipe.onNext(PublishStreamRequest.PROTOBUF.toBytes(request));
            // Assert response
            assertThat(fromPluginBytes)
                    .hasSize(1)
                    .first()
                    .extracting(bytesToPublishStreamResponseMapper)
                    .isNotNull()
                    .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                    .returns(Code.INVALID_REQUEST, endStreamResponseCodeExtractor);
        }

        /**
         * This test aims to verify that when empty block items are published to
         * the pipeline, an
         * {@link PublishStreamResponse.EndOfStream}
         * response is returned with code {@link Code#INVALID_REQUEST}.
         */
        @Test
        @DisplayName("Test publish empty block items")
        void testPublishEmptyItems() {
            // Build a PublishStreamRequest with empty block items
            final PublishStreamRequest request = PublishStreamRequest.newBuilder()
                    .blockItems(BlockItemSet.newBuilder()
                            .blockItems(Collections.emptyList())
                            .build())
                    .build();
            // Send the request to the pipeline
            toPluginPipe.onNext(PublishStreamRequest.PROTOBUF.toBytes(request));
            // Assert response
            assertThat(fromPluginBytes)
                    .hasSize(1)
                    .first()
                    .extracting(bytesToPublishStreamResponseMapper)
                    .isNotNull()
                    .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                    .returns(Code.INVALID_REQUEST, endStreamResponseCodeExtractor);
        }

        /**
         * This test aims to verify that when a request with unset oneOf is
         * published to the pipeline, an
         * {@link PublishStreamResponse.EndOfStream}
         * response is returned with code {@link Code#ERROR}.
         */
        @Test
        @DisplayName("Test publish unset oneOf")
        void testPublishUnsetOneOf() {
            // Build a PublishStreamRequest with an unset oneOf
            final PublishStreamRequest request =
                    PublishStreamRequest.newBuilder().build();
            // Send the request to the pipeline
            toPluginPipe.onNext(PublishStreamRequest.PROTOBUF.toBytes(request));
            // Assert response
            assertThat(fromPluginBytes)
                    .hasSize(1)
                    .first()
                    .extracting(bytesToPublishStreamResponseMapper)
                    .isNotNull()
                    .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                    .returns(Code.ERROR, endStreamResponseCodeExtractor);
        }

        /**
         * This test aims to verify that when a valid block is published to the
         * pipeline, a {@link PublishStreamResponse.BlockAcknowledgement}
         * response is returned.
         */
        @Test
        @DisplayName("Test publish a valid block as items")
        void testPublishValidBlock() {
            final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(0, 1);
            // Build a PublishStreamRequest with a valid block as items
            final BlockItemSetUnparsed blockItems =
                    BlockItemSetUnparsed.newBuilder().blockItems(block).build();
            final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                    .blockItems(blockItems)
                    .build();
            // Send the request to the pipeline
            toPluginPipe.onNext(PublishStreamRequestUnparsed.PROTOBUF.toBytes(request));
            parkNanos(500_000_000L);
            // Await to ensure async execution and assert response
            assertThat(fromPluginBytes)
                    .hasSize(1)
                    .first()
                    .extracting(bytesToPublishStreamResponseMapper)
                    .isNotNull()
                    .returns(ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor);
        }
    }
}
