// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.ConfigurationBuilder;
import org.hiero.block.api.BlockItemSet;
import org.hiero.block.api.PublishStreamRequest;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockItemUnparsed.ItemOneOfType;
import org.hiero.block.internal.PublishStreamRequestUnparsed;
import org.hiero.block.node.app.config.ServerConfig;
import org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PublishStreamRequestCompatabilityTest {
    private static int maxMessageSize;
    private static Bytes defaultBlockFooterBytes;
    private Bytes publishStreamRequestBytes;
    private int blockItemCount;

    @BeforeAll
    public static void setup() {
        final ServerConfig serverConfig = ConfigurationBuilder.create()
                .withConfigDataType(ServerConfig.class)
                .build()
                .getConfigData(ServerConfig.class);
        ;
        maxMessageSize = serverConfig.maxMessageSizeBytes() - 16384;
        defaultBlockFooterBytes = Bytes.wrap("default_footer".getBytes());
    }

    @BeforeEach
    public void beforeEach() throws ParseException {
        // craft block item producer request
        BlockItem blockHeader = SimpleTestBlockItemBuilder.sampleBlockHeader(0);
        // pre 0-68 footer item is not a known type so pass the bytes along
        Bytes footerItemBytes = BlockItemUnparsed.PROTOBUF.toBytes(BlockItemUnparsed.newBuilder()
                .blockFooter(defaultBlockFooterBytes)
                .build());
        BlockItem blockProof = SimpleTestBlockItemBuilder.sampleBlockProof(0);

        // craft publisher stream request with future block item to be sent over the wire
        BlockItemSet blockItemSet = BlockItemSet.newBuilder()
                .blockItems(
                        blockHeader,
                        BlockItem.PROTOBUF.parse(
                                footerItemBytes.toReadableSequentialData(),
                                false,
                                true,
                                maxMessageSize / 8,
                                maxMessageSize),
                        blockProof)
                .build();
        this.blockItemCount = blockItemSet.blockItems().size();

        PublishStreamRequest publishStreamRequest =
                PublishStreamRequest.newBuilder().blockItems(blockItemSet).build();
        this.publishStreamRequestBytes = PublishStreamRequest.PROTOBUF.toBytes(publishStreamRequest);
    }

    @Test
    public void testBlockItemUnknownFieldsEnabledPre068() throws ParseException {
        BlockItemUnparsed footer = BlockItemUnparsed.newBuilder()
                .blockFooter(Bytes.wrap("footer_item_type".getBytes()))
                .build();
        Bytes footerItemBytes = BlockItemUnparsed.PROTOBUF.toBytes(footer);

        BlockItem blockItem = BlockItem.PROTOBUF.parse(
                footerItemBytes.toReadableSequentialData(), false, true, maxMessageSize / 8, maxMessageSize);
        assertThat(blockItem).isNotNull();
        assertThat(blockItem.getUnknownFields()).isNotNull();
        assertThat(blockItem.getUnknownFields().size()).isEqualTo(1);
    }

    @Test
    public void testBlockItemUnknownFieldsDisabledPre068() throws ParseException {
        BlockItemUnparsed footer = BlockItemUnparsed.newBuilder()
                .blockFooter(Bytes.wrap("footer_item_type".getBytes()))
                .build();
        Bytes footerItemBytes = BlockItemUnparsed.PROTOBUF.toBytes(footer);

        BlockItem blockItem = BlockItem.PROTOBUF.parse(
                footerItemBytes.toReadableSequentialData(), false, false, maxMessageSize / 8, maxMessageSize);
        assertThat(blockItem).isNotNull();
        assertThat(blockItem.getUnknownFields()).isNotNull();
        assertThat(blockItem.getUnknownFields().size()).isEqualTo(0);
    }

    @Test
    public void testPublishStreamRequestUnparsedUnknownEnabled() throws ParseException {
        // parse as unparsed to simulate publisher plugin operation and confirm future proofing
        PublishStreamRequestUnparsed publishStreamRequestUnparsed = PublishStreamRequestUnparsed.PROTOBUF.parse(
                this.publishStreamRequestBytes.toReadableSequentialData(),
                false, // strictMode
                true, // parseUnknownFields
                maxMessageSize / 8,
                maxMessageSize);

        // confirm we can get the block item back out
        assertThat(publishStreamRequestUnparsed).isNotNull();
        assertThat(publishStreamRequestUnparsed.blockItems()).isNotNull();
        assertThat(publishStreamRequestUnparsed.blockItems().blockItems()).isNotNull();
        assertThat(publishStreamRequestUnparsed.blockItems().blockItems().size())
                .isEqualTo(this.blockItemCount);
        BlockItemUnparsed blockItem =
                publishStreamRequestUnparsed.blockItems().blockItems().get(1);
        assertThat(blockItem.item()).isEqualTo(new OneOf<>(ItemOneOfType.BLOCK_FOOTER, defaultBlockFooterBytes));
        assertThat(blockItem.getUnknownFields().size()).isEqualTo(0);
    }

    @Test
    public void testPublishStreamRequestUnparsedUnknownDisabled() throws ParseException {
        // parse as unparsed to simulate publisher plugin operation and confirm future proofing
        PublishStreamRequestUnparsed publishStreamRequestUnparsed = PublishStreamRequestUnparsed.PROTOBUF.parse(
                this.publishStreamRequestBytes.toReadableSequentialData(),
                false, // strictMode
                false, // parseUnknownFields
                maxMessageSize / 8,
                maxMessageSize);

        // confirm we can get the block item back out
        assertThat(publishStreamRequestUnparsed).isNotNull();
        assertThat(publishStreamRequestUnparsed.blockItems()).isNotNull();
        assertThat(publishStreamRequestUnparsed.blockItems().blockItems()).isNotNull();
        assertThat(publishStreamRequestUnparsed.blockItems().blockItems().size())
                .isEqualTo(this.blockItemCount);
        BlockItemUnparsed blockItem =
                publishStreamRequestUnparsed.blockItems().blockItems().get(1);
        assertThat(blockItem.item()).isEqualTo(new OneOf<>(ItemOneOfType.BLOCK_FOOTER, defaultBlockFooterBytes));
        assertThat(blockItem.getUnknownFields().size()).isEqualTo(0);
    }
}
