// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill.client;

import static java.util.Objects.requireNonNull;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.Codec;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;
import io.helidon.grpc.core.MarshallerSupplier;
import io.helidon.webclient.grpc.GrpcClient;
import io.helidon.webclient.grpc.GrpcClientMethodDescriptor;
import io.helidon.webclient.grpc.GrpcServiceClient;
import io.helidon.webclient.grpc.GrpcServiceDescriptor;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.block.api.BlockStreamSubscribeServiceInterface;
import org.hiero.block.api.SubscribeStreamRequest;
import org.hiero.block.api.SubscribeStreamResponse;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.internal.SubscribeStreamResponseUnparsed;

public class BlockNodeSubscribeClient implements StreamObserver<SubscribeStreamResponseUnparsed> {
    private final GrpcServiceClient blockStreamSubscribeServiceClient;
    private final String methodName =
            BlockStreamSubscribeServiceInterface.BlockStreamSubscribeServiceMethod.subscribeBlockStream.name();
    // Per Request State
    private List<BlockItemUnparsed> currentBlockItems;
    private AtomicLong currentBlockNumber;
    private AtomicReference<List<BlockUnparsed>> replyRef;
    private AtomicReference<Throwable> errorRef;
    private CountDownLatch latch;

    public BlockNodeSubscribeClient(GrpcClient grpcClient) {
        // create service client for server status
        this.blockStreamSubscribeServiceClient = grpcClient.serviceClient(GrpcServiceDescriptor.builder()
                .serviceName(BlockStreamSubscribeServiceInterface.FULL_NAME)
                .putMethod(
                        methodName,
                        GrpcClientMethodDescriptor.serverStreaming(
                                        BlockStreamSubscribeServiceInterface.FULL_NAME, methodName)
                                .requestType(SubscribeStreamRequest.class)
                                .responseType(SubscribeStreamResponseUnparsed.class)
                                .marshallerSupplier(new BlockStreamSubscribeMarshaller.Supplier())
                                .build())
                .build());
    }

    public List<BlockUnparsed> getBatchOfBlocks(long startBlockNumber, long endBlockNumber) {
        // Validate input parameters
        if (startBlockNumber < 0 || endBlockNumber < 0 || startBlockNumber > endBlockNumber) {
            throw new IllegalArgumentException("Invalid block range: " + startBlockNumber + " to " + endBlockNumber);
        }
        // reset state for the request
        currentBlockItems = new ArrayList<>();
        currentBlockNumber = new AtomicLong(startBlockNumber);
        replyRef = new AtomicReference<>();
        errorRef = new AtomicReference<>();
        latch = new CountDownLatch(1);
        // Create request
        SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                .startBlockNumber(startBlockNumber)
                .endBlockNumber(endBlockNumber)
                .build();
        // Call
        blockStreamSubscribeServiceClient.serverStream(methodName, request, this);

        // wait for response or error
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for blocks", e);
        }
        if (errorRef.get() != null) {
            throw new RuntimeException("Error fetching blocks", errorRef.get());
        }
        return replyRef.get();
    }

    @Override
    public void onNext(SubscribeStreamResponseUnparsed subscribeStreamResponse) {
        if (subscribeStreamResponse.hasBlockItems()) {
            List<BlockItemUnparsed> blockItems =
                    subscribeStreamResponse.blockItems().blockItems();
            // Check if is new Block
            if (blockItems.getFirst().hasBlockHeader()) {
                // verify is the expected block number
                long expectedBlockNumber = currentBlockNumber.get();
                long actualBlockNumber = 0;
                try {
                    actualBlockNumber = BlockHeader.PROTOBUF
                            .parse(blockItems.getFirst().blockHeaderOrThrow())
                            .number();
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
                if (actualBlockNumber != expectedBlockNumber) {
                    throw new IllegalStateException(
                            "Expected block number " + expectedBlockNumber + " but received " + actualBlockNumber);
                }
                // Create new Block and add to current block items
                currentBlockItems = new ArrayList<>(blockItems);
            } else {
                // Add items to current block
                currentBlockItems.addAll(blockItems);
            }

            // Check if response contains block proof (end of block)
            if (blockItems.getLast().hasBlockProof()) {
                // Create Block from current items
                BlockUnparsed block =
                        BlockUnparsed.newBuilder().blockItems(currentBlockItems).build();
                // Add to reply
                List<BlockUnparsed> blocks = replyRef.get();
                if (blocks == null) {
                    blocks = new ArrayList<>();
                    replyRef.set(blocks);
                }
                blocks.add(block);
                // Reset current block items and number for next block
                currentBlockItems = new ArrayList<>();
                currentBlockNumber.incrementAndGet();
            }

        } else if (subscribeStreamResponse.hasStatus()) {
            // If response has code, set the status
            SubscribeStreamResponse.Code codeStatus = subscribeStreamResponse.status();
            if (codeStatus != SubscribeStreamResponse.Code.SUCCESS) {
                errorRef.set(new RuntimeException("Received error code: " + codeStatus));
            }
        } else {
            // If no block items and no code, this is unexpected
            errorRef.set(new RuntimeException("Received unexpected response without block items or code"));
        }
    }

    @Override
    public void onError(Throwable throwable) {
        errorRef.set(throwable);
        replyRef.set(null);
        latch.countDown();
    }

    @Override
    public void onCompleted() {
        latch.countDown();
    }

    public static class BlockStreamSubscribeMarshaller<T> implements MethodDescriptor.Marshaller<T> {
        private final Codec<T> codec;

        public BlockStreamSubscribeMarshaller(@NonNull final Class<T> clazz) {
            requireNonNull(clazz);

            if (clazz == SubscribeStreamRequest.class) {
                this.codec = (Codec<T>) SubscribeStreamRequest.PROTOBUF;
            } else if (clazz == SubscribeStreamResponseUnparsed.class) {
                this.codec = (Codec<T>) SubscribeStreamResponseUnparsed.PROTOBUF;
            } else {
                throw new IllegalArgumentException("Unsupported class: " + clazz.getName());
            }
        }

        @Override
        public InputStream stream(@NonNull final T obj) {
            requireNonNull(obj);
            return codec.toBytes(obj).toInputStream();
        }

        @Override
        public T parse(@NonNull final InputStream inputStream) {
            requireNonNull(inputStream);

            try (inputStream) {
                return codec.parse(Bytes.wrap(inputStream.readAllBytes()));
            } catch (final ParseException | IOException e) {
                throw new RuntimeException(e);
            }
        }

        public static class Supplier implements MarshallerSupplier {
            @Override
            public <T> MethodDescriptor.Marshaller<T> get(@NonNull final Class<T> clazz) {
                return new BlockStreamSubscribeMarshaller<>(clazz);
            }
        }
    }
}
