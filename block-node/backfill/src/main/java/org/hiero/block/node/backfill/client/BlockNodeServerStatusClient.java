// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill.client;

import static java.util.Objects.requireNonNull;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.hiero.block.api.BlockNodeServiceInterface;
import org.hiero.block.api.ServerStatusRequest;
import org.hiero.block.api.ServerStatusResponse;

public class BlockNodeServerStatusClient implements StreamObserver<ServerStatusResponse> {
    private final GrpcServiceClient serverStatusServiceClient;
    private final String methodName = BlockNodeServiceInterface.BlockNodeServiceMethod.serverStatus.name();
    // Per Request State
    private AtomicReference<ServerStatusResponse> replyRef;
    private AtomicReference<Throwable> errorRef;
    private CountDownLatch latch;

    public BlockNodeServerStatusClient(final GrpcClient grpcClient) {
        // create service client for server status
        this.serverStatusServiceClient = grpcClient.serviceClient(GrpcServiceDescriptor.builder()
                .serviceName(BlockNodeServiceInterface.FULL_NAME)
                .putMethod(
                        methodName,
                        GrpcClientMethodDescriptor.unary(BlockNodeServiceInterface.FULL_NAME, methodName)
                                .requestType(ServerStatusRequest.class)
                                .responseType(ServerStatusResponse.class)
                                .marshallerSupplier(
                                        new BlockNodeServerStatusClient.ServerStatusRequestResponseMarshaller
                                                .Supplier())
                                .build())
                .build());
    }

    public ServerStatusResponse getServerStatus() {
        // reset state for the request
        replyRef = new AtomicReference<>();
        errorRef = new AtomicReference<>();
        latch = new CountDownLatch(1);
        // Call
        serverStatusServiceClient.unary(methodName, new ServerStatusRequest(), this);
        // wait for response or error
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        // Check for error
        if (errorRef.get() != null) {
            if (errorRef.get() instanceof RuntimeException re) {
                throw re;
            }
            throw new RuntimeException(errorRef.get());
        }
        // Check for reply
        if (replyRef.get() != null) {
            return replyRef.get();
        }
        // If we reach here, it means we did not receive a reply or an error
        throw new RuntimeException("Call to serverStatus completed w/o receiving a reply or an error explicitly.");
    }

    @Override
    public void onNext(ServerStatusResponse serverStatusResponse) {
        if (replyRef.get() != null) {
            throw new IllegalStateException(
                    "serverStatus is unary, but received more than one reply. The latest reply is: "
                            + serverStatusResponse);
        }
        replyRef.set(serverStatusResponse);
        latch.countDown();
    }

    @Override
    public void onError(Throwable throwable) {
        errorRef.set(throwable);
        latch.countDown();
    }

    @Override
    public void onCompleted() {
        latch.countDown();
    }

    public static class ServerStatusRequestResponseMarshaller<T> implements MethodDescriptor.Marshaller<T> {
        private final Codec<T> codec;

        ServerStatusRequestResponseMarshaller(@NonNull final Class<T> clazz) {
            requireNonNull(clazz);

            if (clazz == ServerStatusRequest.class) {
                this.codec = (Codec<T>) ServerStatusRequest.PROTOBUF;
            } else if (clazz == ServerStatusResponse.class) {
                this.codec = (Codec<T>) ServerStatusResponse.PROTOBUF;
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
                return new ServerStatusRequestResponseMarshaller<>(clazz);
            }
        }
    }
}
