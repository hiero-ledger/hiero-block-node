// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.pbj;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;
import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Counter.RequestsFailed;
import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Counter.RequestsReceived;

import com.hedera.hapi.block.ServerStatusRequest;
import com.hedera.hapi.block.ServerStatusResponse;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.GrpcException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.Pipelines;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;
import javax.inject.Inject;
import org.hiero.block.server.metrics.MetricsService;
import org.hiero.block.server.service.ServiceStatus;

public class PbjServerStatusServiceProxy implements PbjServerStatusService {

    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    private final ServiceStatus serviceStatus;
    private final MetricsService metricsService;

    /**
     * Creates a new PbjBlockAccessServiceProxy instance.
     *
     * @param serviceStatus the service status
     * //     * @param blockReader the block reader
     * @param metricsService the metrics service
     */
    @Inject
    public PbjServerStatusServiceProxy(
            @NonNull final ServiceStatus serviceStatus, @NonNull final MetricsService metricsService) {
        this.serviceStatus = Objects.requireNonNull(serviceStatus);
        this.metricsService = Objects.requireNonNull(metricsService);
    }

    @NonNull
    @Override
    public Pipeline<? super Bytes> open(
            @NonNull Method method, @NonNull RequestOptions requestOptions, @NonNull Pipeline<? super Bytes> replies)
            throws GrpcException {
        try {
            final var m = (ServerStatusMethod) method;
            return switch (m) {
                case serverStatus -> Pipelines.<ServerStatusRequest, ServerStatusResponse>unary()
                        .mapRequest(this::parseServerStatusRequest)
                        .method(this::serverStatus)
                        .mapResponse(this::createServerStatusResponse)
                        .respondTo(replies)
                        .build();
            };
        } catch (Exception e) {
            replies.onError(e);
            return Pipelines.noop();
        }
    }

    @NonNull
    private Bytes createServerStatusResponse(@NonNull final ServerStatusResponse serverStatusResponse) {
        return ServerStatusResponse.PROTOBUF.toBytes(serverStatusResponse);
    }

    ServerStatusResponse serverStatus(ServerStatusRequest serverStatusRequest) {
        LOGGER.log(DEBUG, "Executing Unary serverStatus gRPC method");
        if (serviceStatus.isRunning()) {
            metricsService.get(RequestsReceived).increment();
            return ServerStatusResponse.newBuilder()
                    .firstAvailableBlock(serviceStatus.getFirstAvailableBlockNumber())
                    .lastAvailableBlock(serviceStatus.getLatestReceivedBlockNumber())
                    .onlyLatestState(serviceStatus.getOnlyLatestState())
                    .versionInformation(serviceStatus.getVersionInformation())
                    .build();
        } else {
            metricsService.get(RequestsFailed).increment();
            LOGGER.log(ERROR, "Unary serverStatus gRPC method is not currently running");

            return ServerStatusResponse.newBuilder().build();
        }
    }

    @NonNull
    private ServerStatusRequest parseServerStatusRequest(@NonNull final Bytes message) throws ParseException {
        return ServerStatusRequest.PROTOBUF.parse(message);
    }
}
