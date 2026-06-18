// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.health;

import static java.lang.System.Logger.Level.TRACE;

import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.http.HeaderNames;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import java.lang.System.Logger;
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.api.NetworkConnection;
import org.hiero.block.api.NetworkData;
import org.hiero.block.node.spi.ApplicationStateFacility;

/**
 * Builds and sends the {@code /statusz/inbound} response: the merge of the known publishers, the
 * inbound designated partners, and the backfill sources, serialized as {@link NetworkData} JSON.
 *
 * <p>This is an independent, {@code final} class with no shared base or interface (to avoid
 * virtual-call overhead). Its single {@link #createAndSendResponse()} entry point is invoked
 * synchronously on the (virtual) request thread by {@link HealthServicePlugin}.
 */
final class InboundStatusHandler {
    private final Logger LOGGER = System.getLogger(getClass().getName());
    private final ServerRequest request;
    private final ServerResponse response;
    private final ApplicationStateFacility appState;

    InboundStatusHandler(
            @NonNull final ServerRequest request,
            @NonNull final ServerResponse response,
            @NonNull final ApplicationStateFacility appState) {
        this.request = request;
        this.response = response;
        this.appState = appState;
    }

    /** Builds the inbound {@link NetworkData} and sends it as an {@code application/json} response. */
    void createAndSendResponse() {
        final List<NetworkConnection> endpoints = new ArrayList<>();
        endpoints.addAll(appState.knownPublishers().activeEndpoints());
        endpoints.addAll(appState.inboundPartners().activeEndpoints());
        endpoints.addAll(appState.backfillSources().activeEndpoints());
        final NetworkData data =
                NetworkData.newBuilder().activeEndpoints(endpoints).build();
        response.status(200)
                .header(HeaderNames.CONTENT_TYPE, "application/json")
                .send(NetworkData.JSON.toJSON(data));
        LOGGER.log(
                TRACE,
                "Responded to {0} with {1} inbound endpoints",
                request.path().path(),
                endpoints.size());
    }
}
