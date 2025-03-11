// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.health;

import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;

/** Defines the contract for health http service, needed for implementing an standard */
public interface HealthService extends HttpService {
    /**
     * The path for the health group endpoints. Root path for all health endpoints.
     *
     * @return the root path for the health group endpoints
     */
    @NonNull
    String getHealthRootPath();

    /**
     * Handles the request for liveness endpoint, that it most be defined on routing implementation.
     *
     * @param req the server request
     * @param res the server response
     */
    void handleLivez(@NonNull final ServerRequest req, @NonNull final ServerResponse res);

    /**
     * Handles the request for readiness endpoint, that it most be defined on routing
     * implementation.
     *
     * @param req the server request
     * @param res the server response
     */
    void handleReadyz(@NonNull final ServerRequest req, @NonNull final ServerResponse res);
}
