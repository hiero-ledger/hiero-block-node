// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.health;

import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.health.HealthFacility;

/** Provides implementation for the health endpoints of the server. */
public class HealthServicePlugin implements BlockNodePlugin {

    // TODO maybe these want to be package protected so they can be used in tests, as they are never module exported
    // then that is fine
    private static final String HEALTH_PATH = "/healthz";
    private static final String LIVEZ_PATH = "/livez";
    private static final String READYZ_PATH = "/readyz";

    /** The health facility, used for getting server status */
    private HealthFacility healthFacility;

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
        healthFacility = context.serverHealth();
        serviceBuilder.registerHttpService(
                HEALTH_PATH,
                httpRules -> httpRules.get(LIVEZ_PATH, this::handleLivez).get(READYZ_PATH, this::handleReadyz));
    }

    /**
     * Handles the request for liveness endpoint, that it most be defined on routing implementation.
     *
     * @param req the server request
     * @param res the server response
     */
    public final void handleLivez(@NonNull final ServerRequest req, @NonNull final ServerResponse res) {
        if (healthFacility.isRunning()) {
            res.status(200).send("OK");
        } else {
            res.status(503).send("Service is not running");
        }
    }

    /**
     * Handles the request for readiness endpoint, that it most be defined on routing
     * implementation.
     *
     * @param req the server request
     * @param res the server response
     */
    public final void handleReadyz(@NonNull final ServerRequest req, @NonNull final ServerResponse res) {
        if (healthFacility.isRunning()) {
            res.status(200).send("OK");
        } else {
            res.status(503).send("Service is not running");
        }
    }
}
