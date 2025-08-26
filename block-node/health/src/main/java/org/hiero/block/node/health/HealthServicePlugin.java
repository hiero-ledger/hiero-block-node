// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.health;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;

import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import java.lang.System.Logger;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.health.HealthFacility;

/** Provides implementation for the health endpoints of the server. */
public class HealthServicePlugin implements BlockNodePlugin {
    private final Logger LOGGER = System.getLogger(getClass().getName());
    protected static final String HEALTHZ_PATH = "/healthz";
    protected static final String LIVEZ_PATH = "/livez";
    protected static final String READYZ_PATH = "/readyz";

    /** The health facility, used for getting server status */
    private HealthFacility healthFacility;

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
        healthFacility = context.serverHealth();
        serviceBuilder.registerHttpService(
                HEALTHZ_PATH,
                httpRules -> httpRules.get(LIVEZ_PATH, this::handleLivez).get(READYZ_PATH, this::handleReadyz));
        LOGGER.log(TRACE, "Completed health facility initialization");
    }

    /**
     * Handles the request for liveness endpoint, that it most be defined on routing implementation.
     *
     * @param req the server request
     * @param res the server response
     */
    public final void handleLivez(@NonNull final ServerRequest req, @NonNull final ServerResponse res) {
        try {
            if (healthFacility.isRunning()) {
                res.status(200).send("OK");
                LOGGER.log(TRACE, "Responded code 200 (OK) to liveness check");
            } else {
                res.status(503).send("Service is not running");
                LOGGER.log(INFO, "Responded code 503 (Service is not running) to liveness check");
            }
        } catch (final RuntimeException e) {
            LOGGER.log(WARNING, "Failed to respond to liveness check due to %s".formatted(e), e);
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
        try {
            if (healthFacility.isRunning()) {
                res.status(200).send("OK");
                LOGGER.log(TRACE, "Responded code 200 (OK) to readiness check");
            } else {
                res.status(503).send("Service is not running");
                LOGGER.log(INFO, "Responded code 503 (Service is not running) to readiness check");
            }
        } catch (final RuntimeException e) {
            LOGGER.log(WARNING, "Failed to respond to readiness check due to %s".formatted(e), e);
        }
    }
}
