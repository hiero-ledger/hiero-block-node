// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.health;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;

import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import java.lang.System.Logger;
import java.util.List;
import org.hiero.block.node.spi.ApplicationStateFacility;
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
    protected static final String STATUSZ_PATH = "/statusz";
    protected static final String INBOUND_PATH = "/inbound";
    protected static final String OUTBOUND_PATH = "/outbound";

    /** The health facility, used for getting server status */
    private HealthFacility healthFacility;

    /** The application state facility, used to obtain connection information for the statusz endpoints */
    private ApplicationStateFacility applicationStateFacility;

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
        healthFacility = context.serverHealth();
        applicationStateFacility = context.applicationStateFacility();
        // A null port (the default) shares server.port
        final Integer port =
                context.configuration().getConfigData(HealthConfig.class).port();
        serviceBuilder.registerHttpService(HEALTHZ_PATH, port, httpRules -> httpRules
                .get(LIVEZ_PATH, this::handleLivez)
                .get(READYZ_PATH, this::handleReadyz));
        serviceBuilder.registerHttpService(STATUSZ_PATH, port, httpRules -> httpRules
                .get(INBOUND_PATH, this::handleStatusz)
                .get(OUTBOUND_PATH, this::handleStatusz));
        LOGGER.log(DEBUG, "Completed health facility initialization");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(HealthConfig.class);
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

    /**
     * Handles requests for the {@code /statusz/inbound} and {@code /statusz/outbound} endpoints. Both
     * routes dispatch here; the request path selects which handler runs. The matching handler builds and
     * sends its {@link org.hiero.block.api.NetworkData} response synchronously on the (virtual) request
     * thread.
     *
     * @param req the server request
     * @param res the server response
     */
    public final void handleStatusz(@NonNull final ServerRequest req, @NonNull final ServerResponse res) {
        try {
            final String path = req.path().path();
            if (path.endsWith(INBOUND_PATH)) {
                new InboundStatusHandler(req, res, applicationStateFacility).createAndSendResponse();
            } else if (path.endsWith(OUTBOUND_PATH)) {
                new OutboundStatusHandler(req, res, applicationStateFacility).createAndSendResponse();
            } else {
                res.status(404).send("Unknown statusz subpath");
                LOGGER.log(INFO, "Responded code 404 (Unknown statusz subpath) for {0}", path);
            }
        } catch (final RuntimeException e) {
            LOGGER.log(WARNING, "Failed to respond to statusz check due to %s".formatted(e), e);
        }
    }
}
