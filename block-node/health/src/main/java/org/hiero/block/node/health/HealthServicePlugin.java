// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.health;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;
import static org.hiero.block.node.health.HttpConnectionSupport.closeAfterHttp1;

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
import org.hiero.metrics.LongCounter;
import org.hiero.metrics.core.MetricKey;
import org.hiero.metrics.core.MetricRegistry;

/// Provides implementation for the health endpoints of the server.
///
/// Every HTTP/1.1 response sets the `Connection: close` header (see [HttpConnectionSupport]).
/// Kubernetes probes hit these endpoints continuously; left on keep-alive they accumulate as
/// idle connections that Helidon only reaps after `server.idleConnectionTimeoutMinutes`,
/// eventually exhausting `server.maxTcpConnections` and causing the server to refuse new
/// connections. Closing after each response keeps the probe traffic from holding sockets open.
/// Helidon flushes the full response before closing (see
/// `Http1ServerResponse.keepConnectionOpen()`), so the reply is always delivered.
public class HealthServicePlugin implements BlockNodePlugin {
    private final Logger LOGGER = System.getLogger(getClass().getName());
    protected static final String HEALTHZ_PATH = "/healthz";
    protected static final String LIVEZ_PATH = "/livez";
    protected static final String READYZ_PATH = "/readyz";
    protected static final String STATUSZ_PATH = "/statusz";
    protected static final String INBOUND_PATH = STATUSZ_PATH + "/inbound";
    protected static final String OUTBOUND_PATH = STATUSZ_PATH + "/outbound";

    /// Counter for health-check HTTP requests, labeled by [#LABEL_ENDPOINT] and [#LABEL_RESULT].
    /// Kubernetes probes flatlining this counter while the pod stays up is an early signal that the
    /// server has stopped accepting connections (see class doc), before liveness/readiness fail.
    public static final MetricKey<LongCounter> METRIC_HEALTH_REQUESTS =
            MetricKey.of("health_requests", LongCounter.class).addCategory(METRICS_CATEGORY);

    /// Label naming the endpoint group that served the request. Only the health checks
    /// (`livez`/`readyz`) are counted, so the sole value is [#ENDPOINT_HEALTH]; the `statusz`
    /// endpoints serve statistics rather than health checks and are intentionally not counted.
    static final String LABEL_ENDPOINT = "endpoint";
    /// Label naming the outcome of the request (`success`, `failed`).
    static final String LABEL_RESULT = "result";
    static final String ENDPOINT_HEALTH = "health";
    static final String RESULT_SUCCESS = "success";
    static final String RESULT_FAILED = "failed";

    /// The health facility, used for getting server status
    private HealthFacility healthFacility;

    /// The application state facility, used to obtain connection information
    /// for the statusz endpoints
    private ApplicationStateFacility applicationStateFacility;

    /// Counter tracking every request served by the health/status endpoints.
    private LongCounter requestCounter;

    /// {@inheritDoc}
    @Override
    public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
        healthFacility = context.serverHealth();
        applicationStateFacility = context.applicationStateFacility();
        final MetricRegistry metricRegistry = context.metricRegistry();
        requestCounter = metricRegistry.register(LongCounter.builder(METRIC_HEALTH_REQUESTS)
                .setDescription("Health check requests by endpoint and result")
                .addDynamicLabelNames(LABEL_ENDPOINT, LABEL_RESULT));
        // A null port (the default) shares server.port
        final Integer port =
                context.configuration().getConfigData(HealthConfig.class).port();
        serviceBuilder.registerHttpService(HEALTHZ_PATH, port, httpRules -> httpRules
                .get(LIVEZ_PATH, this::handleLivez)
                .get(READYZ_PATH, this::handleReadyz));
        serviceBuilder.registerHttpService(STATUSZ_PATH, port, httpRules -> httpRules.get("*", this::handleStatusz));
        LOGGER.log(DEBUG, "Completed health facility initialization");
    }

    /// {@inheritDoc}
    @Override
    @NonNull
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(HealthConfig.class);
    }

    /// Handles the request for liveness endpoint, that must be defined
    /// in the routing implementation.
    ///
    /// @param req the server request
    /// @param res the server response
    public final void handleLivez(@NonNull final ServerRequest req, @NonNull final ServerResponse res) {
        try {
            if (healthFacility.isRunning()) {
                closeAfterHttp1(req, res.status(200)).send("OK");
                recordRequest(RESULT_SUCCESS);
                LOGGER.log(TRACE, "Responded code 200 (OK) to liveness check");
            } else {
                closeAfterHttp1(req, res.status(503)).send("Service is not running");
                recordRequest(RESULT_FAILED);
                LOGGER.log(INFO, "Responded code 503 (Service is not running) to liveness check");
            }
        } catch (final RuntimeException e) {
            recordRequest(RESULT_FAILED);
            LOGGER.log(WARNING, "Failed to respond to liveness check due to %s".formatted(e), e);
        }
    }

    /// Handles the request for readiness endpoint, that must be defined
    /// in the routing implementation.
    ///
    /// @param req the server request
    /// @param res the server response
    public final void handleReadyz(@NonNull final ServerRequest req, @NonNull final ServerResponse res) {
        try {
            if (healthFacility.isRunning()) {
                closeAfterHttp1(req, res.status(200)).send("OK");
                recordRequest(RESULT_SUCCESS);
                LOGGER.log(TRACE, "Responded code 200 (OK) to readiness check");
            } else {
                closeAfterHttp1(req, res.status(503)).send("Service is not running");
                recordRequest(RESULT_FAILED);
                LOGGER.log(INFO, "Responded code 503 (Service is not running) to readiness check");
            }
        } catch (final RuntimeException e) {
            recordRequest(RESULT_FAILED);
            LOGGER.log(WARNING, "Failed to respond to readiness check due to %s".formatted(e), e);
        }
    }

    /// Handles requests for the `/statusz` endpoints. The request subpath
    /// selects which handler runs. The matching handler builds and sends its
    /// [org.hiero.block.api.NetworkData] response synchronously on the request thread.
    ///
    /// @param req the server request
    /// @param res the server response
    public final void handleStatusz(@NonNull final ServerRequest req, @NonNull final ServerResponse res) {
        try {
            final String path = req.path().path();
            if (path.endsWith(INBOUND_PATH)) {
                new InboundStatusHandler(req, res, applicationStateFacility).createAndSendResponse();
            } else if (path.endsWith(OUTBOUND_PATH)) {
                new OutboundStatusHandler(req, res, applicationStateFacility).createAndSendResponse();
            } else {
                LOGGER.log(INFO, "Responded code 404 (Unknown statusz subpath) for {0}", path);
                closeAfterHttp1(req, res.status(404)).send("Unknown statusz subpath");
            }
        } catch (final RuntimeException e) {
            LOGGER.log(WARNING, "Failed to respond to statusz check due to %s".formatted(e), e);
            closeAfterHttp1(req, res.status(500)).send();
        }
    }

    /// Increments [#METRIC_HEALTH_REQUESTS] for a health check ([#ENDPOINT_HEALTH]) with the
    /// given result.
    ///
    /// @param result the outcome of the request
    private void recordRequest(@NonNull final String result) {
        requestCounter
                .getOrCreateLabeled(LABEL_ENDPOINT, ENDPOINT_HEALTH, LABEL_RESULT, result)
                .increment();
    }
}
