// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.service;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;

import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.webserver.WebServer;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class WebServerStatusImpl implements WebServerStatus {

    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private WebServer webServer;
    private final int delayMillis;

    @Inject
    public WebServerStatusImpl(@NonNull final ServiceConfig serviceConfig) {
        this.delayMillis = serviceConfig.shutdownDelayMillis();
    }

    /**
     * Checks if the service is running.
     *
     * @return true if the service is running, false otherwise
     */
    public boolean isRunning() {
        return isRunning.get();
    }

    /**
     * Sets the running status of the service.
     *
     * @param className the name of the class stopping the service
     */
    public void stopRunning(final String className) {
        LOGGER.log(DEBUG, String.format("%s set the status to stopped", className));
        isRunning.set(false);
    }

    /**
     * Sets the web server instance.
     *
     * @param webServer the web server instance
     */
    public void setWebServer(@NonNull final WebServer webServer) {
        this.webServer = webServer;
    }

    /**
     * Stops the service and web server. This method is called to shut down the service and the web
     * server in the event of an unrecoverable exception or during expected maintenance.
     *
     * @param className the name of the class stopping the service
     */
    public void stopWebServer(@NonNull final String className) {

        LOGGER.log(DEBUG, String.format("%s is stopping the server", className));

        // Flag the service to stop
        // accepting new connections
        isRunning.set(false);

        try {
            // Delay briefly while outbound termination messages
            // are sent to the consumers and producers, etc.
            Thread.sleep(delayMillis);
        } catch (InterruptedException e) {
            LOGGER.log(ERROR, "An exception was thrown waiting to shut down the server: ", e);
        }

        // Stop the web server
        webServer.stop();
    }
}
