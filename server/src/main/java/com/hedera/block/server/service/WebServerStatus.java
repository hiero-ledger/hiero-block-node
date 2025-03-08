// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.service;

import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.webserver.WebServer;

public interface WebServerStatus {

    /**
     * Checks if the service is running.
     *
     * @return true if the service is running, false otherwise
     */
    boolean isRunning();

    /**
     * Sets the running status of the service.
     *
     * @param className the name of the class stopping the service
     */
    void stopRunning(final String className);

    /**
     * Sets the web server instance.
     *
     * @param webServer the web server instance
     */
    void setWebServer(@NonNull final WebServer webServer);

    /**
     * Stops the service and web server. This method is called to shut down the service and the web
     * server in the event of an error or when the service needs to restart.
     *
     * @param className the name of the class stopping the service
     */
    void stopWebServer(final String className);
}
