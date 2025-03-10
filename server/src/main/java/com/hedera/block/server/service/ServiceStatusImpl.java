// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.service;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;

import com.hedera.block.server.block.BlockInfo;
import com.hedera.hapi.block.BlockNodeVersions;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.webserver.WebServer;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * The ServiceStatusImpl class implements the ServiceStatus interface. It provides the
 * implementation for checking the status of the service and shutting down the web server.
 */
@Singleton
public class ServiceStatusImpl implements ServiceStatus {
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final int delayMillis;
    private WebServer webServer;
    private volatile BlockInfo latestAckedBlock;
    private volatile long latestReceivedBlockNumber;
    private volatile long firstAvailableBlockNumber = Long.MIN_VALUE;
    private volatile long lastAvailableBlockNumber = Long.MIN_VALUE;
    private volatile boolean onlyLatestState;
    private BlockNodeVersions blockNodeVersions;

    /**
     * Use the ServiceStatusImpl to check the status of the block node server and to shut it down if
     * necessary.
     *
     * @param serviceConfig the service configuration
     */
    @Inject
    public ServiceStatusImpl(@NonNull final ServiceConfig serviceConfig) {
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

    @Override
    public BlockInfo getLatestAckedBlock() {
        return latestAckedBlock;
    }

    @Override
    public void setLatestAckedBlock(BlockInfo latestAckedBlock) {
        this.latestAckedBlock = latestAckedBlock;
    }

    @Override
    public long getLatestReceivedBlockNumber() {
        return latestReceivedBlockNumber;
    }

    @Override
    public void setLatestReceivedBlockNumber(long latestReceivedBlockNumber) {
        this.latestReceivedBlockNumber = latestReceivedBlockNumber;
    }

    @Override
    public long getFirstAvailableBlockNumber() {
        return firstAvailableBlockNumber;
    }

    @Override
    public void setFirstAvailableBlockNumber(final long firstAvailableBlockNumber) {
        this.firstAvailableBlockNumber = firstAvailableBlockNumber;
    }

    @Override
    public long getLastAvailableBlockNumber() {
        return lastAvailableBlockNumber;
    }

    @Override
    public void setLastAvailableBlockNumber(long lastAvailableBlockNumber) {
        this.lastAvailableBlockNumber = lastAvailableBlockNumber;
    }

    @Override
    public boolean getOnlyLatestState() {
        return onlyLatestState;
    }

    @Override
    public void setOnlyLatestState(boolean onlyLatestState) {
        this.onlyLatestState = onlyLatestState;
    }

    @Override
    public BlockNodeVersions getVersionInformation() {
        return blockNodeVersions;
    }

    @Override
    public void setVersionInformation(BlockNodeVersions versionInformation) {
        this.blockNodeVersions = versionInformation;
    }
}
