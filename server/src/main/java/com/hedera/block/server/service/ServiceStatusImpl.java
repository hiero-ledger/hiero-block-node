// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.service;

import com.hedera.block.server.block.BlockInfo;
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
    private WebServer webServer;
    private volatile BlockInfo latestAckedBlock;
    private volatile long latestReceivedBlockNumber;
    private final int delayMillis;

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
}
