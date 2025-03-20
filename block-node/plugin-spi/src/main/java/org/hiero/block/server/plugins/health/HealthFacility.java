package org.hiero.block.server.plugins.health;

/**
 * The status service is used to get the current status of the block node.
 */
public interface HealthFacility {
    /**
     * The status of the block node.
     */
    enum State {
        /** The block node is starting up. */
        STARTING,
        /** The block node is running. */
        RUNNING,
        /** The block node is shutting down. */
        SHUTTING_DOWN,
    }

    /**
     * Use this method to get the current status of the block node.
     *
     * @return the current status of the block node
     */
    State blockNodeState();

    /**
     * Checks if the service is running.
     *
     * @return true if the service is running, false otherwise
     */
    default boolean isRunning() {
        return blockNodeState() == State.RUNNING;
    }

    /**
     * Shutdown the block node. This method is called to start shutting down the block node.
     *
     * @param className the name of the class stopping the service, for tracing shutdown reason
     * @param reason the reason for shutting down the block node
     */
    void shutdown(final String className, final String reason);
}

