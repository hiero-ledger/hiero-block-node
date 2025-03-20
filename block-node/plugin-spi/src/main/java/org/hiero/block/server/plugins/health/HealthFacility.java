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
}

