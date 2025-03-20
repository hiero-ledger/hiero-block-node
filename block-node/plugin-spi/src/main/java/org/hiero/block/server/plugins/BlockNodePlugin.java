package org.hiero.block.server.plugins;

import io.helidon.common.Builder;
import io.helidon.webserver.Routing;
import java.util.List;

/**
 * Interface for all block node plugins to implement. Plugins are registered as module services that provide this interface.
 */
public interface BlockNodePlugin {
    /**
     * The name of the plugin.
     *
     * @return the name of the plugin
     */
    String name();

    /**
     * Start the plugin. This method is called when the block node is starting up. It provides the block node context to
     * the plugin which can be used to access the different facilities of the block node. It returns a list of routing
     * builders that will be used to create the HTTP routing for the block node.
     *
     * @param context the block node context
     * @return a list of routing builders that will be used to create the HTTP routing for the block node
     */
    List<Builder<?, ? extends Routing>> start(BlockNodeContext context);

    /**
     * Stop the plugin. This method is called when the block node is shutting down. This expectation is to do the
     * minimum for a clean shutdown stopping all threads as need.
     */
    void stop();
}
