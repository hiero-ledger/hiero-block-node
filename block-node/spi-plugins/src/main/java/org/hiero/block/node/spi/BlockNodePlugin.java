// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.hiero.block.api.BlockNodeVersions.PluginVersion;
import org.hiero.block.common.utils.SemanticVersionUtilities;

/**
 * Interface for all block node plugins to implement. Plugins are registered as module services that provide this interface.
 */
public interface BlockNodePlugin {
    /**
     * The name of the metrics category for all the block node metrics. Having here makes it very easy for plugins to
     * create metrics.
     */
    String METRICS_CATEGORY = "hiero_block_node";

    /**
     * Special value for block number that indicates that the block number is unknown.
     */
    long UNKNOWN_BLOCK_NUMBER = -1L;

    /**
     * The name of the plugin.
     *
     * @return the name of the plugin
     */
    default String name() {
        return getClass().getSimpleName();
    }

    /**
     * Get all configuration data types that should be added to the configuration api. The returned collection must not
     * be null but can be empty set if no configuration is needed for this plugin. This is called before init and all
     * configuration is loaded before init is called.
     *
     * @return list of Java record classes that use the {@link com.swirlds.config.api.ConfigData} and
     * {@link com.swirlds.config.api.ConfigProperty} annotations
     */
    @NonNull
    default List<Class<? extends Record>> configDataTypes() {
        return Collections.emptyList();
    }

    /**
     * Start the plugin. This method is called when the block node is starting up. It provides the block node context to
     * the plugin which can be used to access the different facilities of the block node. Also web routing builder that
     * will be used to create the HTTP/GRPC routing for the block node.
     *
     * @param context        the block node context
     * @param serviceBuilder the service builder that will be used to create the HTTP/GRPC routing for the block node
     */
    default void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {}

    /**
     * Start the plugin. This method is called when the block node is starting up after all initialization is complete.
     * At this point all facilities are available and the plugin can use them. Any background threads should be started
     * here. This method is called after the {@link #init(BlockNodeContext, ServiceBuilder)} method.
     * <p>
     * The default implementation does nothing. This is to be overridden by the plugin if it needs to do
     * anything on start.
     * </p>
     */
    default void start() {}

    /**
     * Stop the plugin. This method is called when the block node is shutting down. This expectation is to do the
     * minimum for a clean shutdown stopping all threads as need.
     * <p>
     * The default implementation does nothing. This is to be overridden by the plugin if it needs to do any cleanup.
     * </p>
     */
    default void stop() {}

    /**
     * The version of the plugin as specified by the classname and the Implementation-Version field in the MANIFEST.MF of the jar
     * containing this class.
     */
    default PluginVersion version() {
        Class<?> clazz = this.getClass();
        return PluginVersion.newBuilder()
                .pluginId(clazz.getName())
                .pluginSoftwareVersion(SemanticVersionUtilities.from(this.getClass()))
                .pluginFeatureNames(new ArrayList<>())
                .build();
    }
}
