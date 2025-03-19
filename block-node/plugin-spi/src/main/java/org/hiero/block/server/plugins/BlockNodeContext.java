package org.hiero.block.server.plugins;

public interface BlockNodeContext {

    BlockNodeConfig getBlockNodeConfig();

    BlockNodeMetrics getBlockNodeMetrics();

    BlockNodeEvents getBlockNodeEvents();

    BlockNodeHealth getBlockNodeHealth();

    MessagingService getMessagingService();
}
