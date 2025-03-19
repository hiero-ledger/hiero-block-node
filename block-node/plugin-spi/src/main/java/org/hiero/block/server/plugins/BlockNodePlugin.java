package org.hiero.block.server.plugins;

public interface BlockNodePlugin {
    public void start(BlockNodeContext context);
    public void stop();
}
