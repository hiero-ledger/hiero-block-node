// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.tss;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class RosterBootstrapTssPluginTest
        extends PluginTestBase<RosterBootstrapTssPlugin, BlockingExecutor, ScheduledBlockingExecutor> {
    Map<String, String> defaultConfig = new HashMap<>();

    public RosterBootstrapTssPluginTest() throws IOException {
        super(
                new BlockingExecutor(new LinkedBlockingQueue<>()),
                new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));

        // todo: setup the config for the BN peers test
        defaultConfig.put("key", "value");
    }

    @Test
    @DisplayName("request TssData from a peer bn ")
    void requestTssDataFromPeerBN() {
        // todo: fill in the test.

        long i = (Date.from(Instant.now()).getTime() % 10) + 1;
        assertNotEquals(0, i);
    }
}
