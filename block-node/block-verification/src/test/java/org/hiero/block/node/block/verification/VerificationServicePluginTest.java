// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.junit.jupiter.api.Test;

/// Plugin-level integration test for [VerificationServicePlugin].
///
/// All test blocks used here must use the latest supported HAPI version that routes to a real
/// verification session.
class VerificationServicePluginTest
        extends PluginTestBase<VerificationServicePlugin, ExecutorService, ScheduledExecutorService> {
    public VerificationServicePluginTest() {
        super(Executors.newVirtualThreadPerTaskExecutor(), new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        // stub
    }

    @Test
    void test() {
        // stub
    }
}
