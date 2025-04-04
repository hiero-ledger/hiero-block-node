// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures;

import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * Generic Test Utilities.
 */
public class TestUtils {
    /**
     * Enable debug logging for the test. This is useful for debugging test failures.
     */
    public static void enableDebugLogging() {
        // enable debug System.logger logging
        Logger rootLogger = LogManager.getLogManager().getLogger("");
        rootLogger.setLevel(Level.ALL);
        for (var handler : rootLogger.getHandlers()) {
            handler.setLevel(Level.ALL);
        }
    }
}
