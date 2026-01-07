// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.logging;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

/**
 * Minimal JUL handler to capture log messages during testing.
 */
public class TestLogHandler extends Handler {
    private final List<String> messages = new ArrayList<>();

    @Override
    public void publish(LogRecord record) {
        messages.add(record.getMessage());
    }

    @Override
    public void flush() {
        // no-op
    }

    @Override
    public void close() throws SecurityException {
        // no-op
    }

    public int countContaining(final String substring) {
        return (int) messages.stream()
                .filter(msg -> msg != null && msg.contains(substring))
                .count();
    }
}
