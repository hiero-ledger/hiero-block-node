// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.logging;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Test class for {@link CleanColorfulFormatter}.
 */
class CleanColorfulFormatterTest {

    private CleanColorfulFormatter formatter;

    @BeforeEach
    void setUp() {
        formatter = new CleanColorfulFormatter();
    }

    @Test
    @DisplayName("Test that makeLoggingColorful sets CleanColorfulFormatter on ConsoleHandler")
    void testMakeLoggingColorful() {
        CleanColorfulFormatter.makeLoggingColorful();
        final Logger rootLogger = Logger.getLogger("");
        boolean found = false;
        for (var handler : rootLogger.getHandlers()) {
            if (handler instanceof ConsoleHandler) {
                found = handler.getFormatter() instanceof CleanColorfulFormatter;
                if (found) break;
            }
        }
        assertTrue(found, "CleanColorfulFormatter should be set on the ConsoleHandler");
    }

    @Test
    @DisplayName("Test format method with INFO level log record")
    void testFormat() {
        final LogRecord record = new LogRecord(Level.INFO, "Test message");
        record.setLoggerName("TestLogger");
        record.setSourceClassName("TestSourceClass");
        record.setSourceMethodName("testMethod");
        final String formattedMessage = formatter.format(record);
        assertTrue(formattedMessage.contains("Test message"), "Formatted message should contain the log message");
        assertTrue(formattedMessage.contains("INFO"), "Formatted message should contain the log level");
        assertTrue(
                formattedMessage.contains("TestSourceClass#testMethod"),
                "Formatted message should contain the source class and method");
    }

    @Test
    @DisplayName("Test format method with SEVERE level log record and exception")
    void testFormatWithException() {
        final LogRecord record = new LogRecord(Level.SEVERE, "Test message with exception");
        record.setLoggerName("TestLogger");
        record.setSourceClassName("TestSourceClass");
        record.setSourceMethodName("testMethod");
        record.setThrown(new RuntimeException("Test exception"));
        final String formattedMessage = formatter.format(record);
        assertTrue(
                formattedMessage.contains("Test message with exception"),
                "Formatted message should contain the log message");
        assertTrue(formattedMessage.contains("SEVERE"), "Formatted message should contain the log level");
        assertTrue(
                formattedMessage.contains("TestSourceClass#testMethod"),
                "Formatted message should contain the source class and method");
        assertTrue(
                formattedMessage.contains("Test exception"), "Formatted message should contain the exception message");
    }

    @Test
    @DisplayName("Test format method with all log levels")
    void testAllLogLevels() {
        final Level[] levels = {
            Level.SEVERE, Level.WARNING, Level.INFO, Level.CONFIG, Level.FINE, Level.FINER, Level.FINEST
        };
        for (final Level level : levels) {
            final LogRecord record = new LogRecord(level, "Test message for " + level.getName());
            record.setLoggerName("TestLogger");
            record.setSourceClassName("TestSourceClass");
            record.setSourceMethodName("testMethod");
            final String formattedMessage = formatter.format(record);
            assertTrue(
                    formattedMessage.contains("Test message for " + level.getName()),
                    "Formatted message should contain the log message");
            assertTrue(formattedMessage.contains(level.getName()), "Formatted message should contain the log level");
            assertTrue(
                    formattedMessage.contains("TestSourceClass#testMethod"),
                    "Formatted message should contain the source class and method");
        }
    }

    @Test
    @DisplayName("Test format method with null source class name")
    void testFormatWithNullSourceClassName() {
        final LogRecord record = new LogRecord(Level.INFO, "Test message with null source class name");
        record.setLoggerName("TestLogger");
        record.setSourceClassName(null);
        record.setSourceMethodName("testMethod");
        final String formattedMessage = formatter.format(record);
        assertTrue(
                formattedMessage.contains("Test message with null source class name"),
                "Formatted message should contain the log message");
        assertTrue(formattedMessage.contains("INFO"), "Formatted message should contain the log level");
        assertTrue(formattedMessage.contains("TestLogger"), "Formatted message should contain the logger name");
    }
}
