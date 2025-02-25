// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server;

import static java.util.logging.Level.FINE;
import static java.util.logging.Level.FINER;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class StackTracesTest {

    private java.util.logging.Formatter formatter;
    private Instant fourthOfJuly;
    private String formattedFourthOfJuly;

    @BeforeEach
    public void init() throws IOException {
        // Locate and load our production logging properties
        final String loggingPropertiesPath = Paths.get("")
                .toAbsolutePath()
                .resolve("docker")
                .resolve("logging.properties")
                .toAbsolutePath()
                .toString();
        System.setProperty("java.util.logging.config.file", loggingPropertiesPath);

        // Initialize the JUL logging system
        final java.util.logging.LogManager logManager = java.util.logging.LogManager.getLogManager();
        logManager.reset();
        logManager.readConfiguration();

        // Get the logger and make an info call to reify the logger configuration
        Logger sourceLogger = java.util.logging.Logger.getLogger(getClass().getName());
        sourceLogger.info("Ignore this message. Getting past lazy load only.");

        // Traverse the logger hierarchy to find the root logger
        while (sourceLogger.getParent() != null) {
            sourceLogger = sourceLogger.getParent();
        }

        // Find the console handler
        Handler[] handlers = sourceLogger.getHandlers();
        if (handlers.length == 0) {
            fail("No handlers found");
        }

        // Get the formatter
        java.util.logging.Formatter formatter = null;
        for (Handler handler : handlers) {
            if (handler instanceof java.util.logging.ConsoleHandler) {
                formatter = handlers[0].getFormatter();
            }
        }

        if (formatter == null) {
            fail("No formatter found");
        }

        this.formatter = formatter;

        // Set a fixed point in time
        fourthOfJuly =
                LocalDate.of(2024, Month.JULY, 4).atStartOfDay(ZoneOffset.UTC).toInstant();

        ZonedDateTime zonedDateTime = fourthOfJuly.atZone(ZoneId.systemDefault());

        // Format and print the result
        formattedFourthOfJuly = zonedDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSZ"));
    }

    @ParameterizedTest
    @MethodSource("provideLoggingMessages")
    public void testLoggingMessage(String methodName, String message, String template, Level logLevel) {

        String expected = String.format(
                template, formattedFourthOfJuly, logLevel, getClass().getName(), methodName, message);

        LogRecord record = new LogRecord(logLevel, message);
        record.setInstant(fourthOfJuly);
        record.setSourceMethodName(methodName);
        record.setSourceClassName(getClass().getName());

        assertEquals(expected, formatter.format(record));
    }

    @ParameterizedTest
    @MethodSource("provideLoggingMessagesWithStackTraces")
    public void testLoggingMessageWithException(String methodName, String message, String template, Level logLevel) {

        final IOException testIOException = new IOException(message);
        String expected = String.format(
                template,
                formattedFourthOfJuly,
                logLevel,
                getClass().getName(),
                methodName,
                message,
                buildExpectedStackTrace(testIOException));

        LogRecord record = new LogRecord(logLevel, message);
        record.setInstant(fourthOfJuly);
        record.setSourceMethodName(methodName);
        record.setSourceClassName(getClass().getName());
        record.setThrown(testIOException);

        assertEquals(expected, formatter.format(record));
    }

    private static String buildExpectedStackTrace(IOException ioException) {
        StringBuilder sb = new StringBuilder();
        sb.append(ioException.getClass().getName())
                .append(": ")
                .append(ioException.getMessage())
                .append("\n");

        for (StackTraceElement element : ioException.getStackTrace()) {
            sb.append("\tat ").append(element).append("\n");
        }

        return sb.toString();
    }

    private static Stream<Arguments> provideLoggingMessages() {
        return Stream.of(
                // The templates can have different spacing based on the length of the log level string
                // For example, INFO has 3 spaces after it, while FINE has 4 spaces after it since it must
                // fit in a 7 char width dictated by the logging.properties file format property: %4$-7s

                // INFO
                Arguments.of("testLoggingMessage", "Info test message", "%s %s    [%s %s] %s\n", INFO),
                // DEBUG
                Arguments.of("testLoggingMessage", "Debug test message", "%s %s    [%s %s] %s\n", FINE),
                // TRACE
                Arguments.of("testLoggingMessage", "Trace test message", "%s %s   [%s %s] %s\n", FINER));
    }

    private static Stream<Arguments> provideLoggingMessagesWithStackTraces() {
        return Stream.of(
                // ERROR
                Arguments.of(
                        "testLoggingMessageWithException",
                        "Exception test message",
                        "%s %s  [%s %s] %s\n%s\n",
                        SEVERE));
    }
}
