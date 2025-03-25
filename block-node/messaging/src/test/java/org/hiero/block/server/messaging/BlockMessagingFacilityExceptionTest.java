// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import org.hiero.block.node.messaging.BlockMessagingFacilityImpl;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockNotification.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@link BlockMessagingFacilityImpl} class.
 */
@SuppressWarnings("BusyWait")
public class BlockMessagingFacilityExceptionTest {

    /** The logger used for logging messages. */
    private java.util.logging.Logger logger;
    /** The custom log handler used to capture log messages. */
    private TestLogHandler logHandler;

    /**
     * Set up the test environment by initializing the logger and adding a custom log handler so that we can capture
     * log messages.
     */
    @BeforeEach
    void setUp() {
        logger = java.util.logging.Logger.getLogger(BlockMessagingFacilityImpl.class.getName());
        System.out.println("logger = " + logger);
        logHandler = new TestLogHandler();
        logger.addHandler(logHandler);
        logger.setLevel(java.util.logging.Level.ALL);
    }

    /**
     * Test to verify that a log message is captured in testing
     */
    @Test
    void testLogMessage() {
        String expectedMessage = "Test log message";
        logger.info(expectedMessage);
        assertTrue(logHandler.getLogMessages().contains(expectedMessage), "Log message should be captured");
        // tests with system logger as well
        final System.Logger systemLogger = System.getLogger(BlockMessagingFacilityImpl.class.getName());
        String expectedMessage2 = "SYSTEM-MESSAGE";
        systemLogger.log(System.Logger.Level.INFO, expectedMessage2);
        assertTrue(logHandler.getLogMessages().contains(expectedMessage2), "Log message should be captured");
    }

    /**
     * Test to verify that an exception is logged when the block item handler throws an exception.
     */
    @Test
    void testBlockItemHandlerException() {
        BlockMessagingFacility service = new BlockMessagingFacilityImpl();
        // register a block item handler that just throws an exception
        service.registerBlockItemHandler(
                (blockItems) -> {
                    // Simulate an exception
                    throw new RuntimeException("Simulated exception");
                },
                false,
                null);
        // start services and send empty block items
        service.start();
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        service.sendBlockItems(new BlockItems(Collections.emptyList(), -1));
        service.sendBlockItems(new BlockItems(Collections.emptyList(), -1));
        service.stop();
        // wait for the log handler to process the log messages
        for (int i = 0; i < 10 && logHandler.getLogMessages().isEmpty(); i++) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        // check exception was logged
        assertTrue(
                logHandler.getLogMessages().contains("Exception"),
                "Exception log message should be captured : \"" + logHandler.getLogMessages() + "\"");
    }

    /**
     * Test to verify that an exception is logged when the block notification handler throws an exception.
     */
    @Test
    void testBlockNotificationHandlerException() {
        BlockMessagingFacility service = new BlockMessagingFacilityImpl();
        // register a block notification handler that just throws an exception
        service.registerBlockNotificationHandler(
                blockNotification -> {
                    // Simulate an exception
                    throw new RuntimeException("Simulated exception");
                },
                false,
                null);
        // start services and send empty block notifications
        service.start();
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        service.sendBlockNotification(new BlockNotification(1, Type.BLOCK_VERIFIED, null));
        service.sendBlockNotification(new BlockNotification(1, Type.BLOCK_VERIFIED, null));
        service.stop();
        // wait for the log handler to process the log messages
        for (int i = 0; i < 10 && logHandler.getLogMessages().isEmpty(); i++) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        // check exception was logged
        assertTrue(
                logHandler.getLogMessages().contains("Exception"),
                "Exception log message should be captured : \"" + logHandler.getLogMessages() + "\"");
    }

    /**
     * Custom log handler to capture log messages for testing.
     */
    private static class TestLogHandler extends Handler {
        private final StringBuilder logMessages = new StringBuilder();

        @Override
        public void publish(LogRecord record) {
            logMessages.append(record.getMessage()).append("\n");
            System.out.println("############ Log message: " + record.getMessage());
        }

        @Override
        public void flush() {
            // No-op
        }

        @Override
        public void close() throws SecurityException {
            // No-op
        }

        public String getLogMessages() {
            return logMessages.toString();
        }
    }
}
