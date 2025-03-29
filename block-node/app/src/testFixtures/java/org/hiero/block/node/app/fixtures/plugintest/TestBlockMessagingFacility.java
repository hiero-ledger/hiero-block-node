// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.plugintest;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.hiero.block.node.spi.blockmessaging.BlockItemHandler;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.NoBackPressureBlockItemHandler;

/**
 * A test implementation of the {@link BlockMessagingFacility} interface.
 * <p>
 * This class is used to test things that need a super simple block messaging facility with no threading. It allows you
 * to send block items and notifications, and register and unregister handlers. All sent data is recorded in lists and
 * accessible for checking in tests. As well as the number of handlers registered.
 */
@SuppressWarnings("unused")
public class TestBlockMessagingFacility implements BlockMessagingFacility {
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** set of block item handlers */
    private final Set<BlockItemHandler> blockItemHandlers = new HashSet<>();
    /** set of block notification handlers */
    private final Set<BlockNotificationHandler> blockNotificationHandlers = new HashSet<>();
    /** list of all sent block items */
    private final List<BlockItems> sentBlockBlockItems = new ArrayList<>();
    /** list of all sent block notifications */
    private final List<BlockNotification> sentBlockNotifications = new ArrayList<>();

    /**
     * Get all block items sent to the block messaging facility.
     *
     * @return the list of sent block items
     */
    public List<BlockItems> getSentBlockItems() {
        return sentBlockBlockItems;
    }

    /**
     * Get all block notifications sent to the block messaging facility.
     *
     * @return the list of sent block notifications
     */
    public List<BlockNotification> getSentBlockNotifications() {
        return sentBlockNotifications;
    }

    /**
     * Get the number of block item handlers registered.
     *
     * @return the number of block item handlers
     */
    public int getBlockItemHandlerCount() {
        return blockItemHandlers.size();
    }

    /**
     * Get the number of block notification handlers registered.
     *
     * @return the number of block notification handlers
     */
    public int getBlockNotificationHandlerCount() {
        return blockNotificationHandlers.size();
    }

    // ==== BlockMessagingFacility Methods =============================================================================

    /**
     * {@inheritDoc}
     */
    @Override
    public void sendBlockItems(BlockItems blockItems) {
        LOGGER.log(System.Logger.Level.DEBUG, "Sending block items " + blockItems);
        sentBlockBlockItems.add(blockItems);
        for (BlockItemHandler handler : blockItemHandlers) {
            handler.handleBlockItemsReceived(blockItems);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerBlockItemHandler(BlockItemHandler handler, boolean cpuIntensiveHandler, String handlerName) {
        blockItemHandlers.add(handler);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerNoBackpressureBlockItemHandler(
            NoBackPressureBlockItemHandler handler, boolean cpuIntensiveHandler, String handlerName) {
        blockItemHandlers.add(handler);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unregisterBlockItemHandler(BlockItemHandler handler) {
        blockItemHandlers.remove(handler);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sendBlockNotification(BlockNotification notification) {
        LOGGER.log(System.Logger.Level.DEBUG, "Sending block notification " + notification);
        sentBlockNotifications.add(notification);
        for (BlockNotificationHandler handler : blockNotificationHandlers) {
            handler.handleBlockNotification(notification);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerBlockNotificationHandler(
            BlockNotificationHandler handler, boolean cpuIntensiveHandler, String handlerName) {
        blockNotificationHandlers.add(handler);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unregisterBlockNotificationHandler(final BlockNotificationHandler handler) {
        //noinspection SuspiciousMethodCalls
        blockItemHandlers.remove(handler);
    }
}
