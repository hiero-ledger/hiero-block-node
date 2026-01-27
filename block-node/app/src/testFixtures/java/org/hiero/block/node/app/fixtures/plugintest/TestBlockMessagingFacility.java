// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.plugintest;

import java.lang.System.Logger.Level;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import org.hiero.block.node.spi.blockmessaging.BackfilledBlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockItemHandler;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.NewestBlockKnownToNetworkNotification;
import org.hiero.block.node.spi.blockmessaging.NoBackPressureBlockItemHandler;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.PublisherStatusUpdateNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;

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
    private final Set<BlockItemHandler> blockItemHandlers =
            new ConcurrentSkipListSet<>(Comparator.comparingInt(Object::hashCode));
    /** set of non-backpressure block item handlers */
    private final Set<BlockItemHandler> nonBackpressureBlockItemHandlers =
            new ConcurrentSkipListSet<>(Comparator.comparingInt(Object::hashCode));
    /** set of block notification handlers */
    private final Set<BlockNotificationHandler> blockNotificationHandlers =
            new ConcurrentSkipListSet<>(Comparator.comparingInt(Object::hashCode));
    /** list of all sent block items */
    private final List<BlockItems> sentBlockBlockItems = new CopyOnWriteArrayList<>();
    /** list of all sent block verification notifications */
    private final List<VerificationNotification> sentVerificationNotifications = new CopyOnWriteArrayList<>();
    /** list of all sent block persistence notifications */
    private final List<PersistedNotification> sentPersistedNotifications = new CopyOnWriteArrayList<>();
    /** List of all sent backfilled block notifications */
    private final List<NewestBlockKnownToNetworkNotification> sentNewestBlockKnownToNetworkNotifications =
            new CopyOnWriteArrayList<>();
    /** List of all sent publisher status update notifications */
    private final List<PublisherStatusUpdateNotification> sentPublisherStatusUpdateNotifications =
            new CopyOnWriteArrayList<>();
    /** Set of handlers for which we must simulate a handler that is behind and producing backpressure. */
    private final Set<BlockItemHandler> handlersWithBackpressure =
            new ConcurrentSkipListSet<>(Comparator.comparingInt(Object::hashCode));

    /**
     * Get all block items sent to the block messaging facility.
     *
     * @return the list of sent block items
     */
    public List<BlockItems> getSentBlockItems() {
        return sentBlockBlockItems;
    }

    /**
     * Get all block verification notifications sent to the block messaging facility.
     *
     * @return the list of sent block verification notifications
     */
    public List<VerificationNotification> getSentVerificationNotifications() {
        return sentVerificationNotifications;
    }

    /**
     * Get all block persistence notifications sent to the block messaging facility.
     *
     * @return the list of sent block persistence notifications
     */
    public List<PersistedNotification> getSentPersistedNotifications() {
        return sentPersistedNotifications;
    }

    /**
     * Get all backfilled block notifications sent to the block messaging facility.
     *
     * @return the list of sent backfilled block notifications
     */
    public List<NewestBlockKnownToNetworkNotification> getSentNewestBlockKnownToNetworkNotifications() {
        return sentNewestBlockKnownToNetworkNotifications;
    }

    /**
     * Get all publisher status update notifications sent to the block messaging facility.
     *
     * @return the list of sent publisher status update notifications
     */
    public List<PublisherStatusUpdateNotification> getSentPublisherStatusUpdateNotifications() {
        return sentPublisherStatusUpdateNotifications;
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

    /**
     * Initiate "slow mode" for a handler.
     * Make the messaging facility act like a particular handler has
     * fallen behind when reading blocks, and either apply backpressure or
     * call the `onTooFarBehind` method for non-backpressure handlers.
     */
    public void setHandlerBehind(final BlockItemHandler handler) {
        if (handler instanceof NoBackPressureBlockItemHandler nonBlockingHandler) {
            unregisterBlockItemHandler(nonBlockingHandler);
            nonBlockingHandler.onTooFarBehindError();
        } else {
            handlersWithBackpressure.add(handler);
        }
    }

    public void clearBackpressure(final BlockItemHandler handler) {
        if (handlersWithBackpressure.contains(handler)) {
            handlersWithBackpressure.remove(handler);
        } else if (handler instanceof NoBackPressureBlockItemHandler nonBlockingHandler) {
            registerNoBackpressureBlockItemHandler(nonBlockingHandler, false, handler.toString());
        }
    }

    // ==== BlockMessagingFacility Methods =============================================================================

    /**
     * {@inheritDoc}
     * <p>
     * This implementation will throw a {@link RejectedExecutionException} if a handler is behind
     * and this causes backpressure.  This is <strong>different from the interface</strong> so that
     * unit tests can detect backpressure without having to handle threading and possible deadlock.
     * <p>
     * Important note:
     * <blockquote>
     * All handlers will receive the published block before the exception is thrown, to avoid leaving handlers
     * in inconsistent states.
     * </blockquote>
     */
    @Override
    public void sendBlockItems(BlockItems blockItems) {
        final int handlerCount = blockItemHandlers.size() + nonBackpressureBlockItemHandlers.size();
        LOGGER.log(
                Level.TRACE,
                "Sending next %d block items for block %d to %d handlers."
                        .formatted(blockItems.blockItems().size(), blockItems.blockNumber(), handlerCount));
        sentBlockBlockItems.add(blockItems);
        boolean handlerHasBackpressure = false;
        for (BlockItemHandler handler : blockItemHandlers) {
            if (handlersWithBackpressure.contains(handler)) {
                handlerHasBackpressure = true;
            }
            LOGGER.log(Level.TRACE, "Calling Handler %s.".formatted(handler));
            handler.handleBlockItemsReceived(blockItems);
        }
        for (BlockItemHandler handler : nonBackpressureBlockItemHandlers) {
            LOGGER.log(Level.TRACE, "Calling Handler %s.".formatted(handler));
            handler.handleBlockItemsReceived(blockItems);
        }
        LOGGER.log(Level.TRACE, "Sent block items:%n%s".formatted(blockItems));
        if (handlerHasBackpressure) {
            throw new RejectedExecutionException();
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
        nonBackpressureBlockItemHandlers.add(handler);
        LOGGER.log(
                Level.DEBUG,
                "Added handler %s as %sCPU intensive.".formatted(handlerName, cpuIntensiveHandler ? "" : "not "));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unregisterBlockItemHandler(BlockItemHandler handler) {
        if (blockItemHandlers.contains(handler)) {
            blockItemHandlers.remove(handler);
        } else if (nonBackpressureBlockItemHandlers.contains(handler)) {
            nonBackpressureBlockItemHandlers.remove(handler);
        } else {
            LOGGER.log(Level.TRACE, "Handler %s unregistered without being registered.".formatted(handler));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sendBlockVerification(VerificationNotification notification) {
        LOGGER.log(Level.TRACE, "Sending block notification " + notification);
        sentVerificationNotifications.add(notification);
        for (BlockNotificationHandler handler : blockNotificationHandlers) {
            handler.handleVerification(notification);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sendBlockPersisted(PersistedNotification notification) {
        LOGGER.log(Level.TRACE, "Sending block notification " + notification);
        sentPersistedNotifications.add(notification);
        for (BlockNotificationHandler handler : blockNotificationHandlers) {
            handler.handlePersisted(notification);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sendBackfilledBlockNotification(BackfilledBlockNotification notification) {
        LOGGER.log(Level.TRACE, "Sending backfilled block notification " + notification);
        for (BlockNotificationHandler handler : blockNotificationHandlers) {
            handler.handleBackfilled(notification);
        }
    }

    @Override
    public void sendNewestBlockKnownToNetwork(NewestBlockKnownToNetworkNotification notification) {
        LOGGER.log(Level.TRACE, "Sending NewestBlockKnownToNetworkNotification block notification " + notification);
        sentNewestBlockKnownToNetworkNotifications.add(notification);
        for (BlockNotificationHandler handler : blockNotificationHandlers) {
            handler.handleNewestBlockKnownToNetwork(notification);
        }
    }

    @Override
    public void sendPublisherStatusUpdate(final PublisherStatusUpdateNotification notification) {
        LOGGER.log(Level.TRACE, "Sending PublisherStatusUpdateNotification block notification " + notification);
        sentPublisherStatusUpdateNotifications.add(notification);
        for (final BlockNotificationHandler handler : blockNotificationHandlers) {
            handler.handlePublisherStatusUpdate(notification);
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
        blockNotificationHandlers.remove(handler);
    }
}
