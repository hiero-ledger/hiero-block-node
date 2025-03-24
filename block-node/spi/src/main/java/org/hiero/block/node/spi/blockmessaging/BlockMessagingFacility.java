// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.blockmessaging;

import java.util.List;
import org.hiero.hapi.block.node.BlockItemUnparsed;

/**
 * Service that handles the messaging between the different components of the server. It is used to send block items
 * and block notifications to the different components. It is not meant to become a general purpose messaging service.
 * Implementations of this service are expected to be thread safe and to handle back pressure.
 */
public interface BlockMessagingFacility {

    /**
     * Use this method to send block items to the service. The service will forward items to all registered block item
     * handlers. If the block item handlers are too slow, it will apply back pressure to this caller, by this call
     * taking a long time to return. This should be called by a single thread and the order of calls is significant and
     * preserved.
     *
     * @param blockItems the block items to send
     */
    void sendBlockItems(BlockItems blockItems);

    /**
     * Use this method to register a block item handler. The handler will be called every time new block items arrive.
     * The calls will be on its own thread, every handler registered has its own thread. It can consume block items at
     * its own pace, if it is too slow then it will apply back pressure to block item producer. It is too slow by taking
     * too long in the handleBlockItemsReceived method. If handleBlockItemsReceived is non-blocking, then it will not
     * apply back pressure.
     *
     * @param handler             the block item handler to register
     * @param cpuIntensiveHandler hint to the service that this handler is CPU intensive vs IO intensive
     * @param handlerName         the name of the handler, used for thread name and logging
     * @throws IllegalStateException if the service is already started
     */
    void registerBlockItemHandler(BlockItemHandler handler, boolean cpuIntensiveHandler, String handlerName);

    /**
     * Use this method to dynamically register a block item handler. The handler will be called every time new block
     * items arrive. It will be called on its own thread, every handler registered has its own thread. It can consume
     * block items at its own pace, if it is too slow then the
     * {@link NoBackPressureBlockItemHandler#onTooFarBehindError} method will be called and the handler will be
     * unregistered.
     *
     * @param handler             the block item handler to unregister
     * @param cpuIntensiveHandler hint to the service that this handler is CPU intensive vs IO intensive
     * @param handlerName         the name of the handler, used for thread name and logging
     */
    void registerNoBackpressureBlockItemHandler(
            NoBackPressureBlockItemHandler handler, boolean cpuIntensiveHandler, String handlerName);

    /**
     * Use this method to unregister any block item handler. The handler will no longer be called when new block
     * items arrive. You only need to unregister handlers if they need to be unregistered before the service is
     * shutdown. Shutting down the service will unregister all handlers.
     *
     * @param handler the block item handler to unregister
     */
    void unregisterBlockItemHandler(BlockItemHandler handler);

    /**
     * Use this method to send block notifications to all registered handlers.
     *
     * @param notification the block notification to send
     */
    void sendBlockNotification(BlockNotification notification);

    /**
     * Use this method to register a block notification handler. The handler will be called every time new block
     * notifications arrive. The calls will be on its own thread, every handler registered has its own thread. It can
     * consume block notifications at its own pace, if it is too slow then it will apply back pressure to block
     * notification producer. It is too slow by taking too long in the handleBlockNotificationReceived method. If
     * handleBlockNotificationReceived is non-blocking, then it will not apply back pressure.
     *
     * @param handler             the block notification handler to register
     * @param cpuIntensiveHandler hint to the service that this handler is CPU intensive vs IO intensive
     * @param handlerName         the name of the handler, used for thread name and logging
     */
    void registerBlockNotificationHandler(
            BlockNotificationHandler handler, boolean cpuIntensiveHandler, String handlerName);

    /**
     * Use this method to dynamically unregister a block notification handler. The handler will no longer be called when
     * new block notifications arrive. You only need to unregister handlers if they need to be unregistered before the
     * service is shutdown. Shutting down the service will unregister all handlers.
     *
     * @param handler the block notification handler to unregister
     */
    void unregisterBlockNotificationHandler(BlockNotificationHandler handler);

    /**
     * Start the messaging service. This will start the internal threads and start processing messages. All non-dynamic
     * handlers must have been registered before calling this.
     */
    void start();

    /**
     * Stop the messaging service. This will stop the internal threads and stop processing messages. All handlers will
     * be unregistered.
     */
    void shutdown();
}
