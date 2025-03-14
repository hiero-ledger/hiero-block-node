// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging;

import com.hedera.hapi.block.BlockItemUnparsed;
import java.util.List;
import org.hiero.block.server.messaging.impl.MessagingServiceImpl;

/**
 * Service that handles the messaging between the different components of the server. It is used to send block items
 * and block notifications to the different components. It is not meant to become a general purpose messaging service.
 */
public interface MessagingService {

    /**
     * Use this method to get the new instance of the messaging service.
     *
     * @return the instance of the messaging service
     */
    static MessagingService createMessagingService() {
        return new MessagingServiceImpl();
    }

    /**
     * Use this method to send block items to the service. The service will forward items to all registered block item
     * handlers. If the block item handlers are too slow, it will apply back pressure to this caller, by this call
     * taking a long time to return. This should be called by a single thread and the order of calls is significant and
     * preserved.
     *
     * @param items the block items to send
     */
    void sendBlockItems(List<BlockItemUnparsed> items);

    /**
     * Use this method to register a block item handler. The handler will be called every time new block items arrive.
     * The calls will be on its own virtual thread, every handler registered has its own virtual thread. It can consume
     * block items at its own pace, if it is too slow then it will apply back pressure to block item producer. It is too
     * slow by taking too long in the handleBlockItemsReceived method. If handleBlockItemsReceived is non-blocking, then
     * it will not apply back pressure.
     * <br>
     * It has to be called before starting the service.
     *
     * @param handler the block item handler to register
     * @throws IllegalStateException if the service is already started
     */
    void registerBlockItemHandler(BlockItemHandler handler) throws IllegalStateException;

    /**
     * Use this method to dynamically register a block item handler. The handler will be called every time new block
     * items arrive. It will be called on its own virtual thread, every handler registered has its own virtual thread.
     * It can consume block items at its own pace, if it is too slow then the
     * {@link NoBackPressureBlockItemHandler#onTooFarBehindError} method will be called and the handler will be
     * unregistered.
     * <br>
     * This method has to be called after the service is started.
     *
     * @param handler the block item handler to unregister
     */
    void registerDynamicNoBackpressureBlockItemHandler(NoBackPressureBlockItemHandler handler);

    /**
     * Use this method to unregister a dynamic block item handler. The handler will no longer be called when new block
     * items arrive.
     *
     * @param handler the block item handler to unregister
     */
    void unregisterDynamicNoBackpressureBlockItemHandler(NoBackPressureBlockItemHandler handler);

    /**
     * Use this method to send block notifications to all registered handlers.
     *
     * @param notification the block notification to send
     */
    void sendBlockNotification(BlockNotification notification);

    /**
     * Use this method to register a block notification handler. The handler will be called every time new block
     * notifications arrive. The calls will be on its own virtual thread, every handler registered has its own virtual
     * thread. It can consume block notifications at its own pace, if it is too slow then it will apply back pressure to
     * block notification producer. It is too slow by taking too long in the handleBlockNotificationReceived method. If
     * handleBlockNotificationReceived is non-blocking, then it will not apply back pressure.
     * <br>
     * It has to be called before starting the service.
     *
     * @param handler the block notification handler to register
     * @throws IllegalStateException if the service is already started
     */
    void registerBlockNotificationHandler(BlockNotificationHandler handler) throws IllegalStateException;

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
