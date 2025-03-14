// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.verification;

import static java.lang.System.Logger.Level.ERROR;

import com.hedera.hapi.block.BlockItemUnparsed;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Objects;
import javax.inject.Singleton;
import org.hiero.block.server.events.BlockNodeEventHandler;
import org.hiero.block.server.events.ObjectEvent;
import org.hiero.block.server.mediator.SubscriptionHandler;
import org.hiero.block.server.metrics.MetricsService;
import org.hiero.block.server.notifier.Notifier;
import org.hiero.block.server.service.ServiceStatus;
import org.hiero.block.server.verification.service.BlockVerificationService;

/**
 * Verification Handler, receives the block items from the ring buffer, validates their type and uses the BlockVerificationService to verify the block items.
 */
@Singleton
public class StreamVerificationHandlerImpl implements BlockNodeEventHandler<ObjectEvent<List<BlockItemUnparsed>>> {

    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    private final SubscriptionHandler<List<BlockItemUnparsed>> subscriptionHandler;
    private final Notifier notifier;
    private final MetricsService metricsService;
    private final ServiceStatus serviceStatus;
    private final BlockVerificationService blockVerificationService;

    /**
     * Constructs a new instance of {@link StreamVerificationHandlerImpl}.
     *
     * @param subscriptionHandler     the subscription handler
     * @param notifier                the notifier
     * @param metricsService          the metrics service
     * @param serviceStatus           the service status
     * @param blockVerificationService the block verification service
     */
    public StreamVerificationHandlerImpl(
            @NonNull final SubscriptionHandler<List<BlockItemUnparsed>> subscriptionHandler,
            @NonNull final Notifier notifier,
            @NonNull final MetricsService metricsService,
            @NonNull final ServiceStatus serviceStatus,
            @NonNull final BlockVerificationService blockVerificationService) {
        this.subscriptionHandler = Objects.requireNonNull(subscriptionHandler);
        this.notifier = Objects.requireNonNull(notifier);
        this.metricsService = Objects.requireNonNull(metricsService);
        this.serviceStatus = Objects.requireNonNull(serviceStatus);
        this.blockVerificationService = Objects.requireNonNull(blockVerificationService);
    }

    /**
     * Handles the event from the ring buffer, unpacks it and uses the BlockVerificationService to verify the block items.
     */
    @Override
    public void onEvent(ObjectEvent<List<BlockItemUnparsed>> event, long l, boolean b) {

        try {
            if (!serviceStatus.isRunning()) {
                LOGGER.log(ERROR, "Service is not running. Block item will not be processed further.");
                return;
            }

            final List<BlockItemUnparsed> blockItems = event.get();
            blockVerificationService.onBlockItemsReceived(blockItems);
        } catch (final Exception e) {

            LOGGER.log(ERROR, "Failed to verify BlockItems: ", e);
            // Trigger the server to stop accepting new requests
            serviceStatus.stopRunning(getClass().getName());

            // Unsubscribe from the mediator to avoid additional onEvent calls.
            unsubscribe();

            // @todo(662) We need an error channel to broadcast
            // messages to the consumers and producers
            notifier.notifyUnrecoverableError();
        }
    }

    @Override
    public void unsubscribe() {
        subscriptionHandler.unsubscribe(this);
    }
}
