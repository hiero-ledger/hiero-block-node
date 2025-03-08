// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence;

import static com.hedera.block.server.metrics.BlockNodeMetricTypes.Counter.StreamPersistenceHandlerError;
import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;

import com.hedera.block.common.utils.FileUtilities;
import com.hedera.block.server.ack.AckBlockStatus;
import com.hedera.block.server.ack.AckHandler;
import com.hedera.block.server.block.BlockInfo;
import com.hedera.block.server.events.BlockNodeEventHandler;
import com.hedera.block.server.events.ObjectEvent;
import com.hedera.block.server.exception.BlockStreamProtocolException;
import com.hedera.block.server.mediator.SubscriptionHandler;
import com.hedera.block.server.metrics.MetricsService;
import com.hedera.block.server.notifier.Notifier;
import com.hedera.block.server.persistence.storage.PersistenceStorageConfig;
import com.hedera.block.server.persistence.storage.archive.LocalBlockArchiver;
import com.hedera.block.server.persistence.storage.path.BlockPathResolver;
import com.hedera.block.server.persistence.storage.path.UnverifiedBlockPath;
import com.hedera.block.server.persistence.storage.write.AsyncBlockWriter;
import com.hedera.block.server.persistence.storage.write.AsyncBlockWriterFactory;
import com.hedera.block.server.persistence.storage.write.BlockPersistenceResult;
import com.hedera.block.server.persistence.storage.write.BlockPersistenceResult.BlockPersistenceStatus;
import com.hedera.block.server.service.ServiceStatus;
import com.hedera.block.server.service.WebServerStatus;
import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.TransferQueue;
import java.util.stream.Stream;
import javax.inject.Singleton;

/**
 * Use the StreamPersistenceHandlerImpl to persist live block items passed asynchronously through
 * the LMAX Disruptor
 *
 * <p>This implementation is the primary integration point between the LMAX Disruptor and the file
 * system. The stream persistence handler implements the EventHandler interface so the Disruptor can
 * invoke the onEvent() method when a new SubscribeStreamResponse is available.
 */
@Singleton
public class StreamPersistenceHandlerImpl implements BlockNodeEventHandler<ObjectEvent<List<BlockItemUnparsed>>> {
    private static final System.Logger LOGGER = System.getLogger(StreamPersistenceHandlerImpl.class.getName());
    private final SubscriptionHandler<List<BlockItemUnparsed>> subscriptionHandler;
    private final Notifier notifier;
    private final MetricsService metricsService;
    private final ServiceStatus serviceStatus;
    private final WebServerStatus webServerStatus;
    private final AckHandler ackHandler;
    private final AsyncBlockWriterFactory asyncBlockWriterFactory;
    private final CompletionService<Void> completionService;
    private final LocalBlockArchiver archiver;
    private final BlockPathResolver pathResolver;
    private TransferQueue<BlockItemUnparsed> currentWriterQueue;

    /**
     * Constructor.
     *
     * @param subscriptionHandler valid, non-null instance of {@link SubscriptionHandler}
     * @param notifier valid, non-null instance of {@link Notifier}
     * @param metricsService valid, non-null instance of {@link MetricsService}
     * @param serviceStatus valid, non-null instance of {@link ServiceStatus}
     * @param ackHandler valid, non-null instance of {@link AckHandler}
     * @param asyncBlockWriterFactory valid, non-null instance of {@link AsyncBlockWriterFactory}
     * @param writerExecutor valid, non-null instance of {@link Executor}
     * @param archiver valid, non-null instance of {@link LocalBlockArchiver}
     * @param persistenceStorageConfig valid, non-null instance of {@link PersistenceStorageConfig}
     */
    public StreamPersistenceHandlerImpl(
            @NonNull final SubscriptionHandler<List<BlockItemUnparsed>> subscriptionHandler,
            @NonNull final Notifier notifier,
            @NonNull final MetricsService metricsService,
            @NonNull final ServiceStatus serviceStatus,
            @NonNull final WebServerStatus webServerStatus,
            @NonNull final AckHandler ackHandler,
            @NonNull final AsyncBlockWriterFactory asyncBlockWriterFactory,
            @NonNull final Executor writerExecutor,
            @NonNull final LocalBlockArchiver archiver,
            @NonNull final BlockPathResolver pathResolver,
            @NonNull final PersistenceStorageConfig persistenceStorageConfig)
            throws IOException {
        this.subscriptionHandler = Objects.requireNonNull(subscriptionHandler);
        this.notifier = Objects.requireNonNull(notifier);
        this.metricsService = metricsService;
        this.serviceStatus = Objects.requireNonNull(serviceStatus);
        this.asyncBlockWriterFactory = Objects.requireNonNull(asyncBlockWriterFactory);
        this.archiver = Objects.requireNonNull(archiver);
        this.pathResolver = Objects.requireNonNull(pathResolver);
        this.completionService = new ExecutorCompletionService<>(Objects.requireNonNull(writerExecutor));
        this.webServerStatus = Objects.requireNonNull(webServerStatus);
        // Ensure that the root paths exist
        final Path liveRootPath = Objects.requireNonNull(persistenceStorageConfig.liveRootPath());
        final Path archiveRootPath = Objects.requireNonNull(persistenceStorageConfig.archiveRootPath());
        final Path unverifiedRootPath = Objects.requireNonNull(persistenceStorageConfig.unverifiedRootPath());
        Files.createDirectories(liveRootPath);
        Files.createDirectories(archiveRootPath);
        Files.createDirectories(unverifiedRootPath);

        try (final Stream<Path> blockFilesInUnverified = Files.list(unverifiedRootPath)) {
            // Clean up the unverified directory at startup. Any files under the
            // unverified root at startup are to be considered unreliable
            blockFilesInUnverified.map(Path::toFile).forEach(File::delete);
        }

        // @todo(796) default value for long is a 0, so this means that if no
        //   value is set to the service status for these numbers, the default
        //   in the beginning will be 0, which is a valid number for us. This
        //   maybe makes it erroneous. Maybe we should have a value of -1 as
        //   initial one, so we know that if we get it, nothing has ever set
        //   changed the value yet.

        final Optional<Long> firstAvailableBlockNumberOpt = pathResolver.findFirstAvailableBlockNumber();
        if (firstAvailableBlockNumberOpt.isPresent()) {
            final long firstAvailableBlockNumber = firstAvailableBlockNumberOpt.get();
            serviceStatus.setFirstAvailableBlockNumber(firstAvailableBlockNumber);
        }

        final Optional<Long> latestAvailableBlockNumberOpt = pathResolver.findLatestAvailableBlockNumber();
        if (latestAvailableBlockNumberOpt.isPresent()) {
            final long latestAvailableBlockNumber = latestAvailableBlockNumberOpt.get();
            final BlockInfo latestAckedBlockInfo = new BlockInfo(latestAvailableBlockNumber);
            final AckBlockStatus blockStatus = latestAckedBlockInfo.getBlockStatus();
            blockStatus.setPersisted();
            blockStatus.setVerified();
            blockStatus.markAckSentIfNotAlready();
            serviceStatus.setLatestAckedBlock(latestAckedBlockInfo);
            serviceStatus.setLatestReceivedBlockNumber(latestAvailableBlockNumber);
        }

        // It is indeed a very bad idea to expose `this` to the outside world
        // for an object that has not finished initializing, unfortunately there
        // is no way around this until we have much needed architectural
        // changes. Generally, the whole concept of the ackHandler to be calling
        // back to the persistence handler is a bad idea since it couples the
        // two classes together and makes the ackHandler an overlord.
        // As mentioned, we should get rid of this as soon as possible and
        // be publishing results which would then be picked up instead of having
        // direct method calls. As far as exposing `this`, thankfully the logic
        // that would call `this` from the ackHandler is not executed until the
        // persistence handler is fully initialized, so there is no risk of
        // calling a method on an object that is not fully initialized, BUT we
        // need to take extra care! Essentially, in order to ever call the
        // `moveVerified` method, the ackHandler must be fully initialized, and
        // the block which would be first in line for moving must be both
        // verified and persisted into the unverified directory. Persisting into
        // the unverified directory is done by `this`, so it would not be
        // possible to call `moveVerified` before the persistence handler is
        // fully initialized.
        this.ackHandler = Objects.requireNonNull(ackHandler);
        this.ackHandler.registerPersistence(this);
    }

    public void moveVerified(final long blockNumber) throws IOException {
        final Optional<UnverifiedBlockPath> optUnverified = pathResolver.findUnverifiedBlock(blockNumber);
        if (optUnverified.isPresent()) {
            final UnverifiedBlockPath unverifiedBlockPath = optUnverified.get();
            final Path source = unverifiedBlockPath.dirPath().resolve(unverifiedBlockPath.blockFileName());
            final Path rawPathToLive = pathResolver.resolveLiveRawPathToBlock(unverifiedBlockPath.blockNumber());
            final Path target = FileUtilities.appendExtension(
                    rawPathToLive, unverifiedBlockPath.compressionType().getFileExtension());
            Files.createDirectories(target.getParent());
            Files.move(source, target);
            archiver.notifyBlockPersisted(blockNumber);
        } else {
            throw new FileNotFoundException(
                    "File for Block [%s] not found under unverified root".formatted(blockNumber));
        }
    }

    /**
     * The onEvent method is invoked by the Disruptor when a new SubscribeStreamResponse is
     * available. The method processes the response and persists the block item to the file system.
     *
     * @param event the ObjectEvent containing the SubscribeStreamResponse
     * @param l the sequence number of the event
     * @param b true if the event is the last in the sequence
     */
    @Override
    public void onEvent(final ObjectEvent<List<BlockItemUnparsed>> event, final long l, final boolean b) {
        try {
            if (webServerStatus.isRunning()) {
                final List<BlockItemUnparsed> blockItems = event.get();
                if (blockItems.isEmpty()) {
                    final String message = "BlockItems list is empty.";
                    throw new BlockStreamProtocolException(message);
                }
                handleBlockItems(blockItems);
            } else {
                LOGGER.log(ERROR, "Service is not running. Block items will not be persisted.");
            }
        } catch (final Exception e) {
            LOGGER.log(ERROR, "Failed to persist BlockItems", e);
            teardown();
        }
    }

    @Override
    public void unsubscribe() {
        subscriptionHandler.unsubscribe(this);
    }

    private void handleBlockItems(final List<BlockItemUnparsed> blockItems)
            throws ParseException, BlockStreamProtocolException {
        final BlockItemUnparsed firstItem = blockItems.getFirst();
        if (firstItem.hasBlockHeader()) {
            if (currentWriterQueue != null) {
                // we do not expect to enter here, but if we have, this means that a block header was found
                // before the previous block was completed (no block proof received), the current block is
                // incomplete

                // push the incomplete block to the flag which will signal the async block writer to
                // clean up and return an incomplete block status
                currentWriterQueue.offer(AsyncBlockWriter.INCOMPLETE_BLOCK_FLAG);

                // we need to set the queue to null in case where the first batch does not end with
                // a block proof, we need to keep accepting items in follow-up batches, but not
                // processing them (not pushing them to a queue) until the next block comes along,
                // which will start anew
                currentWriterQueue = null;
            } else {
                final BlockHeader header = BlockHeader.PROTOBUF.parse(firstItem.blockHeader());
                final long blockNumber = header.number();
                if (blockNumber >= 0) {
                    final AsyncBlockWriter writer = asyncBlockWriterFactory.create(blockNumber);
                    currentWriterQueue = writer.getQueue();
                    completionService.submit(writer);
                } else {
                    // we need to notify the ackHandler that the block number is invalid
                    // IMPORTANT: the currentWriterQueue MUST be null after we have
                    // pinged the ack handler with the bad block number status! This must be done
                    // because if the current batch does not end with block proof, we must not
                    // be processing the items (pushing them to a queue) until the next block
                    // comes along, which will start anew. Even if the branching that reaches here
                    // ensures that the queue is null, it is still assigned as an assurance for
                    // future changes that could potentially affect this due to changes in the
                    // branching or other.
                    final BlockPersistenceResult persistenceResult =
                            new BlockPersistenceResult(blockNumber, BlockPersistenceStatus.BAD_BLOCK_NUMBER);
                    LOGGER.log(
                            DEBUG,
                            "Bad Block Number received [%d], publishing Persistence Result: %s"
                                    .formatted(blockNumber, persistenceResult));
                    ackHandler.blockPersisted(persistenceResult);
                    currentWriterQueue = null;
                }
            }
        }
        for (int i = 0; i < blockItems.size() && currentWriterQueue != null; i++) {
            // We need the non-null check because of the bad block number
            // case, we still need to continue processing following block items,
            // but if the first batch with the bad number does not end with a
            // block proof, we need to keep accepting (but not pushing since the
            // queue is null) until we see the proof, and then we can move on to
            // the next block. Also, for the incomplete block flag, we will be
            // setting the queue to null there, for the same reason, in case
            // the first batch does not end with a block proof, to keep accepting
            // items, but not processing them until the next block comes along,
            // which will start anew.
            currentWriterQueue.offer(blockItems.get(i));
        }
        if (blockItems.getLast().hasBlockProof()) {
            currentWriterQueue = null;
        }
        Future<Void> completionResult;
        while ((completionResult = completionService.poll()) != null) {
            handlePersistenceExecution(completionResult);
        }
    }

    private void handlePersistenceExecution(final Future<Void> completionResult) throws BlockStreamProtocolException {
        try {
            if (completionResult.isCancelled()) {
                // @todo(713) submit cancelled to ackHandler when migrated
            } else {
                // we call get here to verify that the task has run to completion
                // we do not expect it to throw an exception, but to publish
                // a meaningful result, if an exception is thrown, it should be
                // either considered a bug or an unhandled exception
                completionResult.get();
            }
        } catch (final ExecutionException e) {
            // we do not expect to enter here, if an exception during execution
            // occurs inside the async block writer, it should publish a sensible
            // result otherwise, it is either a bug or an unhandled case
            throw new BlockStreamProtocolException("Unexpected exception during block persistence.", e);
        } catch (final InterruptedException e) {
            // @todo(713) What would be the proper handling here
            Thread.currentThread().interrupt();
        }
    }

    private void teardown() {
        metricsService.get(StreamPersistenceHandlerError).increment();

        // Trigger the server to stop accepting new requests
        webServerStatus.stopRunning(getClass().getName());

        // Unsubscribe from the mediator to avoid additional onEvent calls.
        unsubscribe();

        // Broadcast the problem to the notifier
        notifier.notifyUnrecoverableError();
    }
}
