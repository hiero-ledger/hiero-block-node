// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.expanded.cloud.storage;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.TRACE;
import static java.lang.System.Logger.Level.WARNING;
import static org.hiero.block.node.base.BlockFile.blockNumberFormated;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.hiero.block.common.utils.StringUtilities;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor.Format;

/**
 * A block node plugin that uploads each individually-verified block as a compressed
 * {@code .blk.zstd} object directly to any S3-compatible object store (AWS S3, GCS via
 * S3-interop, MinIO, etc.).
 *
 * <p>Unlike the {@code s3-archive} plugin, which batches blocks into large tar files, this
 * plugin uploads one block per object. This makes individual blocks immediately queryable and
 * suits consumers that want block-level granularity in the cloud.
 *
 * <h2>Object key format</h2>
 * <pre>{objectKeyPrefix}/{zeroPaddedBlockNumber}.blk.zstd</pre>
 * where {@code zeroPaddedBlockNumber} is zero-padded to 19 digits (matching
 * {@link Long#MAX_VALUE} digit count) for correct lexicographic ordering.
 * Example: {@code blocks/0000000000001234567.blk.zstd}
 *
 * <h2>Enable / disable</h2>
 * The plugin is disabled when {@code expanded.cloud.storage.endpointUrl} is blank (the default).
 * Set it to a non-empty URL to activate uploads.
 *
 * <h2>hedera-bucky swap path</h2>
 * The S3 client used by this plugin is abstracted behind the {@link S3Client} interface.
 * Currently backed by {@link BaseS3ClientAdapter}. When hedera-bucky is available on Maven
 * Central, replace {@link #createS3Client} with a {@code HederaBuckyS3ClientAdapter} — no
 * other changes are required.
 */
public class ExpandedCloudStoragePlugin implements BlockNodePlugin, BlockNotificationHandler {

    private static final String CONTENT_TYPE = "application/octet-stream";

    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    /** Block node context, set during {@link #init}. */
    private BlockNodeContext context;

    /** Plugin configuration, set during {@link #init}. */
    private ExpandedCloudStorageConfig config;

    /**
     * The active S3 client. {@code null} when the plugin is disabled.
     * May be pre-set by the package-private test constructor.
     */
    private S3Client s3Client;

    /** Whether the plugin is enabled (endpoint URL is non-blank). */
    private boolean enabled;

    // ---- Constructors -------------------------------------------------------

    /** No-arg constructor used by the Java {@link java.util.ServiceLoader}. */
    public ExpandedCloudStoragePlugin() {}

    /**
     * Package-private constructor for unit tests. Injects a pre-built {@link S3Client} so tests
     * do not need a real S3 endpoint.
     *
     * @param s3Client the S3 client to use instead of creating one from config
     */
    ExpandedCloudStoragePlugin(@NonNull final S3Client s3Client) {
        this.s3Client = s3Client;
    }

    // ---- BlockNodePlugin ----------------------------------------------------

    /** {@inheritDoc} */
    @NonNull
    @Override
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(ExpandedCloudStorageConfig.class);
    }

    /** {@inheritDoc} */
    @Override
    public void init(@NonNull final BlockNodeContext context, @NonNull final ServiceBuilder serviceBuilder) {
        this.context = context;
        this.config = context.configuration().getConfigData(ExpandedCloudStorageConfig.class);

        if (StringUtilities.isBlank(config.endpointUrl())) {
            LOGGER.log(INFO, "Expanded Cloud Storage plugin is disabled. No endpoint URL configured.");
            return;
        }

        enabled = true;
        context.blockMessaging().registerBlockNotificationHandler(this, false, name());
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        if (!enabled) {
            return;
        }
        // Create the S3 client if not pre-injected (e.g. in tests).
        if (s3Client == null) {
            try {
                s3Client = createS3Client(config);
            } catch (final S3ClientException e) {
                LOGGER.log(WARNING, "Failed to create S3 client; plugin will be disabled.", e);
                enabled = false;
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void stop() {
        BlockNodePlugin.super.stop();
        if (s3Client != null) {
            s3Client.close();
            s3Client = null;
        }
    }

    // ---- BlockNotificationHandler -------------------------------------------

    /** {@inheritDoc} */
    @Override
    public void handlePersisted(@NonNull final PersistedNotification notification) {
        if (!enabled || s3Client == null) {
            return;
        }
        if (!notification.succeeded()) {
            LOGGER.log(TRACE, "Skipping upload for block {0}: persistence did not succeed.", notification.blockNumber());
            return;
        }
        final long blockNumber = notification.blockNumber();
        if (blockNumber < 0) {
            LOGGER.log(TRACE, "Skipping upload: invalid block number {0}.", blockNumber);
            return;
        }
        uploadBlock(blockNumber);
    }

    // ---- Private helpers ----------------------------------------------------

    /**
     * Fetches the block bytes and uploads to S3.
     *
     * @param blockNumber the block to upload
     */
    private void uploadBlock(final long blockNumber) {
        final BlockAccessor accessor = context.historicalBlockProvider().block(blockNumber);
        if (accessor == null) {
            LOGGER.log(WARNING, "Block {0} not found in historical provider; skipping upload.", blockNumber);
            return;
        }
        try {
            final byte[] bytes = accessor.blockBytes(Format.ZSTD_PROTOBUF).toByteArray();
            final String objectKey = buildObjectKey(blockNumber);
            s3Client.uploadFile(
                    objectKey,
                    config.storageClass(),
                    Collections.singletonList(bytes).iterator(),
                    CONTENT_TYPE);
            LOGGER.log(TRACE, "Uploaded block {0} to S3 object key: {1}", blockNumber, objectKey);
        } catch (final S3ClientException | IOException e) {
            LOGGER.log(WARNING, "Failed to upload block {0} to S3: {1}", blockNumber, e.getMessage());
        }
    }

    /**
     * Builds the S3 object key for the given block number.
     *
     * <p>Format: {@code {prefix}/{zeroPaddedBlockNumber}.blk.zstd}
     *
     * @param blockNumber the block number
     * @return the S3 object key
     */
    String buildObjectKey(final long blockNumber) {
        return config.objectKeyPrefix() + "/" + blockNumberFormated(blockNumber) + ".blk.zstd";
    }

    /**
     * Factory method for the production S3 client. Overriding this in tests is not necessary
     * because tests use the package-private constructor to inject a {@link NoOpS3Client} or
     * a real {@link BaseS3ClientAdapter} pointing at MinIO.
     *
     * @param cfg the plugin configuration
     * @return a new {@link S3Client} backed by {@link BaseS3ClientAdapter}
     * @throws S3ClientException if the client cannot be initialised
     */
    private static S3Client createS3Client(final ExpandedCloudStorageConfig cfg) throws S3ClientException {
        return new BaseS3ClientAdapter(cfg);
    }
}
