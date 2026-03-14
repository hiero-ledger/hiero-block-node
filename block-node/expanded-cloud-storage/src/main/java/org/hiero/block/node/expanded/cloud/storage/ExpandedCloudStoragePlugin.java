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
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;
import org.hiero.block.node.spi.blockmessaging.BlockNotificationHandler;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;

/**
 * A block node plugin that uploads each individually-verified block as a compressed
 * {@code .blk.zstd} object directly to any S3-compatible object store (AWS S3, GCS via
 * S3-interop, MinIO, etc.).
 *
 * <p>Unlike the {@code s3-archive} plugin, which batches blocks into large tar files, this
 * plugin uploads one block per object. This makes individual blocks immediately queryable and
 * suits consumers that want block-level granularity in the cloud with minimal latency.
 *
 * <h2>Trigger: {@link VerificationNotification}</h2>
 * The plugin reacts to {@code handleVerification()} rather than {@code handlePersisted()}.
 * This allows cloud upload and local file storage ({@code blocks-file-recent}) to run in
 * parallel — each registered handler gets its own virtual thread. Block bytes are taken
 * directly from {@code notification.block()}, avoiding any dependency on the local historical
 * block provider and eliminating the serialization race that would exist if we waited for
 * {@code PersistedNotification}.
 *
 * <h2>Object key format</h2>
 * <pre>{objectKeyPrefix}/{zeroPaddedBlockNumber}.blk.zstd</pre>
 * where {@code zeroPaddedBlockNumber} is zero-padded to 19 digits (matching
 * {@link Long#MAX_VALUE} digit count) for correct lexicographic ordering.
 * Example: {@code blocks/0000000000001234567.blk.zstd}
 *
 * <h2>Enable / disable</h2>
 * The plugin is disabled when {@code expanded.cloud.storage.endpointUrl} is blank (the
 * default). Set it to a non-empty URL to activate uploads.
 *
 * <h2>hedera-bucky swap path</h2>
 * The S3 client is abstracted behind the {@link S3Client} interface, currently backed by
 * {@link BaseS3ClientAdapter}. When hedera-bucky is available on Maven Central, replace
 * {@link #createS3Client} with a {@code HederaBuckyS3ClientAdapter} — no other changes
 * are required.
 */
public class ExpandedCloudStoragePlugin implements BlockNodePlugin, BlockNotificationHandler {

    private static final String CONTENT_TYPE = "application/octet-stream";

    private final System.Logger LOGGER = System.getLogger(getClass().getName());

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

    /**
     * {@inheritDoc}
     *
     * <p>Reacts to {@link VerificationNotification} to upload the verified block directly to S3
     * in parallel with local file storage. Block bytes are taken from the notification itself
     * ({@code notification.block()}) so there is no dependency on the local historical block
     * provider and no race with {@code blocks-file-recent}.
     */
    @Override
    public void handleVerification(@NonNull final VerificationNotification notification) {
        if (!enabled || s3Client == null) {
            return;
        }
        if (!notification.success()) {
            LOGGER.log(TRACE, "Skipping upload for block {0}: verification did not succeed.", notification.blockNumber());
            return;
        }
        final long blockNumber = notification.blockNumber();
        if (blockNumber < 0) {
            LOGGER.log(TRACE, "Skipping upload: invalid block number {0}.", blockNumber);
            return;
        }
        final BlockUnparsed block = notification.block();
        if (block == null) {
            LOGGER.log(WARNING, "Skipping upload for block {0}: block payload is null.", blockNumber);
            return;
        }
        uploadBlock(blockNumber, block);
    }

    // ---- Private helpers ----------------------------------------------------

    /**
     * Serialises the block to ZSTD-compressed protobuf bytes and uploads to S3.
     *
     * @param blockNumber the block number (used for key generation and logging)
     * @param block       the verified block payload from the notification
     */
    private void uploadBlock(final long blockNumber, @NonNull final BlockUnparsed block) {
        try {
            final byte[] protoBytes = BlockUnparsed.PROTOBUF.toBytes(block).toByteArray();
            final byte[] compressed = CompressionType.ZSTD.compress(protoBytes);
            final String objectKey = buildObjectKey(blockNumber);
            s3Client.uploadFile(
                    objectKey,
                    config.storageClass(),
                    Collections.singletonList(compressed).iterator(),
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
     * Factory method for the production S3 client.
     *
     * @param cfg the plugin configuration
     * @return a new {@link S3Client} backed by {@link BaseS3ClientAdapter}
     * @throws S3ClientException if the client cannot be initialised
     */
    private static S3Client createS3Client(final ExpandedCloudStorageConfig cfg) throws S3ClientException {
        return new BaseS3ClientAdapter(cfg);
    }
}
