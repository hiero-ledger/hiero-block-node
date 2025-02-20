// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.path;

import com.hedera.block.common.utils.FileUtilities;
import com.hedera.block.common.utils.Preconditions;
import com.hedera.block.server.Constants;
import com.hedera.block.server.persistence.storage.PersistenceStorageConfig;
import com.hedera.block.server.persistence.storage.PersistenceStorageConfig.CompressionType;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.zip.ZipFile;

/**
 * A Block path resolver for block-as-file.
 */
public final class BlockAsLocalFilePathResolver implements BlockPathResolver {
    private static final int MAX_LONG_DIGITS = 19;
    private final Path liveRootPath;
    private final int archiveGroupSize;
    private final int archiveDirDepth;
    private final DecimalFormat longLeadingZeroesFormat;
    private final DecimalFormat blockDirDepthFormat;

    /**
     * Constructor.
     *
     * @param config valid, {@code non-null} instance of
     * {@link PersistenceStorageConfig} used for initializing the resolver
     */
    public BlockAsLocalFilePathResolver(@NonNull final PersistenceStorageConfig config) throws IOException {
        this.liveRootPath = Objects.requireNonNull(config.liveRootPath());
        Files.createDirectories(liveRootPath);
        this.archiveGroupSize = config.archiveGroupSize();
        this.archiveDirDepth = MAX_LONG_DIGITS - (int) Math.log10(this.archiveGroupSize);
        this.longLeadingZeroesFormat = new DecimalFormat("0".repeat(MAX_LONG_DIGITS));
        this.blockDirDepthFormat = new DecimalFormat("0".repeat(archiveDirDepth));
    }

    @NonNull
    @Override
    public Path resolveLiveRawPathToBlock(final long blockNumber) {
        Preconditions.requireWhole(blockNumber);
        final String[] blockPath = getRawBlockPath(blockNumber);
        blockPath[blockPath.length - 1] = blockPath[blockPath.length - 1].concat(Constants.BLOCK_FILE_EXTENSION);
        return Path.of(liveRootPath.toString(), blockPath);
    }

    @NonNull
    @Override
    public Path resolveLiveRawUnverifiedPathToBlock(final long blockNumber) {
        Preconditions.requireWhole(blockNumber);
        final String[] blockPath = getRawBlockPath(blockNumber);
        blockPath[blockPath.length - 1] =
                blockPath[blockPath.length - 1].concat(Constants.UNVERIFIED_BLOCK_FILE_EXTENSION);
        return Path.of(liveRootPath.toString(), blockPath);
    }

    @NonNull
    @Override
    public Optional<LiveBlockPath> findLiveBlock(final long blockNumber) {
        Preconditions.requireWhole(blockNumber);
        final Path rawLiveBlockPath = resolveLiveRawPathToBlock(blockNumber); // here is the raw path, no extension
        Optional<LiveBlockPath> result = Optional.empty();
        final CompressionType[] allCompressionTypes = CompressionType.values();
        for (int i = 0; i < allCompressionTypes.length; i++) {
            final CompressionType localCompressionType = allCompressionTypes[i];
            final Path compressionExtendedBlockPath =
                    FileUtilities.appendExtension(rawLiveBlockPath, localCompressionType.getFileExtension());
            if (Files.exists(compressionExtendedBlockPath)) {
                final Path dirPath = compressionExtendedBlockPath.getParent();
                final String blockFileName =
                        compressionExtendedBlockPath.getFileName().toString();
                final LiveBlockPath toReturn =
                        new LiveBlockPath(blockNumber, dirPath, blockFileName, localCompressionType);
                result = Optional.of(toReturn);
                break;
            }
        }
        return result;
    }

    @NonNull
    @Override
    public Optional<ArchiveBlockPath> findArchivedBlock(final long blockNumber) {
        Preconditions.requireWhole(blockNumber);
        final ArchiveBlockPath rawArchiveBlockPath =
                resolveRawArchivePath(blockNumber); // here is the raw path, no extension
        final Path resolvedZipFilePath = rawArchiveBlockPath.dirPath().resolve(rawArchiveBlockPath.zipFileName());
        Optional<ArchiveBlockPath> result = Optional.empty();
        if (Files.exists(resolvedZipFilePath)) {
            try (final ZipFile zipFile = new ZipFile(resolvedZipFilePath.toFile())) {
                final String rawEntryName = rawArchiveBlockPath.zipEntryName();
                final CompressionType[] allCompressionTypes = CompressionType.values();
                for (int i = 0; i < allCompressionTypes.length; i++) {
                    final CompressionType localCompressionType = allCompressionTypes[i];
                    final String compressionExtendedEntry =
                            rawEntryName.concat(localCompressionType.getFileExtension());
                    if (Objects.nonNull(zipFile.getEntry(compressionExtendedEntry))) {
                        final ArchiveBlockPath toReturn = new ArchiveBlockPath(
                                rawArchiveBlockPath.dirPath(),
                                rawArchiveBlockPath.zipFileName(),
                                compressionExtendedEntry,
                                localCompressionType,
                                rawArchiveBlockPath.blockNumber());
                        result = Optional.of(toReturn);
                        break;
                    }
                }
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return result;
    }

    @Override
    public boolean existsVerifiedBlock(final long blockNumber) {
        Preconditions.requireWhole(blockNumber);
        return findLiveBlock(blockNumber).isPresent()
                || findArchivedBlock(blockNumber).isPresent();
    }

    @Override
    public void markVerified(final long blockNumber) throws IOException {
        Preconditions.requireWhole(blockNumber);
        final Path pathToUnverifiedBlockNoCompressionExtension = resolveLiveRawUnverifiedPathToBlock(blockNumber);
        final CompressionType[] allCompressionTypes = CompressionType.values();
        for (int i = 0; i < allCompressionTypes.length; i++) {
            final CompressionType compressionType = allCompressionTypes[i];
            final Path compressionExtendedUnverifiedPath = FileUtilities.appendExtension(
                    pathToUnverifiedBlockNoCompressionExtension, compressionType.getFileExtension());
            if (Files.exists(compressionExtendedUnverifiedPath)) {
                doMarkUnverified(compressionExtendedUnverifiedPath);
                break;
            }
        }
    }

    /**
     * This method resolves the path to where an archived block would reside. No
     * compression extension is appended to the file name.
     * @param blockNumber the block number to look for
     * @return an {@link ArchiveBlockPath} containing the raw path resolved
     */
    ArchiveBlockPath resolveRawArchivePath(final long blockNumber) {
        final long dividedNumber = Math.floorDiv(blockNumber, archiveGroupSize);
        final String formattedNumber = blockDirDepthFormat.format(dividedNumber);
        final StringBuilder pathBuilder = new StringBuilder(archiveDirDepth * 2);
        final char[] arr = formattedNumber.toCharArray();
        for (int i = 0; i < arr.length; i++) {
            pathBuilder.append(arr[i]).append("/");
        }
        pathBuilder.setCharAt(pathBuilder.length() - 1, '.');
        pathBuilder.append("zip");
        final String dst = pathBuilder.toString();
        // use the symlink from the live root path
        final Path destPath = liveRootPath.resolve(Path.of(dst));
        final String rawBlockFileName =
                longLeadingZeroesFormat.format(blockNumber).concat(Constants.BLOCK_FILE_EXTENSION);
        return new ArchiveBlockPath(
                destPath.getParent(),
                destPath.getFileName().toString(),
                rawBlockFileName,
                CompressionType.NONE,
                blockNumber);
    }

    private String[] getRawBlockPath(final long blockNumber) {
        final String rawBlockNumber = longLeadingZeroesFormat.format(blockNumber);
        final String[] split = rawBlockNumber.split("");
        split[split.length - 1] = rawBlockNumber;
        return split;
    }

    private void doMarkUnverified(final Path targetToMove) throws IOException {
        final String verifiedBlockFileName = targetToMove
                .getFileName()
                .toString()
                .replace(Constants.UNVERIFIED_BLOCK_FILE_EXTENSION, Constants.BLOCK_FILE_EXTENSION);
        Files.move(targetToMove, targetToMove.resolveSibling(verifiedBlockFileName));
    }
}
