// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.path;

import static java.lang.System.Logger.Level.INFO;

import com.hedera.block.common.utils.FileUtilities;
import com.hedera.block.common.utils.Preconditions;
import com.hedera.block.server.Constants;
import com.hedera.block.server.persistence.storage.PersistenceStorageConfig;
import com.hedera.block.server.persistence.storage.PersistenceStorageConfig.CompressionType;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.System.Logger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * A Block path resolver for block-as-file.
 */
public final class BlockAsLocalFilePathResolver implements BlockPathResolver {
    private static final Logger LOGGER = System.getLogger(BlockAsLocalFilePathResolver.class.getName());
    private static final int MAX_LONG_DIGITS = 19;
    private final Path liveRootPath;
    private final Path archiveRootPath;
    private final Path unverifiedRootPath;
    private final int archiveGroupSize;
    private final int archiveDirDepth;
    private final DecimalFormat longLeadingZeroesFormat;

    /**
     * Constructor.
     *
     * @param config valid, {@code non-null} instance of
     * {@link PersistenceStorageConfig} used for initializing the resolver
     */
    public BlockAsLocalFilePathResolver(@NonNull final PersistenceStorageConfig config) throws IOException {
        this.liveRootPath = Objects.requireNonNull(config.liveRootPath());
        this.archiveRootPath = Objects.requireNonNull(config.archiveRootPath());
        this.unverifiedRootPath = Objects.requireNonNull(config.unverifiedRootPath());
        this.archiveGroupSize = config.archiveGroupSize();
        this.archiveDirDepth = MAX_LONG_DIGITS - (int) Math.log10(this.archiveGroupSize);
        this.longLeadingZeroesFormat = new DecimalFormat("0".repeat(MAX_LONG_DIGITS));
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
        return unverifiedRootPath.resolve(
                longLeadingZeroesFormat.format(blockNumber).concat(Constants.BLOCK_FILE_EXTENSION));
    }

    @NonNull
    @Override
    public Path resolveRawPathToArchiveParentUnderLive(final long blockNumber) {
        return resolveRawArchivingTarget(blockNumber, liveRootPath, "");
    }

    @NonNull
    @Override
    public Path resolveRawPathToArchiveParentUnderArchive(final long blockNumber) {
        return resolveRawArchivingTarget(blockNumber, archiveRootPath, Constants.ZIP_FILE_EXTENSION);
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

    @NonNull
    @Override
    public Optional<UnverifiedBlockPath> findUnverifiedBlock(final long blockNumber) {
        Preconditions.requireWhole(blockNumber);
        final Path unverifiedPathToBlock =
                resolveLiveRawUnverifiedPathToBlock(blockNumber); // here is the raw path, no extension
        Optional<UnverifiedBlockPath> result = Optional.empty();
        final CompressionType[] allCompressionTypes = CompressionType.values();
        for (int i = 0; i < allCompressionTypes.length; i++) {
            final CompressionType localCompressionType = allCompressionTypes[i];
            final Path compressionExtendedBlockPath =
                    FileUtilities.appendExtension(unverifiedPathToBlock, localCompressionType.getFileExtension());
            if (Files.exists(compressionExtendedBlockPath)) {
                final Path dirPath = compressionExtendedBlockPath.getParent();
                final String blockFileName =
                        compressionExtendedBlockPath.getFileName().toString();
                final UnverifiedBlockPath toReturn =
                        new UnverifiedBlockPath(blockNumber, dirPath, blockFileName, localCompressionType);
                result = Optional.of(toReturn);
                break;
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

    private Optional<Path> dfsFindFistLive(final Path root) throws IOException {
        if (Files.isDirectory(root)) {
            try (final Stream<Path> list = Files.list(root)) {
                final Optional<Path> nextPath = list.sorted().findAny();
                if (nextPath.isPresent()) {
                    LOGGER.log(INFO, "traversing first dfs - " + nextPath.get().toString());
                    return dfsFindFistLive(nextPath.get());
                } else {
                    LOGGER.log(INFO, "traversing first dfs - empty");
                    return Optional.empty();
                }
            }
        } else {
            return Optional.of(root);
        }
    }

    private Optional<Path> dfsFindLatestLive(final Path root) throws IOException {
        if (Files.isDirectory(root)) {
            try (final Stream<Path> list = Files.list(root)) {
                final Optional<Path> nextPath =
                        list.sorted(Comparator.reverseOrder()).findAny();
                if (nextPath.isPresent()) {
                    LOGGER.log(INFO, "traversing last dfs - " + nextPath.get().toString());
                    return dfsFindLatestLive(nextPath.get());
                } else {
                    LOGGER.log(INFO, "traversing last dfs - empty");
                    return Optional.empty();
                }
            }
        } else {
            return Optional.of(root);
        }
    }

    @NonNull
    @Override
    public Optional<Long> getFirstAvailableBlockNumber() throws IOException {
        final Optional<Path> blockOpt = dfsFindFistLive(liveRootPath);
//        try (final Stream<Path> tree = Files.walk(liveRootPath)) {
//            blockOpt = tree.sorted()
//                    .filter(f -> {
//                        final String fileName = f.getFileName().toString();
//                        LOGGER.log(INFO, "first available - " + fileName);
//                        if (Files.isRegularFile(f)) {
//                            return true;
//                        }
//                        return false;
//                    })
//                    .findAny();
//        }
        if (blockOpt.isPresent()) {
            final Path pathToBlock = blockOpt.get();
            final String fileName = pathToBlock.getFileName().toString();
            if (fileName.endsWith(Constants.ZIP_FILE_EXTENSION)) {
                try (final ZipFile zipFile = new ZipFile(pathToBlock.toFile())) {
                    return zipFile.stream()
                            .sorted(Comparator.comparing(ZipEntry::getName))
                            .filter(e -> {
                                LOGGER.log(INFO, "first available from zip - " + e.getName());
                                return !e.isDirectory();
                            })
                            .findAny()
                            .map(ze -> {
                                final String entryName = ze.getName();
                                // remove leading dir as part of the zip entry name
                                final String rawEntryName = entryName.substring(entryName.lastIndexOf('/') + 1);
                                // remove extensions
                                final String toParse = rawEntryName.substring(0, rawEntryName.indexOf('.'));
                                return Long.parseLong(toParse);
                            })
                            .orElse(-1L)
                            .describeConstable();
                }
            } else {
                return Optional.of(Long.parseLong(fileName.substring(0, fileName.indexOf('.'))));
            }
        } else {
            return Optional.empty();
        }
    }

    @NonNull
    @Override
    public Optional<Long> getLatestAvailableBlockNumber() throws IOException {
        final Optional<Path> blockOpt = dfsFindLatestLive(liveRootPath);
//        try (final Stream<Path> tree = Files.walk(liveRootPath)) {
//            blockOpt = tree.sorted(Comparator.reverseOrder())
//                    .filter(f -> {
//                        final String fileName = f.getFileName().toString();
//                        LOGGER.log(INFO, "last available - " + fileName);
//                        if (Files.isRegularFile(f)) {
//                            return true;
//                        }
//                        return false;
//                    })
//                    .findAny();
//        }
        if (blockOpt.isPresent()) {
            final Path pathToBlock = blockOpt.get();
            final String fileName = pathToBlock.getFileName().toString();
            if (fileName.endsWith(Constants.ZIP_FILE_EXTENSION)) {
                try (final ZipFile zipFile = new ZipFile(pathToBlock.toFile())) {
                    return zipFile.stream()
                            .sorted(Comparator.comparing(ZipEntry::getName).reversed())
                            .filter(e -> {
                                LOGGER.log(INFO, "last available from zip - " + e.getName());
                                return !e.isDirectory();
                            })
                            .findAny()
                            .map(ze -> {
                                final String entryName = ze.getName();
                                // remove leading dir as part of the zip entry name
                                final String rawEntryName = entryName.substring(entryName.lastIndexOf('/') + 1);
                                // remove extensions
                                final String toParse = rawEntryName.substring(0, rawEntryName.indexOf('.'));
                                return Long.parseLong(toParse);
                            })
                            .orElse(-1L)
                            .describeConstable();
                }
            } else {
                return Optional.of(Long.parseLong(fileName.substring(0, fileName.indexOf('.'))));
            }
        } else {
            return Optional.empty();
        }
    }

    /**
     * This method resolves the path to where an archived block would reside. No
     * compression extension is appended to the file name.
     * @param blockNumber the block number to look for
     * @return an {@link ArchiveBlockPath} containing the raw path resolved
     */
    ArchiveBlockPath resolveRawArchivePath(final long blockNumber) {
        final Path zipRootUnderLiveLocation = resolveRawPathToArchiveParentUnderLive(blockNumber);
        final String zipEntryName = zipRootUnderLiveLocation
                .relativize(resolveLiveRawPathToBlock(blockNumber))
                .toString();
        final Path zipFileSymlink =
                FileUtilities.appendExtension(zipRootUnderLiveLocation, Constants.ZIP_FILE_EXTENSION);
        return new ArchiveBlockPath(
                zipFileSymlink.getParent(),
                zipFileSymlink.getFileName().toString(),
                zipEntryName,
                CompressionType.NONE,
                blockNumber);
    }

    private String[] getRawBlockPath(final long blockNumber) {
        final String rawBlockNumber = longLeadingZeroesFormat.format(blockNumber);
        final String[] split = rawBlockNumber.split("");
        split[split.length - 1] = rawBlockNumber;
        return split;
    }

    private Path resolveRawArchivingTarget(final long blockNumber, final Path basePath, final String extension) {
        final long batchStartNumber = (blockNumber / archiveGroupSize) * archiveGroupSize;
        final String formattedBatchStartNumber = longLeadingZeroesFormat.format(batchStartNumber);
        final String[] blockPath = formattedBatchStartNumber.split("");
        final int targetFilePosition = archiveDirDepth - 1;
        final String targetFileName = blockPath[targetFilePosition] + extension;
        blockPath[targetFilePosition] = targetFileName;
        // Construct the path
        Path result = basePath;
        for (int i = 0; i < targetFilePosition; i++) {
            result = result.resolve(blockPath[i]);
        }
        result = result.resolve(targetFileName);
        return result;
    }
}
