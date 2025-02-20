// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.archive;

import com.hedera.block.common.utils.FileUtilities;
import com.hedera.block.server.persistence.storage.PersistenceStorageConfig;
import com.hedera.block.server.persistence.storage.path.BlockPathResolver;
import com.hedera.block.server.persistence.storage.path.LiveBlockPath;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.System.Logger.Level;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

// @todo(517) add documentation once we have the final implementation
public final class BlockAsLocalFileArchiver implements LocalBlockArchiver {
    private final BlockArchiverRunnable archiverRunnable;
    private final ExecutorService executor;

    public BlockAsLocalFileArchiver(
            @NonNull final PersistenceStorageConfig config, @NonNull final BlockPathResolver blockPathResolver)
            throws IOException {
        this.archiverRunnable =
                new BlockArchiverRunnable(Objects.requireNonNull(config), Objects.requireNonNull(blockPathResolver));
        this.executor = Executors.newSingleThreadExecutor();
        this.executor.submit(archiverRunnable);
    }

    @Override
    public void signalBlockWritten(final long latestBlockNumber) {
        this.archiverRunnable.signalBlockWritten(latestBlockNumber);
    }

    @Override
    public void stop() throws InterruptedException {
        // @todo(517) this will change a bit once we move to task based solution
        archiverRunnable.stop();
        executor.shutdown();
        final boolean terminatedGracefully = this.executor.awaitTermination(60, TimeUnit.SECONDS);
        if (!terminatedGracefully) {
            // todo log or do something
        }
    }

    // @todo(517) this will become a task based solution, this is a temporary solution to v1 of archiving
    private static final class BlockArchiverRunnable implements Runnable {
        private static final System.Logger LOGGER = System.getLogger(BlockArchiverRunnable.class.getName());
        private final Path archiveRootPath;
        private final int archiveGroupSize;
        private final BlockPathResolver blockPathResolver;
        private volatile ThreadSignalCarrier threadSignalCarrier;
        private volatile boolean running;
        private volatile long lastWrittenBlockNumber = -1;
        private volatile long lastArchivedBlockNumber = -1;
        private int blockWrittenSignalCounter = 0;
        // @todo(517) no state will be needed once we move to task based solution

        private BlockArchiverRunnable(
                @NonNull final PersistenceStorageConfig config, final BlockPathResolver blockPathResolver)
                throws IOException {
            this.archiveRootPath = Objects.requireNonNull(config.archiveRootPath());
            Files.createDirectories(archiveRootPath);
            this.archiveGroupSize = config.archiveGroupSize();
            this.blockPathResolver = Objects.requireNonNull(blockPathResolver);
        }

        private void signalBlockWritten(final long latestBlockNumber) {
            if (running) {
                synchronized (this) {
                    if (running) {
                        // the logic inside this block should be accessed only by a single thread
                        final int currentSignalCount = ++blockWrittenSignalCounter;
                        if (currentSignalCount >= archiveGroupSize) {
                            // if threshold is reached, notify the worker and reset counter
                            blockWrittenSignalCounter = 0;
                            lastWrittenBlockNumber = latestBlockNumber;
                            threadSignalCarrier.doNotify();
                        }
                    }
                }
            }
        }

        @Override
        public void run() { // @todo(517) this will be changed to task based solution
            threadSignalCarrier = new ThreadSignalCarrier();
            running = true;
            while (running) {
                try {
                    if (running) {
                        threadSignalCarrier.doWait();
                    }
                    if (running) {
                        archive();
                    }
                } catch (final Exception e) {
                    LOGGER.log(Level.ERROR, "Archiver failed", e);
                }
            }
        }

        // @todo(517) this is a temporary solution to v1 of archiving, which will change to task based implementation
        // the archiving method will be given a latest block number written and out of that we can easily determine
        // what to archive, since we have the trie structure, anything under the root we will resolved will be archived
        // this also allows us to archive in parallel and not be limited to a single thread
        private void archive() throws IOException {
            final long localLastWrittenBlockNumber = lastWrittenBlockNumber;
            if (localLastWrittenBlockNumber == -1) {
                throw new IllegalStateException("No blocks to archive");
            }

            final long localLastArchivedBlockNumber = lastArchivedBlockNumber;
            final long lastWrittenLastArchivedGap;
            if (localLastArchivedBlockNumber == -1) {
                lastWrittenLastArchivedGap = localLastWrittenBlockNumber;
            } else {
                lastWrittenLastArchivedGap = localLastWrittenBlockNumber - localLastArchivedBlockNumber;
            }

            final boolean shouldArchiveBlocks =
                    lastWrittenLastArchivedGap >= (archiveGroupSize * 2L); // todo is this correct x2 ?

            if (shouldArchiveBlocks) {
                final long amountOfBlocksToWrite = lastWrittenLastArchivedGap - archiveGroupSize;
                final List<Path> pathsToArchiveAscending = new ArrayList<>();
                for (int i = 0; i < amountOfBlocksToWrite; i++) {
                    final Optional<LiveBlockPath> block =
                            blockPathResolver.findLiveBlock(localLastArchivedBlockNumber + 1 + i);
                    if (block.isPresent()) {
                        final LiveBlockPath liveBlockPath = block.get();
                        pathsToArchiveAscending.add(liveBlockPath.dirPath().resolve(liveBlockPath.blockFileName()));
                    }
                }

                // @todo(517) once we migrate to the task based solution all this will not be necessary, only we
                // will need to resolve which dir in the trie we need to archive, then we recursively archive anything
                // under that dir
                TreeMap<Path, List<Path>> pathsToArchive = new TreeMap<>();
                for (final Path path : pathsToArchiveAscending) {
                    final long blockNumber =
                            Long.parseLong(path.getFileName().toString().split("\\.")[0]);
                    final Path zipPath =
                            resolveArchivePathForZipOfBlockNumber(blockNumber, archiveGroupSize, archiveRootPath);
                    if (pathsToArchive.containsKey(zipPath)) {
                        pathsToArchive.get(zipPath).add(path);
                    } else {
                        final List<Path> paths = new ArrayList<>();
                        paths.add(path);
                        pathsToArchive.put(zipPath, paths);
                    }
                }

                pathsToArchive = pathsToArchive.entrySet().stream()
                        .filter(entry -> entry.getValue().size() == archiveGroupSize
                                || entry.getKey()
                                        .toString()
                                        .endsWith("/0".repeat(19 - (int) Math.log10(archiveGroupSize)) + ".zip"))
                        .collect(
                                Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, TreeMap::new));

                for (final Entry<Path, List<Path>> pathListEntry : pathsToArchive.entrySet()) {
                    final Path zipFilePath = pathListEntry.getKey();
                    if (Files.notExists(zipFilePath)) FileUtilities.createFile(zipFilePath);
                    try (final ZipOutputStream zipOutputStream =
                            new ZipOutputStream(Files.newOutputStream(zipFilePath))) {
                        for (final Path path : pathListEntry.getValue()) {
                            final ZipEntry zipEntry =
                                    new ZipEntry(path.getFileName().toString());
                            try {
                                zipOutputStream.putNextEntry(zipEntry);
                                Files.copy(path, zipOutputStream);
                                zipOutputStream.closeEntry();
                            } catch (final IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        }
                    }
                }

                for (final Path folder : pathsToArchive.keySet()) {
                    // @todo(517) for the symlink, the whole live root needs to be replaced with the whole archive root!
                    final Path livePathSymlink = Path.of(folder.toString().replace("archive", "live"));
                    Files.createSymbolicLink(livePathSymlink, folder);
                    final Path toDelete = Path.of(livePathSymlink.toString().replace(".zip", ""));
                    try (Stream<Path> paths = Files.walk(toDelete)) {
                        paths.sorted(Comparator.reverseOrder())
                                .map(Path::toFile)
                                .forEach(File::delete);
                    }
                }

                // @todo(517) will not be needed once we migrate to task based solution, all the state will be
                // removed
                this.lastArchivedBlockNumber = Long.parseLong(pathsToArchive
                        .lastEntry()
                        .getValue()
                        .getLast()
                        .getFileName()
                        .toString()
                        .split("\\.")[0]);
            }
        }

        // @todo(517) this will be improved
        public Path resolveArchivePathForZipOfBlockNumber(long blockNumber, int batchSize, Path archiveRootPath) {
            // Calculate the batch start number
            long batchStartNumber = (blockNumber / batchSize) * batchSize;

            // Format the batch start number to a 19-digit string
            String formattedBatchStartNumber = String.format("%0" + 19 + "d", batchStartNumber);

            // Split the formatted number into an array of characters
            String[] blockPath = formattedBatchStartNumber.split("");

            // Determine the position of the zip file name
            int zipFilePosition = blockPath.length - (int) (Math.log10(batchSize) + 1);

            // Create the zip file name
            String zipFileName = blockPath[zipFilePosition] + ".zip";

            // Replace the appropriate position with the zip file name
            blockPath[zipFilePosition] = zipFileName;

            // Construct the path
            Path result = archiveRootPath;
            for (int i = 0; i < zipFilePosition; i++) {
                result = result.resolve(blockPath[i]);
            }
            result = result.resolve(zipFileName);

            return result;
        }

        // @todo(517) this will be removed, if we are in a task based solution, we will not need this probably
        public void stop() {
            running = false;
            threadSignalCarrier.doNotify();
        }
    }

    // @todo(517) remove this, this is a temporary solution to v1 of archiving, which will change to task based
    // implementation
    static final class ThreadSignalCarrier {
        private final Thread toSignalFor;
        private int signalRaisedCounter = 0;
        private boolean wasSignaled = false;
        private boolean isThreadWaiting = false;

        ThreadSignalCarrier() {
            this.toSignalFor = Thread.currentThread();
        }

        void doNotify() {
            synchronized (this) {
                if (!isThreadWaiting) {
                    signalRaisedCounter++;
                } else {
                    wasSignaled = true;
                }
                // always call notify
                notify();
            }
        }

        void doWait() {
            final Thread callerThread = Thread.currentThread();
            if (!callerThread.equals(toSignalFor)) {
                throw new IllegalStateException(
                        "Thread [%s] is not the target of this signal carrier - only [%s] can wait on this object."
                                .formatted(callerThread.getName(), toSignalFor.getName()));
            } else {
                synchronized (this) {
                    // if a signal has been raised, decrement the counter and continue, no need to wait
                    if (signalRaisedCounter > 0) {
                        signalRaisedCounter--;
                    } else { // else wait until a signal is raised
                        // else wait
                        isThreadWaiting = true;
                        try {
                            // while loop to guard against spurious wake-ups
                            while (!wasSignaled) {
                                wait();
                            }
                            wasSignaled = false;
                        } catch (final InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    // always set the isThreadWaiting to false
                    isThreadWaiting = false;
                }
            }
        }
    }
}
