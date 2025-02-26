// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.archive;

import com.hedera.block.common.utils.FileUtilities;
import com.hedera.block.common.utils.Preconditions;
import com.hedera.block.server.Constants;
import com.hedera.block.server.persistence.storage.PersistenceStorageConfig;
import com.hedera.block.server.persistence.storage.path.BlockPathResolver;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.System.Logger.Level;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * A {@link java.util.concurrent.Callable} that utilizes the
 * {@link com.hedera.block.server.persistence.storage.PersistenceStorageConfig.StorageType#BLOCK_AS_LOCAL_FILE}
 * persistence type.
 */
public final class LocalGroupZipArchiveTask implements Callable<Long> {
    private static final System.Logger LOGGER = System.getLogger(LocalGroupZipArchiveTask.class.getName());
    private static final String THRESHOLD_PASSED_MESSAGE = "Block Number Threshold for archiving passed [{0}]";
    private static final String ARCHIVE_ROOT_RESOLVED_MESSAGE =
            "Archive Root resolved to [{0}], archiving Block Files under this root";
    private static final String NO_FILES_TO_ARCHIVE_MESSAGE = "No files to archive under [{0}]";
    private static final String TARGET_ZIP_CREATED_MESSAGE = "Target Zip Path [{0}] successfully created";
    private static final String ADD_ENTRY_MESSAGE = "Adding Zip Entry [{0}] to zip file [{1}]";
    private static final String ADD_SUCCESS_MESSAGE = "Zip Entry [{0}] successfully added to zip file [{1}]";
    private static final String ZIP_FILE_SUCCESSFULLY_CREATED_MESSAGE = "Zip File [{0}] successfully created";
    private static final String MUST_CREATE_SYMLINK_MESSAGE =
            "Unable to create hard link [{0} <-> {1}], attempting to create a symbolic link instead";
    private static final String LINK_CREATED_MESSAGE = "Link [{0} <-> {1}] created";
    private static final int BUFFER_SIZE = 32768; // 32K should exactly contain one or two disk blocks in most cases.
    private final BlockPathResolver pathResolver;
    private final long blockNumberThreshold;
    private long blockFilesArchived = 0;

    /**
     * Each local group zip archive task requires a block number threshold to
     * be passed. The block number threshold is the block number of the latest
     * block that has been successfully persisted and verified - 1 group size.
     * It is important to ensure gap of at least 1 group size between the latest
     * live persisted and verified and what would be now archived. The archiving
     * task also requires the archive group size we acquire from the
     * {@link PersistenceStorageConfig} reference. The archive group size is
     * the number of blocks that will be archived in a single zip file. The
     * archive group size is always a power of 10. This must be so in order to
     * ensure proper archiving by determining a safe root to archive everything
     * under it. The trie structure that is utilized by the persistence type
     * used ensures that we can safely archive all blocks under the determined
     * root. The task will utilize the {@link BlockPathResolver} to resolve
     * the root archive based on the passed threshold. The threshold must be
     * at least 10 because 10 is the minimum allowed group size and that would
     * be the least possible threshold passed if the group size were 10. Also,
     * the threshold must be exactly divisible by the group size, otherwise it
     * would indicate a problem during creation of the task (it has been passed
     * an invalid threshold).
     *
     * @param blockNumberThreshold the archiving threshold that was passed
     * @param config valid, non-null {@link PersistenceStorageConfig} instance
     * @param pathResolver valid, non-null {@link BlockPathResolver} instance
     */
    LocalGroupZipArchiveTask(
            final long blockNumberThreshold,
            @NonNull final PersistenceStorageConfig config,
            @NonNull final BlockPathResolver pathResolver) {
        this.pathResolver = Objects.requireNonNull(pathResolver);
        final int archiveGroupSize = config.archiveGroupSize();
        // Valid thresholds are all that are exactly divisible by the group size
        // and are greater than or equal to 10 (minimum allowed group size, also
        // that would be the least possible threshold passed if group size were
        // to be 10).
        Preconditions.requireGreaterOrEqual(
                blockNumberThreshold, 10, "Block Number [%d] is required to be greater or equal than [%d].");
        Preconditions.requireExactlyDivisibleBy(
                blockNumberThreshold,
                archiveGroupSize,
                "Block Number [%d] is required to be exactly divisible by archive group size [%d].");
        this.blockNumberThreshold = blockNumberThreshold;
    }

    /**
     * The archiver will archive blocks to local storage. It is given a passed block number threshold, all blocks
     * below 1 group size lower than the threshold will be archived. Due to the trie structure utilized by the
     * persistence type used, we can be sure that simply by determining the archive root, we can archive all blocks
     * under that root. It is not possible due to the different branching of the trie to archive something that
     * should not be archived. This also means that archives for different thresholds can be safely run in
     * parallel.
     */
    @Override
    public Long call() throws IOException {
        LOGGER.log(Level.DEBUG, THRESHOLD_PASSED_MESSAGE, blockNumberThreshold);
        // Upper bound is always the threshold that was passed -1, the threshold % archive group size (pow 10)
        // must always be 0.
        final long upperBound = blockNumberThreshold - 1;
        // We need to determine the root where the upper bound would reside. All blocks under this root
        // will be the target of our archive. We expect that this root exists because we expect blocks
        // to be actually written there.
        final Path rootToArchive = pathResolver.resolveRawPathToArchiveParentUnderLive(upperBound);
        LOGGER.log(Level.DEBUG, ARCHIVE_ROOT_RESOLVED_MESSAGE, rootToArchive);
        final List<Path> pathsToArchive; // all blocks that should be archived
        try (final Stream<Path> tree = Files.walk(rootToArchive)) {
            pathsToArchive = tree.sorted().toList();
        }
        if (!pathsToArchive.isEmpty()) {
            // First, we create the zip, copying (adding entries) all blocks that
            // should be added to it.
            final Path zipFilePath = archiveInZip(upperBound, pathsToArchive, rootToArchive);
            // Then, if that did not throw an exception, this means that the zip is successful,
            // now we can reroute all subsequent reads to be done under through the zip. To do
            // that, we need to create a link to the zip file we just created so readers can
            // find it again under the live root (the specification is that reading will try to
            // first resolve a block from the live root and if it is not found, it will try to
            // resolve it from the archive, which will be linked at the place where we would
            // usually find the live block, based on archive group size option, so we will find
            // a link to the zip we just created at the appropriate place).
            createLink(rootToArchive, zipFilePath);
            // If the link is created and no exception is thrown, we are sure that blocks are
            // safely archived and are discoverable via the link, now we can safely proceed to
            // delete the live blocks, if something goes wrong, we know the archive is fine, we
            // can rely on it, we will no longer be touching that.
            deleteLive(rootToArchive);
            // If deleting does not throw any exception, we are sure that the blocks are safely
            // archived, are discoverable via the link to the archive and the live blocks are
            // deleted. We can also be sure that no data has been lost.
        } else {
            LOGGER.log(Level.DEBUG, NO_FILES_TO_ARCHIVE_MESSAGE, rootToArchive);
        }
        // If no exception is thrown, then we expect that the archiving process is successful,
        // and we can return the number of blocks that were archived.
        return blockFilesArchived;
    }

    @SuppressWarnings("ForLoopReplaceableByForEach")
    private Path archiveInZip(final long upperBound, final List<Path> pathsToArchive, final Path rootToArchive)
            throws IOException {
        // First, we create the zip
        final Path zipFilePath = pathResolver.resolveRawPathToArchiveParentUnderArchive(upperBound);
        // If the zip file cannot be created or already exists we must throw and stop the archiving
        // existing archives must never be overwritten.
        if (!Files.exists(zipFilePath)) {
            // Inability to create the file must stop the archiving process
            FileUtilities.createFile(zipFilePath);
            LOGGER.log(Level.DEBUG, TARGET_ZIP_CREATED_MESSAGE, zipFilePath);
            // Then, we need to populate the zip with all entries that should be in it, resolved before
            // the invocation of this method.

            // @todo(721) write the zip file initially to a temp file and
            //    move to actual location only after successful creation
            try (final OutputStream fileOut = Files.newOutputStream(zipFilePath);
                    final ZipOutputStream zipOut = new ZipOutputStream(fileOut); ) {
                zipOut.setMethod(ZipOutputStream.STORED);
                zipOut.setLevel(Deflater.NO_COMPRESSION);
                for (int i = 0; i < pathsToArchive.size(); i++) {
                    final Path pathToArchive = pathsToArchive.get(i);
                    final Path relativizedEntryName = rootToArchive.relativize(pathToArchive);
                    writeOneEntry(pathToArchive, relativizedEntryName, zipOut, zipFilePath.toString());
                }
            } catch (final IOException e) {
                // If an exception is thrown here, we need to delete the zip file we just made
                Files.deleteIfExists(zipFilePath);
                // We need to propagate the exception in order to not continue with any further executions of this
                // archiving task!
                throw e;
            }
            // If no exception is thrown, this means that the zip is successfully created.
            LOGGER.log(Level.DEBUG, ZIP_FILE_SUCCESSFULLY_CREATED_MESSAGE, zipFilePath);
            return zipFilePath;
        } else {
            throw new FileAlreadyExistsException(
                    "Zip File [%s] already exists, unable to proceed with archiving!".formatted(zipFilePath));
        }
    }

    private void writeOneEntry(
            final Path pathToArchive,
            final Path relativizedEntryName,
            final ZipOutputStream zipOut,
            final String zipFilePath)
            throws IOException {
        if (Files.isDirectory(pathToArchive)) {
            final String entryName = relativizedEntryName.toString().concat("/");
            LOGGER.log(Level.TRACE, ADD_ENTRY_MESSAGE, entryName, zipFilePath);
            writeSingleDirToZip(zipOut, entryName);
            LOGGER.log(Level.TRACE, ADD_SUCCESS_MESSAGE, entryName, zipFilePath);
        } else {
            final String entryName = relativizedEntryName.toString();
            LOGGER.log(Level.TRACE, ADD_ENTRY_MESSAGE, entryName, zipFilePath);
            writeSingleFileToZip(pathToArchive, zipOut, entryName);
            LOGGER.log(Level.TRACE, ADD_SUCCESS_MESSAGE, entryName, zipFilePath);
        }
    }

    private void writeSingleDirToZip(final ZipOutputStream zipOut, final String entryName) throws IOException {
        final ZipEntry zipEntry = new ZipEntry(entryName);
        zipEntry.setMethod(ZipEntry.STORED);
        zipEntry.setSize(0);
        zipEntry.setCompressedSize(0);
        zipEntry.setCrc(0);
        zipOut.putNextEntry(zipEntry);
        zipOut.closeEntry();
    }

    private void writeSingleFileToZip(final Path pathToArchive, final ZipOutputStream zipOut, final String entryName)
            throws IOException {
        try (final FileChannel channel = FileChannel.open(pathToArchive, StandardOpenOption.READ);
                final InputStream fileIn = Channels.newInputStream(channel);
                final CheckedInputStream checkedIn = new CheckedInputStream(fileIn, new CRC32()); ) {
            // read and count all bytes, let the checksum stream calculate the checksum
            long checksum;
            long fileSize = 0;
            int checkedBytesRead;
            byte[] checkedBlockFileBytes = new byte[BUFFER_SIZE];
            while ((checkedBytesRead = checkedIn.read(checkedBlockFileBytes)) > 0) {
                fileSize += checkedBytesRead;
            }
            checksum = checkedIn.getChecksum().getValue();
            final ZipEntry zipEntry = new ZipEntry(entryName);
            zipEntry.setMethod(ZipEntry.STORED);
            zipEntry.setSize(fileSize);
            zipEntry.setCompressedSize(fileSize);
            zipEntry.setCrc(checksum);
            zipOut.putNextEntry(zipEntry);
            channel.position(0);
            // read and write all bytes to the zip entry
            int bytesRead;
            byte[] blockFileBytes = new byte[BUFFER_SIZE];
            while ((bytesRead = fileIn.read(blockFileBytes)) > 0) {
                zipOut.write(blockFileBytes, 0, bytesRead);
            }
            zipOut.closeEntry();
            blockFilesArchived++;
        }
    }

    private void createLink(final Path rootToArchive, final Path zipFilePath) throws IOException {
        // We need to create a link to the zip file we just created so readers can find it.
        final Path livelink = FileUtilities.appendExtension(rootToArchive, Constants.ZIP_FILE_EXTENSION);
        try {
            Files.createLink(livelink, zipFilePath);
        } catch (final IOException e) {
            LOGGER.log(Level.DEBUG, MUST_CREATE_SYMLINK_MESSAGE, livelink, zipFilePath, e);
            // If we are unable to create a link, we need to attempt to create a symbolic link.
            Files.createSymbolicLink(livelink, zipFilePath);
        }
        LOGGER.log(Level.DEBUG, LINK_CREATED_MESSAGE, livelink, zipFilePath);
        // If no exception is thrown here, this means that the link is made and the blocks
        // could now be discovered via it.
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void deleteLive(final Path rootToArchive) throws IOException {
        // We need to move the live dir that we just archived so readers will no longer be able
        // to find it, hence they will fall back to search for the link we just made as well
        // in the meantime, while readers get data from the link, we can safely delete the
        // live dir.
        final Path movedToDelete = FileUtilities.appendExtension(rootToArchive, "del");
        Files.move(rootToArchive, movedToDelete);
        try {
            // Sleep here would improve the chances of the deletion to succeed.
            Thread.sleep(150);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // After the move is successful, reads will be done through the link.
        // If we have reached here, this means that the zipping is successful,
        // the link for the archive is created successfully, and now it is
        // safe for us to start deleting the blocks in the live root. Even if a
        // delete fails, we know our blocks are safely stored in the archive,
        // so we can be sure no data is lost.
        try (final Stream<Path> pathsToDelete = Files.walk(movedToDelete)) {
            pathsToDelete.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
    }
}
