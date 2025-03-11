// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence.storage.path;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import org.hiero.block.server.persistence.storage.PersistenceStorageConfig;
import org.hiero.block.server.service.Constants;

/**
 * A Block path resolver. Used to resolve path to a given Block and all the
 * supporting related operations.
 */
public interface BlockPathResolver {
    /**
     * This method resolves the fs {@link Path} to a Block by a given input
     * number. This method does not guarantee that the returned {@link Path}
     * exists! This method is guaranteed to return a {@code non-null}
     * {@link Path}. The Block File extension
     * {@value Constants#BLOCK_FILE_EXTENSION} is
     * appended to the resolved Block path. No compression extension is appended
     * to the file name. No other file extension is appended to the file name.
     * The provided path is the raw resolved path to the Block inside the live
     * root storage.
     * <br/>
     * <br/>
     * E.G. (illustrative example, actual path may vary):
     * <pre>
     *     If the blockNumber is 10, the resolved path will be:
     *     <b>/path/to/live/block/storage/0.../1/0000000000000000010.blk</b>
     * </pre>
     *
     * @param blockNumber to be resolved the path for
     * @return the resolved path to the given Block by a number
     * @throws IllegalArgumentException if the blockNumber IS NOT a whole number
     */
    @NonNull
    Path resolveLiveRawPathToBlock(final long blockNumber);

    /**
     * This method resolves the fs {@link Path} to a Block by a given input
     * number. This method does not guarantee that the returned {@link Path}
     * exists! This method is guaranteed to return a {@code non-null}
     * {@link Path}. No compression extension is appended
     * to the file name. No other file extension is appended to the file name.
     * The provided path is the raw resolved path to the Block inside the
     * unverified root storage.
     * <br/>
     * <br/>
     * E.G. (illustrative example, actual path may vary):
     * <pre>
     *     If the blockNumber is 10, the resolved path will be:
     *     <b>/path/to/unverified/block/storage/0000000000000000010.blk</b>
     * </pre>
     *
     * @param blockNumber to be resolved the path for
     * @return the resolved path to the given Block by a number
     * @throws IllegalArgumentException if the blockNumber IS NOT a whole number
     */
    @NonNull
    Path resolveLiveRawUnverifiedPathToBlock(final long blockNumber);

    /**
     * This method will resolve the path to a parent directory of a given
     * Block, by Block Number. That parent is the directory under the live root
     * storage, where the Block would reside in for the given
     * {@link PersistenceStorageConfig#archiveGroupSize()}.
     * This means that if our group size is 1000, and the provided block number
     * is let's say 20, then it will be resolved the path to the directory
     * where the 1000s will reside. Another example if we are given the block
     * number 1020, then the resolved path will be the directory where the 2000s
     * reside. This is intended so we know which is the root to be used to
     * archive all blocks under.
     * @param blockNumber to be resolved the path for
     * @return non-null, resolved path for parent of block under live to archive
     */
    @NonNull
    Path resolveRawPathToArchiveParentUnderLive(final long blockNumber);

    /**
     * This method will resolve the path to a parent directory of a given
     * Block, by Block Number. That parent is the directory under the archive
     * storage, where the Block would reside in for the given
     * {@link PersistenceStorageConfig#archiveGroupSize()}.
     * This means that if our group size is 1000, and the provided block number
     * is let's say 20, then it will be resolved the path to the directory
     * where the 1000s will reside. Another example if we are given the block
     * number 1020, then the resolved path will be the directory where the 2000s
     * reside. This is intended so we know which is the root to be used to
     * archive all blocks under. In fact, this is indeed very similar to the
     * {@link #resolveRawPathToArchiveParentUnderLive(long)}, but the difference
     * is that the path is resolved for the archive root and instead of resolving
     * to a directory, we will resolve to a zip file.
     * @param blockNumber to be resolved the path for
     * @return non-null, resolved path for the zip that would contain all
     * blocks archived based on group size
     */
    @NonNull
    Path resolveRawPathToArchiveParentUnderArchive(final long blockNumber);

    /**
     * This method attempts to find a Block by a given number under the
     * persistence storage live root. This method will ONLY check for VERIFIED
     * persisted Blocks. If the Block is found, the method returns a non-empty
     * {@link Optional} of {@link LiveBlockPath}, else an empty {@link Optional}
     * is returned.
     *
     * @param blockNumber to be resolved the path for
     * @return a {@link Optional} of {@link LiveBlockPath} if the block is found
     * @throws IllegalArgumentException if the blockNumber IS NOT a whole number
     */
    @NonNull
    Optional<LiveBlockPath> findLiveBlock(final long blockNumber);

    /**
     * This method attempts to find a Block by a given number under the
     * persistence storage archive. This method will ONLY check for
     * ARCHIVED persisted Blocks. Unverified Blocks cannot be archived, so it is
     * inferred that all Blocks, found under the archive root are verified.
     * If the Block is found, the method returns a non-empty {@link Optional} of
     * {@link ArchiveBlockPath}, else an empty {@link Optional} is returned.
     *
     * @param blockNumber to be resolved the path for
     * @return a {@link Optional} of {@link ArchiveBlockPath} if the block is found
     * @throws IllegalArgumentException if the blockNumber IS NOT a whole number
     */
    @NonNull
    Optional<ArchiveBlockPath> findArchivedBlock(final long blockNumber);

    /**
     * This method attempts to find an UNVERIFIED Block by a given number under
     * the persistence storage unverified root. If the Block is found, the method
     * returns a non-empty {@link Optional} of {@link UnverifiedBlockPath}, else
     * an empty {@link Optional} is returned.
     *
     * @param blockNumber to be resolved the path for
     * @return an {@link Optional} of {@link UnverifiedBlockPath} if the block is found
     * @throws IllegalArgumentException if the blockNumber IS NOT a whole number
     */
    @NonNull
    Optional<UnverifiedBlockPath> findUnverifiedBlock(final long blockNumber);

    /**
     * This method attempts to find a VERIFIED Block by a given number under the
     * persistence storage live root, OR an ARCHIVED Block by that given number.
     * If any of those checks produce a successful find, the method returns
     * {@code true}, else {@code false}.
     *
     * @param blockNumber to be resolved the path for
     * @return a {@link Optional} of {@link LiveBlockPath} if the block is found
     * @throws IllegalArgumentException if the blockNumber IS NOT a whole number
     */
    boolean existsVerifiedBlock(final long blockNumber);

    /**
     * This method attempts to find the first available Block Number. A Block
     * that is considered available is one that is PERSISTED and VERIFIED!
     * If the Block is found, the method returns a non-empty
     * {@link Optional} of {@link Long}, else an empty {@link Optional} is
     * returned.
     *
     * @return a {@link Optional} of {@link Long} with the first available Block
     * Number if it is found, else an empty {@link Optional}
     * @throws IOException if an I/O error occurs
     */
    @NonNull
    Optional<Long> findFirstAvailableBlockNumber() throws IOException;

    /**
     * This method attempts to find the latest available Block Number. A Block
     * that is considered available is one that is PERSISTED and VERIFIED!
     * If the Block is found, the method returns a non-empty
     * {@link Optional} of {@link Long}, else an empty {@link Optional} is
     * returned.
     *
     * @return a {@link Optional} of {@link Long} with the latest available
     * Block Number if it is found, else an empty {@link Optional}
     * @throws IOException if an I/O error occurs
     */
    @NonNull
    Optional<Long> findLatestAvailableBlockNumber() throws IOException;
}
