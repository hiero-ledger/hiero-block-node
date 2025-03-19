// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence.storage.read;

import com.hedera.pbj.runtime.ParseException;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.Optional;

/**
 * The BlockReader interface defines the contract for reading a block from storage.
 *
 * @param <T> the type to be returned after reading the block
 */
public interface BlockReader<T> {
    /**
     * Reads the block with the given block number.
     *
     * @param blockNumber the block number of the block to read
     * @return the block with the given block number
     * @throws IOException if an I/O error occurs fetching the block
     * @throws ParseException if the PBJ codec encounters a problem caused by I/O issues, malformed
     *     input data, or any other reason that prevents the parse() method from completing the
     *     operation when fetching the block.
     * @throws IllegalArgumentException if the blockNumber IS NOT a whole number
     */
    @NonNull
    Optional<T> read(final long blockNumber) throws IOException, ParseException;
}
