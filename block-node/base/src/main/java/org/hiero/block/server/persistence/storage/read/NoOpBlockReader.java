// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence.storage.read;

import com.hedera.hapi.block.BlockUnparsed;
import com.hedera.pbj.runtime.ParseException;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.Optional;

/**
 * A no-op Block reader.
 */
public final class NoOpBlockReader implements BlockReader<BlockUnparsed> {
    /**
     * Constructor.
     */
    private NoOpBlockReader() {}

    /**
     * This method creates and returns a new instance of
     * {@link NoOpBlockReader}.
     *
     * @return a new, fully initialized instance of
     * {@link NoOpBlockReader}
     */
    public static NoOpBlockReader newInstance() {
        return new NoOpBlockReader();
    }

    @NonNull
    @Override
    public Optional<BlockUnparsed> read(final long blockNumber) throws IOException, ParseException {
        return Optional.empty();
    }
}
