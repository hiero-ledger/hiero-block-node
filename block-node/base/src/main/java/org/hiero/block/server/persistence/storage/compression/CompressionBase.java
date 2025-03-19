// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence.storage.compression;

import com.github.luben.zstd.ZstdInputStream;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import org.hiero.block.server.persistence.storage.PersistenceStorageConfig.CompressionType;

/**
 * Base for all compression implementations.
 */
public abstract class CompressionBase implements Compression {
    /*
     * Comment: no matter what the compression type configured is, we must
     * always be able to wrap an {@link InputStream} with any supported
     * compression type. Hence, this method should be final and should not be
     * overridden by any implementing class. Also, the switch gives us safety
     * because any changes to the {@link CompressionType} enum will be caught
     * at compile time, and we will have to handle them.
     */
    @NonNull
    @Override
    public final InputStream wrap(
            @NonNull final InputStream streamToWrap, @NonNull final CompressionType compressionType)
            throws IOException {
        return switch (Objects.requireNonNull(compressionType)) {
            case ZSTD -> new ZstdInputStream(Objects.requireNonNull(streamToWrap));
            case NONE -> Objects.requireNonNull(streamToWrap);
        };
    }
}
