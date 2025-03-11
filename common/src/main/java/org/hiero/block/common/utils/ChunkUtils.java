// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.utils;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Utility class for chunking collections. */
public final class ChunkUtils {
    /**
     * Chunk a collection into a list of lists.
     * The resulting list will have a specified size.
     *
     * @param dataToSplit the collection to chunk, if the collection is empty, an empty list is returned.
     * @param chunkSize the size of each chunk
     * @param <T> the type of the collection
     * @return a list of lists of the specified size
     *  */
    public static <T> List<List<T>> chunkify(@NonNull final Collection<T> dataToSplit, final int chunkSize) {
        Objects.requireNonNull(dataToSplit);
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("Chunk size must be greater than 0");
        }
        if (dataToSplit.isEmpty()) {
            return Collections.emptyList(); // or throw, depends on how we want to handle
        }
        final List<T> localCollection = List.copyOf(dataToSplit);
        final int localCollectionSize = localCollection.size();

        List<List<T>> result = new ArrayList<>();

        for (int i = 0; i < localCollectionSize; i += chunkSize) {
            int end = Math.min(i + chunkSize, localCollectionSize);
            result.add(localCollection.subList(i, end));
        }

        return result;
    }

    private ChunkUtils() {}
}
