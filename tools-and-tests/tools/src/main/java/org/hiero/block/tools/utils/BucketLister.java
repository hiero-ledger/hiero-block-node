// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.utils;

import java.util.List;
import org.hiero.block.tools.records.ChainFile;

/**
 * Interface for listing files from cloud storage buckets.
 *
 * <p>Implementations provide listing capabilities for different storage backends
 * (GCS, S3, MinIO, etc.) while maintaining a consistent interface for day-based
 * file listing operations.
 */
public interface BucketLister {

    /**
     * Lists all chain files (record files, signature files, sidecar files) for a given day.
     *
     * @param blockStartTime the start time of the day in nanoseconds since epoch (OA)
     * @return a list of ChainFile objects representing all files for that day
     */
    List<ChainFile> listDay(long blockStartTime);

    /**
     * Lists all file names for a given day.
     *
     * @param blockStartTime the start time of the day in nanoseconds since epoch (OA)
     * @return a list of file paths
     */
    List<String> listDayFileNames(long blockStartTime);
}
