// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.utils;

import java.util.List;
import org.hiero.block.tools.records.ChainFile;

/**
 * Interface for listing files from cloud storage buckets.
 *
 * <p>Provides abstraction over different cloud storage implementations
 * (Google Cloud Storage with service accounts, S3-compatible with HMAC, etc.)
 */
public interface BucketLister {

    /**
     * Lists all files for a given day.
     *
     * @param blockStartTime the block start time (nanoseconds since epoch) for the day
     * @return list of chain files for the day
     */
    List<ChainFile> listDay(long blockStartTime);

    /**
     * Lists all file names for a given day.
     *
     * @param blockStartTime the block start time (nanoseconds since epoch) for the day
     * @return list of file names for the day
     */
    List<String> listDayFileNames(long blockStartTime);
}
