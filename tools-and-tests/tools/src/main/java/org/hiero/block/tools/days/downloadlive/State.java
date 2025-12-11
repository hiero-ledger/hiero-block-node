// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.downloadlive;

/**
 * Represents the persisted state of the live download poller.
 *
 * <p>This state is used to track download progress across restarts, enabling the poller
 * to resume from where it left off rather than restarting from block 0. The state captures:
 * <ul>
 *   <li>The current day being processed (in YYYY-MM-DD format)</li>
 *   <li>The last successfully processed block number</li>
 *   <li>The timestamp of the last seen block (epoch milliseconds)</li>
 * </ul>
 *
 * <p>This state is typically persisted to disk as JSON and reloaded when the live
 * downloader restarts.
 */
public class State {
    private final String dayKey;
    private final long lastSeenBlock;
    private final long lastSeenTimestamp;

    /**
     * Creates a new state snapshot with the specified day, block number, and timestamp.
     *
     * @param dayKey the day key in YYYY-MM-DD format (e.g., "2025-12-01")
     * @param lastSeenBlock the last block number that was successfully processed
     * @param lastSeenTimestamp the timestamp of the last seen block in epoch milliseconds
     */
    public State(String dayKey, long lastSeenBlock, long lastSeenTimestamp) {
        this.dayKey = dayKey;
        this.lastSeenBlock = lastSeenBlock;
        this.lastSeenTimestamp = lastSeenTimestamp;
    }

    /**
     * Returns the day key representing the current day being processed.
     *
     * @return the day key in YYYY-MM-DD format (e.g., "2025-12-01")
     */
    public String getDayKey() {
        return dayKey;
    }

    /**
     * Returns the last block number that was successfully processed.
     *
     * <p>On restart, the poller will resume from this block number + 1.
     *
     * @return the last successfully processed block number
     */
    public long getLastSeenBlock() {
        return lastSeenBlock;
    }

    /**
     * Returns the timestamp of the last seen block in epoch milliseconds.
     *
     * @return the timestamp of the last seen block
     */
    public long getLastSeenTimestamp() {
        return lastSeenTimestamp;
    }
}
