// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.downloadlive;

/**
 * Minimal descriptor for a blockchain block used by the live poller.
 *
 * <p>This class captures essential metadata about a block from the mirror node API,
 * including its block number, associated record filename, timestamp, and expected hash.
 * It serves as a lightweight data transfer object between the poller and downloader
 * components.
 *
 * <p>Note: This is a transitional implementation that will be aligned with the official
 * mirror node schema in future updates.
 */
public final class BlockDescriptor {
    private final long blockNumber;
    private final String filename;
    private final String timestampIso;
    private final String expectedHash;

    /**
     * Creates a new block descriptor with the specified metadata.
     *
     * @param blockNumber the sequential block number on the blockchain
     * @param filename the name of the primary record file for this block
     * @param timestampIso the ISO-8601 timestamp when this block was created
     * @param expectedHash the expected block hash for validation purposes
     */
    public BlockDescriptor(long blockNumber, String filename, String timestampIso, String expectedHash) {
        this.blockNumber = blockNumber;
        this.filename = filename;
        this.timestampIso = timestampIso;
        this.expectedHash = expectedHash;
    }

    @Override
    public String toString() {
        return "BlockDescriptor{number=%d, file='%s', ts='%s'}".formatted(blockNumber, filename, timestampIso);
    }

    /**
     * Returns the sequential block number on the blockchain.
     *
     * @return the block number
     */
    public long getBlockNumber() {
        return blockNumber;
    }

    /**
     * Returns the name of the primary record file for this block.
     *
     * @return the record filename
     */
    public String getFilename() {
        return filename;
    }

    /**
     * Returns the ISO-8601 formatted timestamp when this block was created.
     *
     * @return the timestamp in ISO-8601 format (e.g., "2025-12-01T00:00:04.319226458Z")
     */
    public String getTimestampIso() {
        return timestampIso;
    }

    /**
     * Returns the expected block hash for validation purposes.
     *
     * @return the expected hash as a hex string, or null if not available
     */
    public String getExpectedHash() {
        return expectedHash;
    }
}
