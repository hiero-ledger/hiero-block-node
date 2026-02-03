// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.downloadlive;

/**
 * Minimal descriptor for a blockchain block used by the live poller.
 *
 * <p>This record captures essential metadata about a block from the mirror node API,
 * including its block number, associated record filename, timestamp, and expected hash.
 * It serves as a lightweight data transfer object between the poller and downloader
 * components.
 *
 * <p>Note: This is a transitional implementation that will be aligned with the official
 * mirror node schema in future updates.
 *
 * @param blockNumber the sequential block number on the blockchain
 * @param filename the name of the primary record file for this block
 * @param timestampIso the ISO-8601 timestamp when this block was created
 * @param expectedHash the expected block hash for validation purposes (may be null)
 */
public record BlockDescriptor(long blockNumber, String filename, String timestampIso, String expectedHash) {}
