// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.downloadlive;

/**
 * Represents the validation status of blockchain records, tracking the running hash chain.
 *
 * <p>This record captures the state of hash-chain validation after processing a block,
 * including:
 * <ul>
 *   <li>The day being processed</li>
 *   <li>The timestamp of the most recent record file</li>
 *   <li>The ending running hash value for that record</li>
 * </ul>
 *
 * <p>The running hash is a cryptographic chain that links each block to the previous one,
 * ensuring blockchain integrity. This status is persisted to disk (typically as JSON) to
 * enable resumption with the correct hash chain state after restarts.
 *
 * <p>The format matches the {@code validateCmdStatus.json} structure used by validation
 * tools, ensuring compatibility across the toolchain.
 *
 * @param dayDate the day key in YYYY-MM-DD format (e.g., "2025-12-01")
 * @param recordFileTime the ISO-8601 timestamp of the record file (e.g., "2025-12-01T00:00:04.319226458Z")
 * @param endRunningHashHex the ending running hash as a hex string after processing this record
 */
public record ValidationStatus(String dayDate, String recordFileTime, String endRunningHashHex) {}
