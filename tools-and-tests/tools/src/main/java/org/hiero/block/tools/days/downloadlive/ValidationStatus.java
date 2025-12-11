// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.downloadlive;

/**
 * Represents the validation status of blockchain records, tracking the running hash chain.
 *
 * <p>This class captures the state of hash-chain validation after processing a block,
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
 */
public class ValidationStatus {
    private final String dayDate;
    private final String recordFileTime;
    private final String endRunningHashHex;

    /**
     * Creates a new validation status snapshot.
     *
     * @param dayDate the day key in YYYY-MM-DD format (e.g., "2025-12-01")
     * @param recordFileTime the ISO-8601 timestamp of the record file (e.g., "2025-12-01T00:00:04.319226458Z")
     * @param endRunningHashHex the ending running hash as a hex string after processing this record
     */
    public ValidationStatus(String dayDate, String recordFileTime, String endRunningHashHex) {
        this.dayDate = dayDate;
        this.recordFileTime = recordFileTime;
        this.endRunningHashHex = endRunningHashHex;
    }

    /**
     * Returns the day key representing when this validation occurred.
     *
     * @return the day key in YYYY-MM-DD format (e.g., "2025-12-01")
     */
    public String getDayDate() {
        return dayDate;
    }

    /**
     * Returns the timestamp of the record file that was validated.
     *
     * @return the ISO-8601 formatted timestamp (e.g., "2025-12-01T00:00:04.319226458Z")
     */
    public String getRecordFileTime() {
        return recordFileTime;
    }

    /**
     * Returns the ending running hash after validating this record.
     *
     * <p>This hash will be used as the "previous hash" input when validating
     * the next record in the chain, ensuring cryptographic continuity.
     *
     * @return the running hash as a hexadecimal string
     */
    public String getEndRunningHashHex() {
        return endRunningHashHex;
    }

    @Override
    public String toString() {
        return "ValidationStatus{dayDate='%s', recordFileTime='%s', endRunningHashHex='%s'}"
                .formatted(dayDate, recordFileTime, endRunningHashHex);
    }
}
