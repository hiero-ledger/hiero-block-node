// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.repair;

/**
 * Outcome of a single zip-file CEN repair attempt.
 *
 * @param status           whether the file was fully repaired, partially repaired, or unrecoverable
 * @param entriesRecovered number of zip entries successfully copied to the repaired file
 * @param detail           human-readable description of the problem ({@code null} when fully repaired)
 */
public record RepairResult(RepairStatus status, int entriesRecovered, String detail) {

    /** Possible outcomes of a zip repair attempt. */
    public enum RepairStatus {
        /** All entries recovered; the repaired zip has a valid CEN. */
        REPAIRED,
        /** Some entries recovered, but the last entry in the source was truncated. */
        PARTIAL,
        /** No entries could be recovered; the file is unreadable. */
        UNRECOVERABLE
    }

    /** @return a fully-repaired result for the given number of recovered entries. */
    public static RepairResult repaired(final int count) {
        return new RepairResult(RepairStatus.REPAIRED, count, null);
    }

    /**
     * @param count  number of entries recovered before truncation
     * @param detail description of the truncation event
     * @return a partial result; the zip was written but the last block may be missing.
     */
    public static RepairResult partial(final int count, final String detail) {
        return new RepairResult(RepairStatus.PARTIAL, count, detail);
    }

    /**
     * @param detail human-readable reason the file could not be repaired
     * @return an unrecoverable result; the original file is left unchanged.
     */
    public static RepairResult unrecoverable(final String detail) {
        return new RepairResult(RepairStatus.UNRECOVERABLE, 0, detail);
    }
}
