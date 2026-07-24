// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.config.types;

/** Selects which TSS address-book proof the crafted block signatures carry. */
public enum TssProofType {
    /** Genesis Schnorr-aggregate proof (192-byte); no WRAPS proving artifacts required. */
    SCHNORR,
    /** Settled WRAPS proof (704-byte compressed); uses the pre-computed committed proof. */
    WRAPS
}
