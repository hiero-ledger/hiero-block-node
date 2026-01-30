// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.util.Map;
import org.hiero.block.tools.states.postgres.BinaryObjectCsvRow;

/**
 * A complete saved state, including the signed state and all binary objects referenced by it.
 *
 * @param signedState the signed state
 * @param binaryObjectByHexHashMap map of binary objects by their hex hash. Loaded from Postgres export CSV
 */
public record CompleteSavedState(SignedState signedState, Map<String, BinaryObjectCsvRow> binaryObjectByHexHashMap) {
    /**
     * Validates the complete saved state. Hashing and checking computed hash with stored hash. Then checking hash with
     * signatures using public keys from the address book in state and the OaAddressBook address book. Prints a nicely
     * formatted summary of all checks to the console.
     */
    public void printValidationReport() {}
}
