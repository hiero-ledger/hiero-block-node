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
public record CompleteSavedState(SignedState signedState, Map<String, BinaryObjectCsvRow> binaryObjectByHexHashMap) {}
