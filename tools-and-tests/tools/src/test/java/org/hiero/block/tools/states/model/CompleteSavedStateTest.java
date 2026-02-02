// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import org.hiero.block.tools.states.postgres.BinaryObjectCsvRow;
import org.junit.jupiter.api.Test;

/** Tests for {@link CompleteSavedState}. */
class CompleteSavedStateTest {

    // ==================== truncateHex ====================

    @Test
    void truncateHexShortString() {
        assertEquals("abcdef", CompleteSavedState.truncateHex("abcdef"));
    }

    @Test
    void truncateHexExactly32() {
        String hex32 = "a".repeat(32);
        assertEquals(hex32, CompleteSavedState.truncateHex(hex32));
    }

    @Test
    void truncateHexLongString() {
        String hex96 = "a".repeat(96);
        String result = CompleteSavedState.truncateHex(hex96);
        assertEquals(35, result.length()); // 16 + 3 + 16
        assertTrue(result.startsWith("a".repeat(16)));
        assertTrue(result.contains("..."));
        assertTrue(result.endsWith("a".repeat(16)));
    }

    // ==================== verifySignature ====================

    @Test
    void verifySignatureNullPublicKeyReturnsFalse() {
        assertFalse(CompleteSavedState.verifySignature(null, new byte[48], new byte[256]));
    }

    @Test
    void verifySignatureNullHashReturnsFalse() {
        assertFalse(CompleteSavedState.verifySignature(null, null, new byte[256]));
    }

    @Test
    void verifySignatureNullSigReturnsFalse() {
        assertFalse(CompleteSavedState.verifySignature(null, new byte[48], null));
    }

    @Test
    void verifySignatureAllNullReturnsFalse() {
        assertFalse(CompleteSavedState.verifySignature(null, null, null));
    }

    // ==================== printValidationReport (smoke test via loaded state) ====================

    @Test
    void constructorCreatesRecord() {
        SignedState signedState = new SignedState();
        HashMap<String, BinaryObjectCsvRow> map = new HashMap<>();
        CompleteSavedState css = new CompleteSavedState(signedState, map);
        assertEquals(signedState, css.signedState());
        assertEquals(map, css.binaryObjectByHexHashMap());
    }
}
