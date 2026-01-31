// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/** Tests for {@link BlobType}. */
class BlobTypeTest {

    // ==================== typeFromCharCode ====================

    @Test
    void typeFromCharCodeAllValidCodes() {
        assertEquals(BlobType.FILE_DATA, BlobType.typeFromCharCode('f'));
        assertEquals(BlobType.FILE_METADATA, BlobType.typeFromCharCode('k'));
        assertEquals(BlobType.CONTRACT_BYTECODE, BlobType.typeFromCharCode('s'));
        assertEquals(BlobType.CONTRACT_STORAGE, BlobType.typeFromCharCode('d'));
        assertEquals(BlobType.SYSTEM_DELETED_ENTITY_EXPIRY, BlobType.typeFromCharCode('e'));
    }

    @Test
    void typeFromCharCodeInvalidThrows() {
        assertThrows(IllegalArgumentException.class, () -> BlobType.typeFromCharCode('x'));
        assertThrows(IllegalArgumentException.class, () -> BlobType.typeFromCharCode('Z'));
    }
}
