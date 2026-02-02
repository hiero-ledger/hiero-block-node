// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.Test;

/** Tests for {@link MapValue}. */
class MapValueTest {

    // ==================== copyFrom bad version ====================

    @Test
    void copyFromBadVersionThrows() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeLong(999L); // bad version
        dos.writeLong(15487001L); // correct object ID
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        assertThrows(IOException.class, () -> MapValue.copyFrom(dis));
    }

    // ==================== copyFrom bad object ID ====================

    @Test
    void copyFromBadObjectIdThrows() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeLong(4L); // current version
        dos.writeLong(9999999L); // bad object ID
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        assertThrows(IOException.class, () -> MapValue.copyFrom(dis));
    }

    // ==================== constructor with null proxy ====================

    @Test
    void constructorWithNullProxy() {
        FCLinkedList<JTransactionRecord> records = new FCLinkedList<>();
        MapValue mv = new MapValue(100L, 0L, 0L, false, null, null, 7776000L, false, records, 0L, "test", false);
        assertEquals(100L, mv.balance());
        assertNull(mv.proxyAccount());
        assertEquals("test", mv.memo());
        assertFalse(mv.isSmartContract());
    }

    // ==================== constructor with non-null proxy ====================

    @Test
    void constructorWithProxy() {
        JAccountID proxy = new JAccountID(0, 0, 98);
        MapValue mv = new MapValue(500L, 0L, 0L, true, null, proxy, 100L, true, new FCLinkedList<>(), 0L, "m", true);
        assertNotNull(mv.proxyAccount());
        assertEquals(98L, mv.proxyAccount().accountNum());
        assertEquals(true, mv.receiverSigRequired());
        assertEquals(true, mv.deleted());
        assertEquals(true, mv.isSmartContract());
    }
}
