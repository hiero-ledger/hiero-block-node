// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.hiero.block.tools.states.postgres.BlobType;
import org.junit.jupiter.api.Test;

/** Tests for {@link StorageKey} and {@link StorageValue}. */
class StorageKeyValueTest {

    // ==================== StorageKey copyFrom/copyTo ====================

    @Test
    void storageKeyCopyFromAndCopyToRoundTrip() throws IOException {
        StorageKey original = new StorageKey("/0/f101");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        original.copyTo(dos);
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        StorageKey read = StorageKey.copyFrom(dis);
        assertEquals(original.path(), read.path());
    }

    // ==================== StorageKey.getId ====================

    @Test
    void storageKeyGetId() {
        assertEquals(101L, new StorageKey("/0/f101").getId());
        assertEquals(12345L, new StorageKey("/0/s12345").getId());
    }

    // ==================== StorageKey.getBlobType ====================

    @Test
    void storageKeyGetBlobTypeAllCodes() {
        assertEquals(BlobType.FILE_DATA, new StorageKey("/0/f101").getBlobType());
        assertEquals(BlobType.FILE_METADATA, new StorageKey("/0/k101").getBlobType());
        assertEquals(BlobType.CONTRACT_BYTECODE, new StorageKey("/0/s101").getBlobType());
        assertEquals(BlobType.CONTRACT_STORAGE, new StorageKey("/0/d101").getBlobType());
        assertEquals(BlobType.SYSTEM_DELETED_ENTITY_EXPIRY, new StorageKey("/0/e101").getBlobType());
    }

    @Test
    void storageKeyGetBlobTypeInvalidPathThrows() {
        assertThrows(IllegalArgumentException.class, () -> new StorageKey("/0/").getBlobType());
    }

    // ==================== StorageValue copyFrom/copyTo ====================

    @Test
    void storageValueWithDataRoundTrip() throws IOException {
        byte[] hashBytes = new byte[48];
        hashBytes[0] = 1;
        BinaryObject binObj = new BinaryObject(new byte[] {10, 20, 30});
        // We need to set the hash via copyFrom, so let's build a full serialized BinaryObject
        // and write a StorageValue with it
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        // Write StorageValue version 2 format
        dos.writeLong(2); // version
        dos.writeLong(15487003); // object ID
        dos.writeBoolean(true); // has data
        // Write BinaryObject
        dos.writeLong(1L); // BinaryObject version
        dos.writeLong(1231553L); // BinaryObject object ID
        dos.write(hashBytes); // 48-byte hash
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        StorageValue read = StorageValue.copyFrom(dis);
        assertNotNull(read.data());
    }

    @Test
    void storageValueWithNullData() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeLong(2); // version
        dos.writeLong(15487003); // object ID
        dos.writeBoolean(false); // no data
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        StorageValue read = StorageValue.copyFrom(dis);
        assertNull(read.data());
    }
}
