// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.Test;

/** Tests for {@link JExchangeRate}, {@link JExchangeRateSet}, and {@link ExchangeRateSetWrapper}. */
class ExchangeRateTest {

    // ==================== ExchangeRateSetWrapper copyFrom/copyTo ====================

    @Test
    void exchangeRateSetWrapperRoundTrip() throws IOException {
        ExchangeRateSetWrapper original =
                new ExchangeRateSetWrapper(30000, 12, 1_700_000_000L, 31000, 13, 1_700_100_000L);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        original.copyTo(dos);
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        ExchangeRateSetWrapper read = ExchangeRateSetWrapper.copyFrom(dis);
        assertEquals(original, read);
    }

    @Test
    void exchangeRateSetWrapperBadVersionThrows() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeLong(99L); // bad version
        dos.flush();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        org.junit.jupiter.api.Assertions.assertThrows(IOException.class, () -> ExchangeRateSetWrapper.copyFrom(dis));
    }
}
