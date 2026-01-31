// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/** Tests for {@link JObjectType}. */
class JObjectTypeTest {

    // ==================== longValue ====================

    @Test
    void longValueForAllTypes() {
        assertEquals(15503731, JObjectType.JKey.longValue());
        assertEquals(15512048, JObjectType.JKeyList.longValue());
        assertEquals(15520365, JObjectType.JThresholdKey.longValue());
        assertEquals(15528682, JObjectType.JEd25519Key.longValue());
        assertEquals(15536999, JObjectType.JECDSA_384Key.longValue());
        assertEquals(15620169, JObjectType.JRSA_3072Key.longValue());
        assertEquals(15545316, JObjectType.JContractIDKey.longValue());
        assertEquals(15553633, JObjectType.JAccountID.longValue());
        assertEquals(15561950, JObjectType.JTransactionID.longValue());
        assertEquals(15570267, JObjectType.JTransactionReceipt.longValue());
        assertEquals(15578584, JObjectType.JContractFunctionResult.longValue());
        assertEquals(15586901, JObjectType.JTransferList.longValue());
        assertEquals(15595218, JObjectType.JTransactionRecord.longValue());
        assertEquals(15603535, JObjectType.JContractLogInfo.longValue());
        assertEquals(15611852, JObjectType.JTimestamp.longValue());
        assertEquals(15628486, JObjectType.JAccountAmount.longValue());
        assertEquals(15636803, JObjectType.JFileInfo.longValue());
        assertEquals(15645120, JObjectType.JExchangeRate.longValue());
        assertEquals(15653437, JObjectType.JExchangeRateSet.longValue());
        assertEquals(15661754, JObjectType.JMemoAdminKey.longValue());
    }

    // ==================== valueOf ====================

    @Test
    void valueOfReturnsCorrectType() {
        assertEquals(JObjectType.JKey, JObjectType.valueOf(15503731));
        assertEquals(JObjectType.JAccountID, JObjectType.valueOf(15553633));
    }

    @Test
    void valueOfUnknownReturnsNull() {
        assertNull(JObjectType.valueOf(999999));
    }

    @Test
    void longValueAndValueOfRoundTrip() {
        for (JObjectType type : JObjectType.values()) {
            assertEquals(type, JObjectType.valueOf(type.longValue()));
        }
    }
}
