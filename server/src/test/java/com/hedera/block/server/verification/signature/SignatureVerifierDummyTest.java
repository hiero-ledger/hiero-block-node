// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.verification.signature;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import org.hiero.block.common.hasher.HashingUtilities;
import org.junit.jupiter.api.Test;

class SignatureVerifierDummyTest {

    @Test
    void testVerifySignature_success() {
        SignatureVerifierDummy signatureVerifierDummy = new SignatureVerifierDummy();

        Bytes fakeBlockHash = Bytes.fromHex("1234567890abcdef");
        Bytes expectedSignature = HashingUtilities.noThrowSha384HashOf(fakeBlockHash);
        signatureVerifierDummy.verifySignature(fakeBlockHash, expectedSignature);

        assertEquals(true, signatureVerifierDummy.verifySignature(fakeBlockHash, expectedSignature));
    }

    @Test
    void testVerifySignature_fails() {
        SignatureVerifierDummy signatureVerifierDummy = new SignatureVerifierDummy();

        Bytes fakeBlockHash = Bytes.fromHex("1234567890abcdef");
        Bytes expectedSignature = Bytes.fromHex("cafebabe");
        signatureVerifierDummy.verifySignature(fakeBlockHash, expectedSignature);

        assertEquals(false, signatureVerifierDummy.verifySignature(fakeBlockHash, expectedSignature));
    }
}
