// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.verification.session;

import com.hedera.hapi.block.stream.output.BlockHeader;

class BlockVerificationSessionSyncTest extends BlockVerificationSessionBaseTest {

    @Override
    protected BlockVerificationSession createSession(BlockHeader blockHeader) {
        return new BlockVerificationSessionSync(blockHeader, metricsService, signatureVerifier);
    }
}
