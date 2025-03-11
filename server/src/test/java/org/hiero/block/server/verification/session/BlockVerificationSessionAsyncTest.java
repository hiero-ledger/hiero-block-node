// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.verification.session;

import com.hedera.hapi.block.stream.output.BlockHeader;
import java.util.concurrent.Executors;

class BlockVerificationSessionAsyncTest extends BlockVerificationSessionBaseTest {

    @Override
    protected BlockVerificationSession createSession(BlockHeader blockHeader) {
        return new BlockVerificationSessionAsync(
                blockHeader, metricsService, signatureVerifier, Executors.newSingleThreadExecutor(), 32);
    }
}
