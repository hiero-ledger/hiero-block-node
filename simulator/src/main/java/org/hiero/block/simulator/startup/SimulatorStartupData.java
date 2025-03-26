// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.startup;

import com.hedera.hapi.block.protoc.PublishStreamResponse;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;

public interface SimulatorStartupData {
    void updateLatestAckBlockStartupData(@NonNull final PublishStreamResponse publishStreamResponse) throws IOException;

    long getLatestAckBlockNumber();

    @NonNull
    byte[] getLatestAckBlockHash();
}
