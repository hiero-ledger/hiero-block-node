// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.startup;

import com.hedera.hapi.block.protoc.PublishStreamResponse;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import org.hiero.block.simulator.config.data.BlockGeneratorConfig;

/**
 * Interface for managing startup data for the simulator.
 * Startup data refers to data that can be used as state to start the simulator
 * in. It is sometimes needed to know (based on previous simulator runs) in what
 * state important for the simulator values were left in. Startup data is
 * initialized only once in the beginning of the application. Getters of the
 * startup data will not change for the duration of the application's life even
 * when startup data is being updated! The startup data will always initialize
 * in a correct state that could be trusted and will always fail if there are
 * any issues during initialization, meaning the simulator will not start in a
 * broken state.
 */
public interface SimulatorStartupData {
    /**
     * This method will update the startup data for the simulator based on a
     * response. At the next startup of the application, values based on the
     * last update will be used for initialization.
     * @param publishStreamResponse The response received from the server
     *
     * @throws IOException if an error occurs while updating the startup data
     */
    void updateLatestAckBlockStartupData(@NonNull final PublishStreamResponse publishStreamResponse) throws IOException;

    /**
     * This method returns the latest acknowledged block number based on startup
     * data. The value this method returns will be initialized only once during
     * application startup and will not change for the duration of the
     * application's life even if updates to this value are made at runtime.
     * This value should generally be used for component initialization purposes
     * and not for runtime decision-making.
     *
     * @return the latest acknowledged block number based on startup data, or
     * the value that would be returned by
     * {@link BlockGeneratorConfig#startBlockNumber()} if no startup data is
     * available (initial startup) or the startup data functionality is disabled
     */
    long getLatestAckBlockNumber();

    /**
     * This method returns the latest acknowledged block hash based on startup
     * data. The value this method returns will be initialized only once during
     * application startup and will not change for the duration of the
     * application's life even if updates to this value are made at runtime.
     * This value should generally be used for component initialization purposes
     * and not for runtime decision-making.
     *
     * @return the latest acknowledged block hash based on startup data, or an
     * empty byte array if no startup data is available (initial startup) or the
     * startup data functionality is disabled
     */
    @NonNull
    byte[] getLatestAckBlockHash();
}
