// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.tss.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.hiero.block.node.roster.bootstrap.tss.GrpcWebClientTuning;
import org.hiero.block.node.roster.bootstrap.tss.client.BlockNodeClient.IntConfigSpec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class BlockNodeClientTest {

    GrpcWebClientTuning tuning = GrpcWebClientTuning.newBuilder()
            .connectTimeout(1)
            .flowControlTimeout(2)
            .initialWindowSize(5)
            .maxFrameSize(8)
            .pingTimeout(11)
            .readTimeout(0)
            .build();

    @Test
    @DisplayName("Test null GrpcWebClientTuning")
    void nullTest() {
        final IntConfigSpec testConfig = new IntConfigSpec("null test", 7, 5, 10, GrpcWebClientTuning::connectTimeout);

        assertEquals(7, testConfig.getValidOrDefault(null));
    }

    @Test
    @DisplayName("Test connectTimeout")
    void connectTimeoutTest() {
        final IntConfigSpec testConfig =
                new IntConfigSpec("connectTimeout test", 7, 5, 10, GrpcWebClientTuning::connectTimeout);

        assertEquals(7, testConfig.getValidOrDefault(tuning));
    }

    @Test
    @DisplayName("Test flowControlTimeout")
    void flowControlTimeout() {
        final IntConfigSpec testConfig =
                new IntConfigSpec("null test", 7, 5, 10, GrpcWebClientTuning::flowControlTimeout);

        assertEquals(7, testConfig.getValidOrDefault(tuning));
    }

    @Test
    @DisplayName("Test initialWindowSize")
    void initialWindowSize() {
        final IntConfigSpec testConfig =
                new IntConfigSpec("initialWindowSize test", 7, 5, 10, GrpcWebClientTuning::initialWindowSize);

        assertEquals(5, testConfig.getValidOrDefault(tuning));
    }

    @Test
    @DisplayName("Test pingTimeout")
    void pingTimeout() {
        final IntConfigSpec testConfig =
                new IntConfigSpec("pingTimeout test", 7, 5, 10, GrpcWebClientTuning::pingTimeout);

        assertEquals(7, testConfig.getValidOrDefault(tuning));
    }

    @Test
    @DisplayName("Test maxFrameSize")
    void maxFrameSize() {
        final IntConfigSpec testConfig =
                new IntConfigSpec("maxFrameSize test", 7, 5, 10, GrpcWebClientTuning::maxFrameSize);

        assertEquals(8, testConfig.getValidOrDefault(tuning));
    }

    @Test
    @DisplayName("Test readTimeout")
    void readTimeoutTest() {
        final IntConfigSpec testConfig =
                new IntConfigSpec("readTimeout test", 7, 5, 10, GrpcWebClientTuning::readTimeout);

        assertEquals(7, testConfig.getValidOrDefault(tuning));
    }
}
