// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.config.data;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Max;
import com.swirlds.config.api.validation.annotation.Min;

/**
 * The GrpcConfig class defines the configuration data for the gRPC client.
 *
 * @param serverAddress the address of the gRPC server
 * @param port the port of the gRPC server
 */
@ConfigData("grpc")
public record GrpcConfig(
        @ConfigProperty(defaultValue = "localhost") String serverAddress,
        @ConfigProperty(defaultValue = "8080") @Min(0) @Max(65535) int port) {}
