// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.config;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Max;
import com.swirlds.config.api.validation.annotation.Min;
import org.hiero.block.node.base.Loggable;

/**
 * Configuration for protobuf message handling.
 *
 * <p>This is the single, operator-settable source of truth for the maximum protobuf message size
 * the node will parse. It is read once at start-up and pushed into
 * {@code org.hiero.block.node.protobuf.ProtobufHandler}, which every module uses when parsing block
 * data — so raising this value lets a node ingest or serve unusually large blocks (e.g. TSS Wraps
 * transition blocks) without a code change.
 *
 * @param maxMessageSizeBytes the maximum protobuf message size, in bytes, the node will parse
 */
@ConfigData("protobuf")
public record ProtobufConfig(
        @Loggable @ConfigProperty(defaultValue = "131_072_000") @Min(1_048_576) @Max(1_610_612_736)
        int maxMessageSizeBytes) {}
