// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.messaging.impl;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import org.hiero.block.server.config.logging.Loggable;

/**
 * Configuration for the messaging system.
 *
 * @param queueSize The maximum number of messages that can be queued for processing.
 */
@ConfigData("messaging")
public record MessagingConfig(@Loggable @ConfigProperty(defaultValue = "1024") int queueSize) {}
