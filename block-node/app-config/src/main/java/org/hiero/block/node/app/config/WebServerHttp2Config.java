// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.config;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import org.hiero.block.node.base.Loggable;

/**
 * Use this configuration across the server features
 *
 * <p>ServerConfig will have settings for the server.
 *
 * @param flowControlTimeout Outbound flow control blocking timeout configured as Duration or text in ISO-8601 format. Blocking timeout defines an interval to wait for the outbound window size changes(incoming window updates). Default value is PT15S.
 *  ISO_8601 format examples:
 *  PT0.1S	100 milliseconds
 *  PT0.5S	500 milliseconds
 *  PT2S	2 seconds
 * @param initialWindowSize This setting indicates the sender's maximum window size in bytes for stream-level flow control. Default and maximum value is 231-1 = 2147483647 bytes. This setting affects the window size of HTTP/2 connection. Any value greater than 2147483647 causes an error. Any value smaller than initial window size causes an error. See RFC 9113 section 6.9.1 for details.
 * @param maxConcurrentStreams Maximum number of concurrent streams that the server will allow. Defaults to 8192. This limit is directional: it applies to the number of streams that the sender permits the receiver to create. It is recommended that this value be no smaller than 100 to not unnecessarily limit parallelism See RFC 9113 section 6.5.2 for details.
 * @param maxEmptyFrames Maximum number of consecutive empty frames allowed on connection.
 * @param maxFrameSize The size of the largest frame payload that the sender is willing to receive in bytes. Default value is 16384 and maximum value is 224-1 = 16777215 bytes. See RFC 9113 section 6.5.2 for details.
 * @param maxHeaderListSize The maximum field section size that the sender is prepared to accept in bytes. See RFC 9113 section 6.5.2 for details. Default is 8192.
 * @param maxRapidResets Maximum number of rapid resets(stream RST sent by client before any data have been sent by server). When reached within rapidResetCheckPeriod(), GOAWAY is sent to client and connection is closed. Default value is 50.
 * @param rapidResetCheckPeriod Period for counting rapid resets(stream RST sent by client before any data have been sent by server). Default value is PT10S.
 */
@ConfigData("server.http2")
public record WebServerHttp2Config(
        @Loggable @ConfigProperty(defaultValue = "PT15S") String flowControlTimeout,
        @Loggable @ConfigProperty(defaultValue = "1048576") int initialWindowSize,
        @Loggable @ConfigProperty(defaultValue = "8192") long maxConcurrentStreams,
        @Loggable @ConfigProperty(defaultValue = "10") int maxEmptyFrames,
        @Loggable @ConfigProperty(defaultValue = "16384") int maxFrameSize,
        @Loggable @ConfigProperty(defaultValue = "8192") long maxHeaderListSize,
        @Loggable @ConfigProperty(defaultValue = "50") int maxRapidResets,
        @Loggable @ConfigProperty(defaultValue = "PT10S") String rapidResetCheckPeriod) {}
