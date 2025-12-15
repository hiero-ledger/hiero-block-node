// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.downloadlive;

import java.time.Duration;
import java.time.Instant;
import org.hiero.block.tools.days.subcommands.DownloadLive;
import picocli.CommandLine;

/**
 * Utility class for parsing and handling date/time values in the live download context.
 *
 * <p>Provides convenience methods for:
 * <ul>
 *   <li>Parsing human-readable duration strings (e.g., "60s", "2m", "1h")</li>
 *   <li>Converting mirror node timestamp strings to Java Instant objects</li>
 * </ul>
 */
public class DateUtil {

    /**
     * Parses a human-readable duration string into a Duration object.
     *
     * <p>Supports multiple formats:
     * <ul>
     *   <li><b>Milliseconds:</b> {@code "100ms"} → 100 milliseconds</li>
     *   <li><b>Seconds:</b> {@code "60s"} → 60 seconds</li>
     *   <li><b>Minutes:</b> {@code "2m"} → 2 minutes</li>
     *   <li><b>Hours:</b> {@code "1h"} → 1 hour</li>
     *   <li><b>ISO-8601:</b> {@code "PT1M"} → 1 minute (standard Java format)</li>
     * </ul>
     *
     * @param text the duration string to parse (e.g., "60s", "2m", "PT1M")
     * @return the parsed Duration object
     * @throws CommandLine.ParameterException if the text cannot be parsed as a valid duration
     */
    public static Duration parseHumanDuration(String text) {
        try {
            if (text.endsWith("ms")) {
                long ms = Long.parseLong(text.substring(0, text.length() - 2));
                return Duration.ofMillis(ms);
            } else if (text.endsWith("s")) {
                long s = Long.parseLong(text.substring(0, text.length() - 1));
                return Duration.ofSeconds(s);
            } else if (text.endsWith("m")) {
                long m = Long.parseLong(text.substring(0, text.length() - 1));
                return Duration.ofMinutes(m);
            } else if (text.endsWith("h")) {
                long h = Long.parseLong(text.substring(0, text.length() - 1));
                return Duration.ofHours(h);
            } else {
                // Fall back to standard ISO-8601 parsing (e.g., PT1M, PT30S)
                return Duration.parse(text);
            }
        } catch (Exception e) {
            throw new CommandLine.ParameterException(
                    new CommandLine(new DownloadLive()),
                    "Invalid duration: " + text + " (use forms like 60s, 2m, 1h, PT1M)");
        }
    }

    /**
     * Parses a mirror node timestamp string into an Instant.
     *
     * <p>Mirror node timestamps are represented as Unix epoch timestamps with nanosecond
     * precision in the format: {@code "<seconds>.<nanoseconds>"}
     *
     * <p>Examples:
     * <ul>
     *   <li>{@code "1762898218.515837000"} → Instant representing that exact moment</li>
     *   <li>{@code "1762898218"} → Instant with zero nanoseconds</li>
     * </ul>
     *
     * <p>The nanosecond component is automatically normalized to 9 digits:
     * <ul>
     *   <li>If longer than 9 digits, it's truncated to 9</li>
     *   <li>If shorter than 9 digits, it's padded with zeros</li>
     * </ul>
     *
     * @param ts the mirror timestamp string to parse (e.g., "1762898218.515837000")
     * @return the parsed Instant, or null if the input is null, empty, or malformed
     */
    public static Instant parseMirrorTimestamp(String ts) {
        if (ts == null || ts.isEmpty()) return null;
        try {
            int dot = ts.indexOf('.');
            if (dot < 0) {
                // No fractional seconds - just epoch seconds
                long seconds = Long.parseLong(ts);
                return Instant.ofEpochSecond(seconds, 0);
            }
            // Parse seconds and nanoseconds separately
            long seconds = Long.parseLong(ts.substring(0, dot));
            String nanoStr = ts.substring(dot + 1);

            // Normalize nanoseconds to exactly 9 digits
            if (nanoStr.length() > 9) {
                nanoStr = nanoStr.substring(0, 9); // Truncate if too long
            }
            while (nanoStr.length() < 9) {
                nanoStr += "0"; // Pad with zeros if too short
            }

            int nanos = Integer.parseInt(nanoStr);
            return Instant.ofEpochSecond(seconds, nanos);
        } catch (Exception e) {
            return null; // Gracefully handle malformed input
        }
    }
}
