// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.archive;

import static java.lang.System.Logger.Level.DEBUG;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.List;

/// Encodes and decodes S3 object keys for tar archive groups.
///
/// Keys are structured paths of 4-digit segments derived from the group's first block number,
/// e.g. `0000/0000/0000/0001/23.tar` for grouping level 2, first block 1230.
/// The last segment has its leading zeros stripped (standard integer formatting), so the
/// round-trip `parse(format(groupStart, level), level) == groupStart` always holds.
final class ArchiveKey {

    private static final System.Logger LOGGER = System.getLogger(ArchiveKey.class.getName());

    /// Sentinel returned by [#parse] for keys that do not conform to the archive key format
    /// produced by [#format]. The bucket may legitimately contain foreign objects created by
    /// other tools; their keys must not fail archiving, they are logged at debug and skipped.
    static final long UNPARSEABLE = -1L;

    /// Width, in characters, of each `/`-delimited path segment produced by [format] and consumed
    /// by [parse] (and, for the segment count alone, by [StartupRecoveryTask#directoryDepth]).
    static final int PATH_SEGMENT_WIDTH = 4;

    /// Number of decimal digits `groupStart` is zero-padded to in [format]; this fits any `long`
    /// block number and keeps S3's key ordering numeric.
    static final int MAX_LONG_DIGITS = Long.toString(Long.MAX_VALUE).length();

    private ArchiveKey() {}

    /// Formats the S3 object key for the tar group whose first block number is `groupStart`,
    /// optionally prepending an `objectKeyPrefix`.
    ///
    /// The key is built by:
    /// 1. Zero-padding `groupStart` to 19 digits and taking the first `19 - groupingLevel` digits
    ///    (the group-level trailing digits are always zero and are omitted).
    /// 2. Splitting that into 4-character path segments.
    /// 3. Stripping leading zeros from the last segment (standard integer formatting).
    /// 4. Joining with `/` and appending `.tar`.
    ///
    /// When `objectKeyPrefix` is non-empty the returned key is `{prefix}/{block-path}.tar`.
    static @NonNull String format(long groupStart, int groupingLevel, @NonNull String objectKeyPrefix) {
        final String truncated =
                String.format("%0" + MAX_LONG_DIGITS + "d", groupStart).substring(0, MAX_LONG_DIGITS - groupingLevel);
        final List<String> parts = new ArrayList<>();
        for (int i = 0; i < truncated.length(); i += PATH_SEGMENT_WIDTH) {
            parts.add(truncated.substring(i, Math.min(i + PATH_SEGMENT_WIDTH, truncated.length())));
        }
        parts.set(parts.size() - 1, String.valueOf(Long.parseLong(parts.getLast())));
        final String blockPath = String.join("/", parts) + ".tar";
        return objectKeyPrefix.isEmpty() ? blockPath : objectKeyPrefix + "/" + blockPath;
    }

    /// Parses the first block number of the group represented by `key`, stripping a leading
    /// `objectKeyPrefix` if one is configured.
    ///
    /// This is the inverse of [format]: the last path segment has had its leading zeros stripped,
    /// so it is zero-padded back to its original width before the segments are concatenated and
    /// the group-level trailing zeros are restored.  When `objectKeyPrefix` is non-empty, the
    /// prefix and its trailing `/` separator are removed before parsing.
    ///
    /// A key that does not conform to the format produced by [format] (for example an object
    /// created in the bucket by another tool, such as
    /// `2026-05-01_21-32-38_0000000000000116700-0000000000000116799`, or a key outside the
    /// configured prefix) is not an error: it is logged at debug level and [#UNPARSEABLE] is
    /// returned so the caller can skip the key. This method never throws.
    ///
    /// **Width of the last segment** for each grouping level:
    /// | level | last-segment width |
    /// |---|---|
    /// | 1 | 2 |
    /// | 2 | 1 |
    /// | 3 | 4 |
    /// | 4 | 3 |
    /// | 5 | 2 |
    /// | 6 | 1 |
    ///
    /// @return the first block number of the group, or [#UNPARSEABLE] when the key does not
    ///     conform to the archive key format
    static long parse(@NonNull String key, int groupingLevel, @NonNull String objectKeyPrefix) {
        long result;
        try {
            result = parseStrict(key, groupingLevel, objectKeyPrefix);
        } catch (final RuntimeException e) {
            // Safety net only: parseStrict reports every condition it can detect by returning
            // UNPARSEABLE, so this catch handles genuinely unexpected exceptions from underlying
            // library calls (e.g. number parsing). Archiving must never fail on a foreign key.
            if (LOGGER.isLoggable(DEBUG)) {
                LOGGER.log(DEBUG, "Key %s is not a valid archive key".formatted(key), e);
            }
            result = UNPARSEABLE;
        }
        return result;
    }

    /// The parsing logic behind [parse]: parses a well formed archive key and returns
    /// [#UNPARSEABLE] for any key that detectably does not conform to the format produced by
    /// [format]. Exceptions are not used for flow control here; the only exceptions that can
    /// escape are unexpected ones from underlying library calls, which [parse] catches.
    private static long parseStrict(@NonNull String key, int groupingLevel, @NonNull String objectKeyPrefix) {
        final long result;
        if (!objectKeyPrefix.isEmpty() && !key.startsWith(objectKeyPrefix + "/")) {
            // Not under the configured prefix: not one of this node's archive keys.
            LOGGER.log(DEBUG, "Key {0} does not start with configured prefix {1}/", key, objectKeyPrefix);
            result = UNPARSEABLE;
        } else {
            final String unprefixed = objectKeyPrefix.isEmpty() ? key : key.substring(objectKeyPrefix.length() + 1);
            final String withoutSuffix =
                    unprefixed.endsWith(".tar") ? unprefixed.substring(0, unprefixed.length() - 4) : unprefixed;
            final String[] segments = withoutSuffix.split("/");
            if (!allSegmentsNumeric(segments)) {
                // format() only ever emits decimal digit segments; this is a foreign object
                // sharing the bucket.
                LOGGER.log(DEBUG, "Key {0} is not a valid archive key", key);
                result = UNPARSEABLE;
            } else {
                // Restore leading zeros stripped by format().
                final int lastSegmentWidth = lastSegmentWidth(groupingLevel);
                segments[segments.length - 1] =
                        String.format("%0" + lastSegmentWidth + "d", Long.parseLong(segments[segments.length - 1]));

                final String joined = String.join("", segments);
                if (joined.length() != MAX_LONG_DIGITS - groupingLevel) {
                    // format() always emits exactly this many digits for the grouping level; a
                    // different length means the key has the wrong number of segments or widths.
                    LOGGER.log(DEBUG, "Key {0} does not have the expected digit count", key);
                    result = UNPARSEABLE;
                } else {
                    // Append groupingLevel zeros: the dropped digits are always zero (they define
                    // the group boundary).
                    final long parsed = Long.parseLong(joined) * Math.powExact(10, groupingLevel);
                    if (parsed < 0) {
                        // The multiplication overflowed: the digits encode a block number beyond
                        // the long range, which format() can never produce.
                        LOGGER.log(DEBUG, "Key {0} does not encode a valid block number", key);
                        result = UNPARSEABLE;
                    } else {
                        result = parsed;
                    }
                }
            }
        }
        return result;
    }

    /// Returns true when every segment is non-empty, consists only of decimal digits, and the
    /// total digit count is small enough that no later [Long#parseLong] call can overflow. A
    /// valid archive key joins to at most [#MAX_LONG_DIGITS] minus one digits, because the
    /// grouping level is at least one.
    private static boolean allSegmentsNumeric(@NonNull final String[] segments) {
        boolean result = true;
        int totalLength = 0;
        for (final String segment : segments) {
            totalLength += segment.length();
            if (segment.isEmpty() || !segment.chars().allMatch(Character::isDigit)) {
                result = false;
                break;
            }
        }
        return result && totalLength < MAX_LONG_DIGITS;
    }

    /// Returns the width (number of characters) of the last path segment for a given grouping level.
    private static int lastSegmentWidth(int groupingLevel) {
        final int width = (MAX_LONG_DIGITS - groupingLevel) % PATH_SEGMENT_WIDTH;
        return width == 0 ? PATH_SEGMENT_WIDTH : width;
    }
}
