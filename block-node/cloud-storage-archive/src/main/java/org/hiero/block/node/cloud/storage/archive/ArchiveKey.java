// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.archive;

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

    private ArchiveKey() {}

    /// Formats the S3 object key for the tar group whose first block number is `groupStart`.
    ///
    /// The key is built by:
    /// 1. Zero-padding `groupStart` to 19 digits and taking the first `19 - groupingLevel` digits
    ///    (the group-level trailing digits are always zero and are omitted).
    /// 2. Splitting that prefix into 4-character path segments.
    /// 3. Stripping leading zeros from the last segment (standard integer formatting).
    /// 4. Joining with `/` and appending `.tar`.
    static @NonNull String format(long groupStart, int groupingLevel) {
        final String truncated = String.format("%019d", groupStart).substring(0, 19 - groupingLevel);
        final List<String> parts = new ArrayList<>();
        for (int i = 0; i < truncated.length(); i += 4) {
            parts.add(truncated.substring(i, Math.min(i + 4, truncated.length())));
        }
        parts.set(parts.size() - 1, String.valueOf(Long.parseLong(parts.getLast())));
        return String.join("/", parts) + ".tar";
    }

    /// Parses the first block number of the group represented by `key`.
    ///
    /// This is the inverse of [format]: the last path segment has had its leading zeros stripped,
    /// so it is zero-padded back to its original width before the segments are concatenated and
    /// the group-level trailing zeros are restored.
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
    static long parse(@NonNull String key, int groupingLevel) {
        final String withoutSuffix = key.endsWith(".tar") ? key.substring(0, key.length() - 4) : key;
        final String[] segments = withoutSuffix.split("/");

        // Restore leading zeros stripped by format().
        final int lastSegmentWidth = lastSegmentWidth(groupingLevel);
        segments[segments.length - 1] =
                String.format("%0" + lastSegmentWidth + "d", Long.parseLong(segments[segments.length - 1]));

        // Append groupingLevel zeros: the dropped digits are always zero (they define the group boundary).
        return Long.parseLong(String.join("", segments)) * Math.powExact(10, groupingLevel);
    }

    /// Returns the width (number of characters) of the last path segment for a given grouping level.
    private static int lastSegmentWidth(int groupingLevel) {
        final int width = (19 - groupingLevel) % 4;
        return width == 0 ? 4 : width;
    }
}
