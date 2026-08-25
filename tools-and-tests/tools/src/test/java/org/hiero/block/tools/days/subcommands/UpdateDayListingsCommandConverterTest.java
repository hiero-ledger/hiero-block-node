// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.subcommands;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.hiero.block.tools.days.listing.ListingRecordFile;
import org.hiero.block.tools.records.ChainFile;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/// Regression tests for
/// [UpdateDayListingsCommand#convertToListingRecordFileStatic(ChainFile)].
///
/// GCS composite objects (created via multipart upload or
/// `gcloud storage compose`) have no MD5 exposed in object metadata, so the
/// bucket lister surfaces it as `null`. Before #3546, the converter passed the
/// `null` straight into `Base64.getDecoder().decode(...)` and crashed the whole
/// day's listing with an NPE. `ListingRecordFile` requires a 32-char hex MD5 and
/// keys `equals` / `hashCode` on it, so we can't fake an empty MD5 either. The
/// converter must return `null` for the caller to skip, so the listing stays
/// consistent for the objects it does have.
final class UpdateDayListingsCommandConverterTest {

    /// A well-formed record-file path so the ChainFile constructor can derive
    /// kind + blockTime + sidecarIndex without failing before we get to the
    /// code under test.
    private static final String RECORD_PATH = "recordstreams/record0.0.3/2026-08-14T14_48_11.516674231Z.rcd.gz";

    /// A valid Base64-encoded MD5 (the MD5 of the empty string) and its hex form.
    /// Value doesn't matter beyond being a well-formed 16-byte MD5, used only to
    /// verify the happy path still decodes correctly.
    private static final String VALID_MD5_BASE64 = "1B2M2Y8AsgTpgAmY7PhCfg==";
    private static final String VALID_MD5_HEX = "d41d8cd98f00b204e9800998ecf8427e";

    @Nested
    @DisplayName("convertToListingRecordFileStatic(ChainFile) — MD5 handling")
    class Md5Handling {

        @Test
        @DisplayName("null MD5 → returns null so caller skips (GCS composite object case, #3546)")
        void nullMd5ReturnsNull() {
            final ChainFile compositeObject = new ChainFile(3, RECORD_PATH, 12345, null);
            final ListingRecordFile result = UpdateDayListingsCommand.convertToListingRecordFileStatic(compositeObject);
            assertNull(
                    result,
                    "null MD5 must return null so the caller can skip the object — "
                            + "GCS composite objects legitimately have no MD5 to record");
        }

        @Test
        @DisplayName("empty MD5 string → returns null so caller skips")
        void emptyMd5ReturnsNull() {
            final ChainFile emptyMd5 = new ChainFile(3, RECORD_PATH, 12345, "");
            final ListingRecordFile result = UpdateDayListingsCommand.convertToListingRecordFileStatic(emptyMd5);
            assertNull(result, "empty-string MD5 must be treated the same as null");
        }

        @Test
        @DisplayName("valid Base64 MD5 → decoded hex (regression pin for happy path)")
        void validMd5ProducesHex() {
            final ChainFile good = new ChainFile(3, RECORD_PATH, 12345, VALID_MD5_BASE64);
            final ListingRecordFile result = UpdateDayListingsCommand.convertToListingRecordFileStatic(good);
            assertNotNull(result);
            assertEquals(VALID_MD5_HEX, result.md5Hex());
        }
    }
}
