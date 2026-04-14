// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.records;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("ChainFile")
class ChainFileTest {

    @Nested
    @DisplayName("Kind.fromFilePath")
    class FromFilePath {

        @Test
        @DisplayName("recognizes .rcd files")
        void recognizesRcd() {
            assertEquals(
                    ChainFile.Kind.RECORD,
                    ChainFile.Kind.fromFilePath("recordstreams/record0.0.3/2026-03-21T00_00_00.000000000Z.rcd"));
        }

        @Test
        @DisplayName("recognizes .rcd.gz files")
        void recognizesRcdGz() {
            assertEquals(
                    ChainFile.Kind.RECORD,
                    ChainFile.Kind.fromFilePath("recordstreams/record0.0.3/2026-03-21T00_00_00.000000000Z.rcd.gz"));
        }

        @Test
        @DisplayName("recognizes .rcd_sig files")
        void recognizesRcdSig() {
            assertEquals(
                    ChainFile.Kind.SIGNATURE,
                    ChainFile.Kind.fromFilePath("recordstreams/record0.0.3/2026-03-21T00_00_00.000000000Z.rcd_sig"));
        }

        @Test
        @DisplayName("recognizes sidecar files")
        void recognizesSidecar() {
            assertEquals(
                    ChainFile.Kind.SIDECAR,
                    ChainFile.Kind.fromFilePath(
                            "recordstreams/record0.0.3/sidecar/2026-03-21T00_00_00.000000000Z_01.rcd.gz"));
        }

        @Test
        @DisplayName("returns null for malformed file with trailing hash suffix on rcd_sig")
        void returnsNullForMalformedRcdSig() {
            assertNull(
                    ChainFile.Kind.fromFilePath(
                            "recordstreams/record0.0.15/2026-03-21T06_42_18.024923000Z.rcd_sig-d41d8cd98f00b204e9800998ecf8427e"));
        }

        @Test
        @DisplayName("returns null for malformed file with trailing hash suffix on rcd.gz")
        void returnsNullForMalformedRcdGz() {
            assertNull(
                    ChainFile.Kind.fromFilePath(
                            "recordstreams/record0.0.15/2026-03-21T06_42_18.024923000Z.rcd.gz-d41d8cd98f00b204e9800998ecf8427e"));
        }

        @Test
        @DisplayName("returns null for completely unknown extension")
        void returnsNullForUnknownExtension() {
            assertNull(ChainFile.Kind.fromFilePath("recordstreams/record0.0.3/somefile.txt"));
        }
    }

    @Nested
    @DisplayName("createOrNull")
    class CreateOrNull {

        @Test
        @DisplayName("returns ChainFile for valid path")
        void returnsChainFileForValidPath() {
            ChainFile cf = ChainFile.createOrNull(
                    3, "recordstreams/record0.0.3/2026-03-21T00_00_00.000000000Z.rcd.gz", 1024, "abc123");
            assertNotNull(cf);
            assertEquals(ChainFile.Kind.RECORD, cf.kind());
            assertEquals(3, cf.nodeAccountId());
        }

        @Test
        @DisplayName("returns null for malformed path")
        void returnsNullForMalformedPath() {
            ChainFile cf = ChainFile.createOrNull(
                    15,
                    "recordstreams/record0.0.15/2026-03-21T06_42_18.024923000Z.rcd_sig-d41d8cd98f00b204e9800998ecf8427e",
                    0,
                    "d41d8cd98f00b204e9800998ecf8427e");
            assertNull(cf);
        }
    }
}
