// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.expanded.cloud.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.util.Collections;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Contract tests for {@link NoOpS3Client}. Verifies that the two interface methods do not
 * throw and are safe to call in any order.
 */
class NoOpS3ClientTest {

    @Test
    @DisplayName("uploadFile and close do not throw")
    void noOpMethodsDoNotThrow() {
        final NoOpS3Client client = new NoOpS3Client();
        assertDoesNotThrow(() -> client.uploadFile(
                "blocks/0000/0000/0000/0000/001.blk.zstd",
                "STANDARD",
                Collections.singletonList(new byte[] {1, 2, 3}).iterator(),
                "application/octet-stream"));
        assertDoesNotThrow(client::close);
    }
}
