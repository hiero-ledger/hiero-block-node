// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.recent;

import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import org.hiero.block.node.base.CompressionType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Test class for {@link BlockFileBlockAccessor}.
 */
@DisplayName("BlockFileBlockAccessor Tests")
class BlockFileBlockAccessorTest {
    /** Test Base Path, resolved under jimfs. */
    private Path testBasePath;

    /**
     * Environment setup for the test class.
     */
    @SuppressWarnings("resource")
    @BeforeEach
    void setup() {
        // Initialize the in-memory file system
        final FileSystem jimfs = Jimfs.newFileSystem(Configuration.unix());
        testBasePath = jimfs.getPath("/tmp");
    }

    /**
     * Tests for the {@link BlockFileBlockAccessor} constructor.
     */
    @Nested
    @DisplayName("Constructor Tests")
    @SuppressWarnings("all")
    final class ConstructorTests {
        /**
         * This test asserts that a {@link NullPointerException} is thrown when
         * the input base path is null.
         */
        @Test
        void testNullBasePath() {
            // call && assert
            assertThatNullPointerException()
                    .isThrownBy(() ->
                            new BlockFileBlockAccessor(null, testBasePath.resolve("1.blk"), CompressionType.ZSTD));
        }
    }
}
