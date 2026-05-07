// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.junit.jupiter.api.Test;
import java.io.IOException;

public class BlockNodeAppStoredBlocksTest {

    @Test
    public void testUpdateStoredBlocks() throws IOException {
        ServiceLoaderFunction serviceLoader = mock(ServiceLoaderFunction.class);
        BlockNodeApp app = new BlockNodeApp(serviceLoader, false);
        
        app.updateStoredBlocks(10, 20);
        app.updateStoredBlocks(30, 40);
        
        // We don't have a public getter for storedBlocks, but we can check metrics if we want.
        // For now, this just verifies that the method exists and doesn't crash.
    }
}
