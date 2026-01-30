// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.hedera.hapi.block.stream.BlockItem;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for {@link MainnetOAState}. */
class MainnetOAStateTest {

    @Test
    void loadOaStateReturnsNonEmptyList() {
        List<BlockItem> blockItems = assertDoesNotThrow(MainnetOAState::loadOaState);
        assertNotNull(blockItems);
        assertFalse(blockItems.isEmpty(), "loadOaState should return a non-empty list of block items");
    }
}
