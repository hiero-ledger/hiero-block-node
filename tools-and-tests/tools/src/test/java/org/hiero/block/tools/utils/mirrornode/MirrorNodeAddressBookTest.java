// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.utils.mirrornode;

import static org.junit.jupiter.api.Assertions.assertFalse;

import com.hedera.hapi.node.base.NodeAddressBook;
import org.junit.jupiter.api.Test;

public class MirrorNodeAddressBookTest {
    @Test
    public void testLoadJsonAddressBook() {
        NodeAddressBook ab = MirrorNodeAddressBook.loadJsonAddressBook(
                MirrorNodeAddressBookTest.class.getClassLoader().getResource("/address-book-oct-2025.json"));

        assertFalse(ab.nodeAddress().isEmpty());
    }
}
