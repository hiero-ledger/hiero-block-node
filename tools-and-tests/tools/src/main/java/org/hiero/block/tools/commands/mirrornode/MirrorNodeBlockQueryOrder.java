// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.mirrornode;

public enum MirrorNodeBlockQueryOrder {
    DESC("desc"),
    ASC("asc");
    private final String value;

    MirrorNodeBlockQueryOrder(String value) {
        this.value = value;
    }
}
