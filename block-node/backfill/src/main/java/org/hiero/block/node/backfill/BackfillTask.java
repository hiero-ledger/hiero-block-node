// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

/** Backfill task built from a typed gap with origin metadata. */
final class BackfillTask {
    private final long id;
    private final TypedGap gap;
    private final BackfillPlugin.BackfillType origin;

    BackfillTask(long id, TypedGap gap, BackfillPlugin.BackfillType origin) {
        this.id = id;
        this.gap = gap;
        this.origin = origin;
    }

    long id() {
        return id;
    }

    TypedGap gap() {
        return gap;
    }

    BackfillPlugin.BackfillType origin() {
        return origin;
    }
}
