// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.live;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Direct unit tests for the in-house {@link LiveState} and {@link LiveStateSnapshotIO}.
 * The plugin uses both as the foundation of every other story in this epic, so they
 * deserve coverage independent of any plugin lifecycle wiring.
 */
class LiveStateTest {

    @Test
    void readsAndWritesAllThreeStateTypes() {
        final LiveState state = new LiveState();

        state.updateSingleton(1, Bytes.fromHex("aa"));
        state.updateKv(2, Bytes.fromHex("01"), Bytes.fromHex("bb"));
        state.updateKv(2, Bytes.fromHex("02"), Bytes.fromHex("cc"));
        state.pushQueue(3, Bytes.fromHex("dd"));
        state.pushQueue(3, Bytes.fromHex("ee"));

        assertThat(state.getSingleton(1)).isEqualTo(Bytes.fromHex("aa"));
        assertThat(state.getKv(2, Bytes.fromHex("01"))).isEqualTo(Bytes.fromHex("bb"));
        assertThat(state.getKv(2, Bytes.fromHex("02"))).isEqualTo(Bytes.fromHex("cc"));
        assertThat(state.getQueueAsList(3)).containsExactly(Bytes.fromHex("dd"), Bytes.fromHex("ee"));
        assertThat(state.peekQueue(3, 0)).isEqualTo(Bytes.fromHex("dd"));
        assertThat(state.peekQueue(3, 1)).isEqualTo(Bytes.fromHex("ee"));
        assertThat(state.peekQueue(3, 2)).isNull();
        assertThat(state.size()).isEqualTo(5L);

        state.removeKv(2, Bytes.fromHex("01"));
        state.popQueue(3);
        state.removeSingleton(1);
        assertThat(state.size()).isEqualTo(2L);
    }

    @Test
    void hashIsDeterministicAcrossInsertOrder() {
        final LiveState a = new LiveState();
        a.updateKv(7, Bytes.fromHex("0102"), Bytes.fromHex("aa"));
        a.updateSingleton(1, Bytes.fromHex("bb"));
        a.updateKv(7, Bytes.fromHex("0304"), Bytes.fromHex("cc"));

        final LiveState b = new LiveState();
        b.updateSingleton(1, Bytes.fromHex("bb"));
        b.updateKv(7, Bytes.fromHex("0304"), Bytes.fromHex("cc"));
        b.updateKv(7, Bytes.fromHex("0102"), Bytes.fromHex("aa"));

        assertThat(a.computeHash()).isEqualTo(b.computeHash());

        // Sensitivity check: any change must change the hash.
        b.updateSingleton(1, Bytes.fromHex("bc"));
        assertThat(a.computeHash()).isNotEqualTo(b.computeHash());
    }

    @Test
    void snapshotRoundTripsAllThreeStateTypes(@TempDir final Path tmp) throws Exception {
        final LiveState original = new LiveState();
        original.updateSingleton(1, Bytes.fromHex("aa"));
        original.updateKv(2, Bytes.fromHex("01"), Bytes.fromHex("bb"));
        original.pushQueue(3, Bytes.fromHex("dd"));
        original.pushQueue(3, Bytes.fromHex("ee"));
        final Bytes originalHash = original.computeHash();

        final Path target = tmp.resolve("snap").resolve("state.bin");
        LiveStateSnapshotIO.write(target, original);
        assertThat(Files.exists(target)).isTrue();

        final LiveState restored = new LiveState();
        LiveStateSnapshotIO.read(target, restored);

        assertThat(restored.computeHash()).isEqualTo(originalHash);
        assertThat(restored.getSingleton(1)).isEqualTo(Bytes.fromHex("aa"));
        assertThat(restored.getKv(2, Bytes.fromHex("01"))).isEqualTo(Bytes.fromHex("bb"));
        assertThat(restored.getQueueAsList(3)).containsExactly(Bytes.fromHex("dd"), Bytes.fromHex("ee"));
    }
}
