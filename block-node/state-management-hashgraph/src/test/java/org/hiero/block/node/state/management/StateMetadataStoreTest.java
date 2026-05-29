// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.management;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.hiero.block.api.StateMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Covers the JSON load / atomic-save behaviour of {@link StateMetadataStore},
 * including the missing-file and malformed-content edge cases.
 */
class StateMetadataStoreTest {

    @Test
    void loadAndSaveRoundTripAtomicallyAndHandleMissingAndCorruptFiles(@TempDir final Path tmp) throws IOException {
        final Path file = tmp.resolve("nested").resolve("stateMetadata.json");
        final StateMetadataStore store = new StateMetadataStore(file);

        // 1. Missing file -> Optional.empty().
        assertThat(store.load()).isEmpty();

        // 2. Save creates the parent dir and writes valid JSON.
        final StateMetadata original = StateMetadata.newBuilder()
                .blockNumber(123L)
                .roundNumber(42L)
                .stateRootHash(Bytes.fromHex("deadbeef"))
                .stateSize(7L)
                .build();
        store.save(original);
        assertThat(Files.exists(file)).isTrue();

        assertThat(store.load()).contains(original);

        // 3. Overwrite is atomic — no leftover .tmp.
        final StateMetadata next = original.copyBuilder().blockNumber(124L).build();
        store.save(next);
        assertThat(store.load()).contains(next);
        assertThat(Files.exists(file.resolveSibling(file.getFileName() + ".tmp"))).isFalse();

        // 4. Corrupt content raises IOException. PBJ's JSON parser tolerates a fair amount of
        // sloppy input, so we feed it bytes that simply can't be a JSON object.
        Files.write(file, new byte[] {0x00, (byte) 0xFF, (byte) 0xFE, 0x10});
        assertThatThrownBy(store::load).isInstanceOf(IOException.class);
    }
}
