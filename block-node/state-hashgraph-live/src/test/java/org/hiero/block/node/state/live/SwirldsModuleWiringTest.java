// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.live;

import static org.assertj.core.api.Assertions.assertThat;

import com.swirlds.state.BinaryState;
import com.swirlds.state.State;
import com.swirlds.state.StateLifecycleManager;
import com.swirlds.state.merkle.VirtualMapState;
import com.swirlds.state.merkle.VirtualMapStateLifecycleManager;
import com.swirlds.virtualmap.VirtualMap;
import org.junit.jupiter.api.Test;

/**
 * Confidence test that the swirlds-state-api, swirlds-state-impl, and swirlds-virtualmap
 * modules are resolvable, on the module path, and exporting the symbols the live-state
 * plugin will rely on. This is a smoke test for the gradle wiring done in STORY-1B —
 * full lifecycle instantiation requires a Metrics + Time + Configuration triple that
 * belongs in STORY-4's plugin tests.
 */
class SwirldsModuleWiringTest {

    @Test
    void swirldsStateAndVirtualMapTypesAreReachable() {
        assertThat(State.class.getModule().getName()).isEqualTo("com.swirlds.state.api");
        assertThat(BinaryState.class.getModule().getName()).isEqualTo("com.swirlds.state.api");
        assertThat(StateLifecycleManager.class.getModule().getName()).isEqualTo("com.swirlds.state.api");
        assertThat(VirtualMapState.class.getModule().getName()).isEqualTo("com.swirlds.state.impl");
        assertThat(VirtualMapStateLifecycleManager.class.getModule().getName()).isEqualTo("com.swirlds.state.impl");
        assertThat(VirtualMap.class.getModule().getName()).isEqualTo("com.swirlds.virtualmap");
    }
}
