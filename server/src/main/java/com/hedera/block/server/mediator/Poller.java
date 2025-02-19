// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.mediator;

public interface Poller<V> {
    V poll() throws Exception;
}
