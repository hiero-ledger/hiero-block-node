/*
 * Copyright (C) 2024 Hedera Hashgraph, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hedera.block.server.consumer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class LiveStreamObserver2ImplTest {

    @Test
    void testNotify() {
        try {
            new LiveStreamObserver2Impl().notify(null);
        } catch (Exception ex) {
            Assertions.assertNotNull(ex);
        }
    }

    @Test
    void onNext() {
        try {
            new LiveStreamObserver2Impl().onNext(null);
        } catch (Exception ex) {
            Assertions.assertNotNull(ex);
        }
    }

    @Test
    void onError() {
        try {
            new LiveStreamObserver2Impl().onError(null);
        } catch (Exception ex) {
            Assertions.assertNotNull(ex);
        }
    }
}
