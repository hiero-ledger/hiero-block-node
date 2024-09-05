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

package com.hedera.block.server.mediator;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.hedera.block.server.ServiceStatus;
import com.hedera.block.server.config.BlockNodeContext;
import com.hedera.block.server.data.ObjectEvent;
import com.hedera.block.server.persistence.storage.write.BlockWriter;
import com.hedera.hapi.block.SubscribeStreamResponse;
import com.hedera.hapi.block.stream.BlockItem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MediatorInjectionModuleTest {

    @Mock private BlockWriter<BlockItem> blockWriter;

    @Mock private BlockNodeContext blockNodeContext;

    @Mock private ServiceStatus serviceStatus;

    @BeforeEach
    void setup() {
        // Any setup before each test can be done here
    }

    @Test
    void testProvidesStreamMediator() {
        // Call the method under test
        StreamMediator<BlockItem, ObjectEvent<SubscribeStreamResponse>> streamMediator =
                MediatorInjectionModule.providesStreamMediator(blockNodeContext, serviceStatus);

        // Verify that the streamMediator is correctly instantiated
        assertNotNull(streamMediator);
    }
}
