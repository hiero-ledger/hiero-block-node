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

package com.hedera.block.server.config;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.hedera.block.server.metrics.MetricsService;
import com.swirlds.config.api.Configuration;
import com.swirlds.metrics.api.Metrics;
import org.junit.jupiter.api.Test;

class BlockNodeContextTest {

    @Test
    void BlockNodeContext_initializesWithMetricsAndConfiguration() {
        Metrics metrics = mock(Metrics.class);
        Configuration configuration = mock(Configuration.class);
        MetricsService metricsService = mock(MetricsService.class);

        BlockNodeContext context = new BlockNodeContext(metrics, metricsService, configuration);

        assertEquals(metrics, context.metrics());
        assertEquals(metricsService, context.metricsService());
        assertEquals(configuration, context.configuration());
    }
}
