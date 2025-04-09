// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Stream;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for the HistoricalBlockFacilityImpl class.
 */
class HistoricalBlockFacilityImplTest {

    @Mock
    private BlockProviderPlugin provider1;

    @Mock
    private BlockProviderPlugin provider2;

    @Mock
    private ServiceLoader<BlockProviderPlugin> serviceLoader;

    @Mock
    private ServiceLoader.Provider<BlockProviderPlugin> provider1Provider;

    @Mock
    private ServiceLoader.Provider<BlockProviderPlugin> provider2Provider;

    private HistoricalBlockFacilityImpl facility;

    /**
     * Sets up the test environment before each test.
     */
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        facility = new HistoricalBlockFacilityImpl(List.of(provider1, provider2));
    }

    /**
     * Tests the constructor with ServiceLoader.
     */
    @SuppressWarnings("rawtypes")
    @Test
    @DisplayName("Test constructor with ServiceLoader")
    void testConstructorWithServiceLoader() {
        try (MockedStatic<ServiceLoader> mockedServiceLoader = mockStatic(ServiceLoader.class)) {
            mockedServiceLoader
                    .when(() -> ServiceLoader.load(
                            BlockProviderPlugin.class, getClass().getClassLoader()))
                    .thenReturn(serviceLoader);
            when(provider1Provider.get()).thenReturn(provider1);
            when(provider2Provider.get()).thenReturn(provider2);
            when(serviceLoader.stream()).thenReturn(Stream.of(provider1Provider, provider2Provider));

            assertEquals(
                    serviceLoader,
                    ServiceLoader.load(BlockProviderPlugin.class, getClass().getClassLoader()));

            final HistoricalBlockFacilityImpl facility = new HistoricalBlockFacilityImpl();

            final List<BlockProviderPlugin> providers = facility.allBlockProvidersPlugins();
            assertNotNull(providers);
            assertEquals(2, providers.size());
            assertTrue(providers.contains(provider1));
            assertTrue(providers.contains(provider2));
        }
    }

    /**
     * Tests block retrieval.
     */
    @Test
    @DisplayName("Test block retrieval")
    void testBlock() {
        final long blockNumber = 123L;
        final BlockAccessor blockAccessor = mock(BlockAccessor.class);

        when(provider1.block(blockNumber)).thenReturn(null);
        when(provider2.block(blockNumber)).thenReturn(blockAccessor);

        final BlockAccessor result = facility.block(blockNumber);

        assertNotNull(result);
        assertEquals(blockAccessor, result);
        verify(provider1).block(blockNumber);
        verify(provider2).block(blockNumber);
    }

    /**
     * Tests block retrieval when the block is not found.
     */
    @Test
    @DisplayName("Test block not found")
    void testBlockNotFound() {
        final long blockNumber = 123L;

        when(provider1.block(blockNumber)).thenReturn(null);
        when(provider2.block(blockNumber)).thenReturn(null);

        final BlockAccessor result = facility.block(blockNumber);

        assertNull(result);
        verify(provider1).block(blockNumber);
        verify(provider2).block(blockNumber);
    }

    @Test
    void testAllBlockProvidersPlugins() {
        List<BlockProviderPlugin> providers = facility.allBlockProvidersPlugins();

        assertNotNull(providers);
        assertEquals(2, providers.size());
        assertTrue(providers.contains(provider1));
        assertTrue(providers.contains(provider2));
    }

    /**
     * Tests the availableBlocks method.
     */
    @Test
    @DisplayName("Test availableBlocks")
    void testAvailableBlocks() {
        BlockRangeSet mockRangeSet = mock(BlockRangeSet.class);
        when(provider1.availableBlocks()).thenReturn(mockRangeSet);
        when(provider2.availableBlocks()).thenReturn(mockRangeSet);

        facility = new HistoricalBlockFacilityImpl(List.of(provider1, provider2));
        BlockRangeSet result = facility.availableBlocks();

        assertNotNull(result);
        assertEquals(facility.availableBlocks(), result);
    }

    /**
     * Tests the toString method.
     */
    @Test
    @DisplayName("Test toString method")
    void testToString() {
        BlockRangeSet mockRangeSet = mock(BlockRangeSet.class);
        when(provider1.availableBlocks()).thenReturn(mockRangeSet);
        when(provider2.availableBlocks()).thenReturn(mockRangeSet);

        facility = new HistoricalBlockFacilityImpl(List.of(provider1, provider2));
        String result = facility.toString();

        assertNotNull(result);
        assertTrue(result.contains("HistoricalBlockFacilityImpl"));
        assertTrue(result.contains("availableBlocks="));
        assertTrue(result.contains(provider1.getClass().getSimpleName()));
        assertTrue(result.contains(provider2.getClass().getSimpleName()));
    }
}
