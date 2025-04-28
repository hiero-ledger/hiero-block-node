// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.stream.Stream;
import org.hiero.block.node.base.ranges.ConcurrentLongRangeSet;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for the HistoricalBlockFacilityImpl class.
 */
class HistoricalBlockFacilityImplTest {

    @Mock
    private BlockProviderPlugin provider1;

    @Mock
    private BlockProviderPlugin provider2;

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
    @Test
    @DisplayName("Test constructor with ServiceLoader")
    void testConstructorWithServiceLoader() {
        final ServiceLoaderFunction serviceLoaderFunction = new ServiceLoaderFunction() {
            @SuppressWarnings("unchecked")
            @Override
            public <C> Stream<? extends C> loadServices(Class<C> serviceClass) {
                if (serviceClass == BlockProviderPlugin.class) {
                    return Stream.of(provider1, provider2).map(service -> (C) service);
                }
                return super.loadServices(serviceClass);
            }
        };
        // check that the custom service loader function is working
        assertEquals(
                provider1,
                serviceLoaderFunction
                        .loadServices(BlockProviderPlugin.class)
                        .findFirst()
                        .orElse(null));
        // create the facility with the custom service loader function
        final HistoricalBlockFacilityImpl facility = new HistoricalBlockFacilityImpl(serviceLoaderFunction);
        // check that the facility has the correct providers
        final List<BlockProviderPlugin> providers = facility.allBlockProvidersPlugins();
        assertNotNull(providers);
        assertEquals(2, providers.size());
        assertTrue(providers.contains(provider1));
        assertTrue(providers.contains(provider2));
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

    /**
     * Tests block retrieval with providers in priority order.
     */
    @Test
    @DisplayName("Test block retrieval respects provider priority")
    void testBlockRetrievalWithPriority() {
        // Create providers with different priorities
        BlockProviderPlugin highPriorityProvider = mock(BlockProviderPlugin.class);
        when(highPriorityProvider.defaultPriority()).thenReturn(100);

        BlockProviderPlugin mediumPriorityProvider = mock(BlockProviderPlugin.class);
        when(mediumPriorityProvider.defaultPriority()).thenReturn(50);

        BlockProviderPlugin lowPriorityProvider = mock(BlockProviderPlugin.class);
        when(lowPriorityProvider.defaultPriority()).thenReturn(10);

        // Set up available blocks for all providers
        BlockRangeSet mockRangeSet = mock(BlockRangeSet.class);
        when(highPriorityProvider.availableBlocks()).thenReturn(mockRangeSet);
        when(mediumPriorityProvider.availableBlocks()).thenReturn(mockRangeSet);
        when(lowPriorityProvider.availableBlocks()).thenReturn(mockRangeSet);

        // Create facility with providers in unsorted order
        HistoricalBlockFacilityImpl priorityFacility = new HistoricalBlockFacilityImpl(
                List.of(lowPriorityProvider, highPriorityProvider, mediumPriorityProvider));

        // Verify the providers are sorted by priority
        List<BlockProviderPlugin> sortedProviders = priorityFacility.allBlockProvidersPlugins();
        assertEquals(3, sortedProviders.size());
        assertEquals(highPriorityProvider, sortedProviders.get(0));
        assertEquals(mediumPriorityProvider, sortedProviders.get(1));
        assertEquals(lowPriorityProvider, sortedProviders.get(2));

        // Test block retrieval checks providers in priority order
        long blockNumber = 456L;
        BlockAccessor mockAccessor = mock(BlockAccessor.class);

        // Only the medium priority provider has the block
        when(highPriorityProvider.block(blockNumber)).thenReturn(null);
        when(mediumPriorityProvider.block(blockNumber)).thenReturn(mockAccessor);
        // Low priority provider should not be checked

        BlockAccessor result = priorityFacility.block(blockNumber);

        assertNotNull(result);
        assertEquals(mockAccessor, result);

        // Verify high priority provider was checked first
        verify(highPriorityProvider).block(blockNumber);
        // Verify medium priority provider was checked second
        verify(mediumPriorityProvider).block(blockNumber);
        // Verify low priority provider was not checked since medium priority provider had the block
        verify(lowPriorityProvider, org.mockito.Mockito.never()).block(blockNumber);
    }

    /**
     * Tests block retrieval when the first provider has the block.
     */
    @Test
    @DisplayName("Test block retrieval when first provider has the block")
    void testBlockRetrievalFirstProvider() {
        final long blockNumber = 789L;
        final BlockAccessor blockAccessor = mock(BlockAccessor.class);

        when(provider1.block(blockNumber)).thenReturn(blockAccessor);
        // provider2 should not be checked

        final BlockAccessor result = facility.block(blockNumber);

        assertNotNull(result);
        assertEquals(blockAccessor, result);
        verify(provider1).block(blockNumber);
        // Verify second provider was not checked since first provider had the block
        verify(provider2, org.mockito.Mockito.never()).block(blockNumber);
    }

    /**
     * Tests the toString method more comprehensively.
     */
    @Test
    @DisplayName("Test toString method comprehensively")
    void testToStringComprehensive() {
        // Mock providers with recognizable class names
        BlockProviderPlugin provider1 = mock(BlockProviderPlugin.class, "MockProvider1");
        BlockProviderPlugin provider2 = mock(BlockProviderPlugin.class, "MockProvider2");

        // Set up mock block range set
        when(provider1.availableBlocks()).thenReturn(new ConcurrentLongRangeSet(1, 10));
        when(provider2.availableBlocks()).thenReturn(new ConcurrentLongRangeSet(11, 20));

        // Create the facility with mocked providers
        HistoricalBlockFacilityImpl testFacility = new HistoricalBlockFacilityImpl(List.of(provider1, provider2));

        // Get the toString result
        String result = testFacility.toString();
        System.out.println("result = " + result);

        // Verify the result contains expected components
        assertNotNull(result);
        assertTrue(result.startsWith("HistoricalBlockFacilityImpl{"), "Should start with class name");
        assertTrue(result.contains("availableBlocks="), "Should include availableBlocks label");
        assertTrue(result.contains("availableBlocks=[1->20]"), "Should include availableBlocks range");
        assertTrue(result.contains("providers=["), "Should include providers label");

        // Verify provider class names are included
        assertTrue(result.contains(provider1.getClass().getSimpleName()), "Should include first provider class name");
    }
}
