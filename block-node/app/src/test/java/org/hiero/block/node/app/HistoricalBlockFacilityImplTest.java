// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Stream;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.hiero.block.node.spi.historicalblocks.BlockProviderPlugin;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
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

    /**
     * Tests retrieval of the oldest block number.
     */
    @Test
    @DisplayName("Test oldest block number")
    void testOldestBlockNumber() {
        when(provider1.oldestBlockNumber()).thenReturn(100L);
        when(provider2.oldestBlockNumber()).thenReturn(200L);

        final long result = facility.oldestBlockNumber();

        assertEquals(100L, result);
    }

    /**
     * Tests retrieval of the oldest block number when it is unknown.
     */
    @Test
    @DisplayName("Test oldest block number unknown")
    void testOldestBlockNumberUnknown() {
        when(provider1.oldestBlockNumber()).thenReturn(BlockProviderPlugin.UNKNOWN_BLOCK_NUMBER);
        when(provider2.oldestBlockNumber()).thenReturn(BlockProviderPlugin.UNKNOWN_BLOCK_NUMBER);

        final long result = facility.oldestBlockNumber();

        assertEquals(HistoricalBlockFacility.UNKNOWN_BLOCK_NUMBER, result);
    }

    /**
     * Tests retrieval of the latest block number.
     */
    @Test
    @DisplayName("Test latest block number")
    void testLatestBlockNumber() {
        when(provider1.latestBlockNumber()).thenReturn(300L);
        when(provider2.latestBlockNumber()).thenReturn(400L);

        final long result = facility.latestBlockNumber();

        assertEquals(400L, result);
    }

    /**
     * Tests retrieval of the latest block number when it is unknown.
     */
    @Test
    @DisplayName("Test latest block number unknown")
    void testLatestBlockNumberUnknown() {
        when(provider1.latestBlockNumber()).thenReturn(BlockProviderPlugin.UNKNOWN_BLOCK_NUMBER);
        when(provider2.latestBlockNumber()).thenReturn(BlockProviderPlugin.UNKNOWN_BLOCK_NUMBER);

        final long result = facility.latestBlockNumber();

        assertEquals(HistoricalBlockFacility.UNKNOWN_BLOCK_NUMBER, result);
    }

    /**
     * Tests the toString method.
     */
    @Test
    @DisplayName("Test toString method")
    void testToString() {
        when(provider1.oldestBlockNumber()).thenReturn(100L);
        when(provider2.oldestBlockNumber()).thenReturn(200L);
        when(provider1.latestBlockNumber()).thenReturn(300L);
        when(provider2.latestBlockNumber()).thenReturn(400L);

        final String result = facility.toString();

        assertTrue(result.contains("oldest=100"));
        assertTrue(result.contains("latest=400"));
        assertTrue(result.contains("providers=[" + provider1.getClass().getSimpleName() + ", "
                + provider2.getClass().getSimpleName() + "]"));
    }

    @Test
    void testAllBlockProvidersPlugins() {
        List<BlockProviderPlugin> providers = facility.allBlockProvidersPlugins();

        assertNotNull(providers);
        assertEquals(2, providers.size());
        assertTrue(providers.contains(provider1));
        assertTrue(providers.contains(provider2));
    }
}
