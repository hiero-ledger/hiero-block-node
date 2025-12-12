// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.downloadlive;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link LivePoller}.
 *
 * <p>These tests focus on the state persistence logic which is the most
 * testable part of LivePoller without requiring extensive mocking.
 */
@DisplayName("LivePoller Tests")
public class LivePollerTest {

    @Nested
    @DisplayName("State Persistence Tests")
    class StatePersistenceTests {

        @TempDir
        Path tempDir;

        Path statePath;

        @BeforeEach
        void setup() {
            statePath = tempDir.resolve("state.json");
        }

        @Test
        @DisplayName("Should write and read state successfully")
        void testWriteAndReadState() {
            String dayKey = "2025-12-01";
            long lastSeenBlock = 12345L;
            State state = new State(dayKey, lastSeenBlock);

            LivePoller.writeState(statePath, state);
            State readState = LivePoller.readState(statePath);

            assertNotNull(readState);
            assertEquals(dayKey, readState.getDayKey());
            assertEquals(lastSeenBlock, readState.getLastSeenBlock());
        }

        @Test
        @DisplayName("Should return null when reading non-existent state file")
        void testReadNonExistentState() {
            Path nonExistentPath = tempDir.resolve("does-not-exist.json");

            State state = LivePoller.readState(nonExistentPath);

            assertNull(state);
        }

        @Test
        @DisplayName("Should handle null state path gracefully")
        void testNullStatePath() {
            State state = new State("2025-12-01", 100L);

            assertDoesNotThrow(() -> LivePoller.writeState(null, state));
            assertNull(LivePoller.readState(null));
        }

        @Test
        @DisplayName("Should handle null state gracefully")
        void testNullState() {
            assertDoesNotThrow(() -> LivePoller.writeState(statePath, null));
        }

        @Test
        @DisplayName("Should create parent directories when writing state")
        void testCreateParentDirectories() {
            Path nestedPath = tempDir.resolve("parent/child/state.json");
            State state = new State("2025-12-01", 100L);

            LivePoller.writeState(nestedPath, state);

            assertTrue(Files.exists(nestedPath.getParent()));
            assertTrue(Files.exists(nestedPath));
        }

        @Test
        @DisplayName("Should persist state with zero block number")
        void testPersistZeroBlockNumber() {
            State state = new State("2025-12-01", 0L);

            LivePoller.writeState(statePath, state);
            State readState = LivePoller.readState(statePath);

            assertNotNull(readState);
            assertEquals(0L, readState.getLastSeenBlock());
        }

        @Test
        @DisplayName("Should persist state with large block number")
        void testPersistLargeBlockNumber() {
            State state = new State("2025-12-01", Long.MAX_VALUE);

            LivePoller.writeState(statePath, state);
            State readState = LivePoller.readState(statePath);

            assertNotNull(readState);
            assertEquals(Long.MAX_VALUE, readState.getLastSeenBlock());
        }

        @Test
        @DisplayName("Should persist state with negative block number")
        void testPersistNegativeBlockNumber() {
            State state = new State("2025-12-01", -1L);

            LivePoller.writeState(statePath, state);
            State readState = LivePoller.readState(statePath);

            assertNotNull(readState);
            assertEquals(-1L, readState.getLastSeenBlock());
        }

        @Test
        @DisplayName("Should overwrite existing state file")
        void testOverwriteExistingState() throws IOException {
            State state1 = new State("2025-12-01", 100L);
            State state2 = new State("2025-12-02", 200L);

            LivePoller.writeState(statePath, state1);
            LivePoller.writeState(statePath, state2);
            State readState = LivePoller.readState(statePath);

            assertNotNull(readState);
            assertEquals("2025-12-02", readState.getDayKey());
            assertEquals(200L, readState.getLastSeenBlock());
        }

        @Test
        @DisplayName("Should handle malformed JSON gracefully")
        void testReadMalformedJson() throws IOException {
            Files.writeString(statePath, "{ invalid json }", StandardCharsets.UTF_8);

            State state = LivePoller.readState(statePath);

            assertNull(state);
        }

        @Test
        @DisplayName("Should handle JSON without dayKey")
        void testReadJsonWithoutDayKey() throws IOException {
            String json = "{\"lastSeenBlock\": 12345}";
            Files.writeString(statePath, json, StandardCharsets.UTF_8);

            State state = LivePoller.readState(statePath);

            assertNull(state);
        }

        @Test
        @DisplayName("Should handle JSON without lastSeenBlock")
        void testReadJsonWithoutLastSeenBlock() throws IOException {
            String json = "{\"dayKey\": \"2025-12-01\"}";
            Files.writeString(statePath, json, StandardCharsets.UTF_8);

            State state = LivePoller.readState(statePath);

            assertNotNull(state);
            assertEquals("2025-12-01", state.getDayKey());
            assertEquals(-1L, state.getLastSeenBlock());
        }

        @Test
        @DisplayName("Should parse JSON with extra whitespace")
        void testReadJsonWithWhitespace() throws IOException {
            String json = "{\n  \"dayKey\"  :  \"2025-12-01\"  ,\n  \"lastSeenBlock\"  :  12345  \n}";
            Files.writeString(statePath, json, StandardCharsets.UTF_8);

            State state = LivePoller.readState(statePath);

            assertNotNull(state);
            assertEquals("2025-12-01", state.getDayKey());
            assertEquals(12345L, state.getLastSeenBlock());
        }

        @Test
        @DisplayName("Should handle state file with special characters in dayKey")
        void testDayKeyWithSpecialCharacters() {
            State state = new State("2025-12-01", 100L);

            LivePoller.writeState(statePath, state);
            State readState = LivePoller.readState(statePath);

            assertNotNull(readState);
            assertEquals("2025-12-01", readState.getDayKey());
        }
    }

    @Nested
    @DisplayName("State File Format Tests")
    class StateFileFormatTests {

        @TempDir
        Path tempDir;

        @Test
        @DisplayName("Should produce valid JSON format")
        void testValidJsonFormat() throws IOException {
            Path statePath = tempDir.resolve("state.json");
            State state = new State("2025-12-01", 12345L);

            LivePoller.writeState(statePath, state);
            String content = Files.readString(statePath, StandardCharsets.UTF_8);

            assertTrue(content.contains("\"dayKey\""), "Should contain dayKey field");
            assertTrue(content.contains("\"lastSeenBlock\""), "Should contain lastSeenBlock field");
            assertTrue(content.contains("2025-12-01"), "Should contain day value");
            assertTrue(content.contains("12345"), "Should contain block number");
        }

        @Test
        @DisplayName("Should use UTF-8 encoding")
        void testUtf8Encoding() throws IOException {
            Path statePath = tempDir.resolve("state.json");
            State state = new State("2025-12-01", 100L);

            LivePoller.writeState(statePath, state);

            byte[] bytes = Files.readAllBytes(statePath);
            String content = new String(bytes, StandardCharsets.UTF_8);
            assertTrue(content.contains("2025-12-01"));
        }
    }
}
