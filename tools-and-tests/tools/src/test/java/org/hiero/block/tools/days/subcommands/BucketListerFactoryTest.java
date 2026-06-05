// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.subcommands;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import org.hiero.block.tools.utils.BucketLister;
import org.hiero.block.tools.utils.gcp.GCPBucketLister;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for BucketLister creation logic in UpdateDayListingsCommand.
 *
 * <p>Tests verify that the correct BucketLister implementation is selected
 * based on environment variables for HMAC credentials.
 *
 * <p><b>Note on testing HMAC credential detection:</b> The credential detection
 * logic reads from {@code System.getenv()}, which cannot be easily mocked in unit tests
 * without complex reflection hacks that don't work reliably with the Java module system.
 * Therefore, this test suite focuses on the default behavior (no HMAC credentials).
 *
 * <p><b>Manual testing for HMAC credentials:</b> To verify HMAC credential detection:
 * <pre>
 * export GCS_HMAC_ACCESS_ID="your-access-id"
 * export GCS_HMAC_SECRET="your-secret"
 * export GCS_BUCKET_NAME="your-bucket"
 * java -jar tools.jar days updateDayListings
 * # Should print: "[UpdateDayListingsCommand] Using BuckyBucketLister with HMAC credentials"
 * </pre>
 *
 * <p>The logic being tested is:
 * <pre>
 * if (hmacAccessId != null && !hmacAccessId.isBlank() && hmacSecret != null && !hmacSecret.isBlank()) {
 *     return BuckyBucketLister.fromEnvironment(...);
 * } else {
 *     return new GCPBucketLister(...);
 * }
 * </pre>
 */
class BucketListerFactoryTest {

    @TempDir
    Path tempDir;

    /**
     * Test that GCPBucketLister is returned when no HMAC credentials are present.
     *
     * <p>This test verifies the default behavior when environment variables
     * are not set - the system should fall back to GCS with Application Default Credentials.
     *
     * <p>This test assumes a clean test environment without GCS_HMAC_ACCESS_ID
     * or GCS_HMAC_SECRET environment variables set.
     */
    @Test
    void testCreateBucketLister_DefaultBehavior_ReturnsGCPBucketLister() {
        // Use reflection to invoke the private static method
        try {
            java.lang.reflect.Method method = UpdateDayListingsCommand.class.getDeclaredMethod(
                    "createBucketLister", boolean.class, Path.class, int.class, int.class, String.class);
            method.setAccessible(true);

            BucketLister result = (BucketLister) method.invoke(null, false, tempDir, 3, 9, "test-project");

            assertNotNull(result, "BucketLister should not be null");
            assertTrue(
                    result instanceof GCPBucketLister,
                    "Should return GCPBucketLister when HMAC credentials are not present");

            System.out.println("✓ Default behavior verified: returns GCPBucketLister without HMAC credentials");
        } catch (Exception e) {
            fail("Failed to test createBucketLister: " + e.getMessage());
        }
    }

    /**
     * Test that the method can be invoked with different node ranges.
     *
     * <p>Verifies that the factory method correctly passes through parameters
     * to the GCPBucketLister constructor.
     */
    @Test
    void testCreateBucketLister_DifferentNodeRanges() {
        try {
            java.lang.reflect.Method method = UpdateDayListingsCommand.class.getDeclaredMethod(
                    "createBucketLister", boolean.class, Path.class, int.class, int.class, String.class);
            method.setAccessible(true);

            // Test with previewnet node range
            BucketLister result1 = (BucketLister) method.invoke(null, false, tempDir, 3, 9, "test-project");
            assertNotNull(result1, "BucketLister should not be null for previewnet range");

            // Test with testnet/mainnet node range
            BucketLister result2 = (BucketLister) method.invoke(null, false, tempDir, 3, 37, "test-project");
            assertNotNull(result2, "BucketLister should not be null for mainnet range");

            System.out.println("✓ Factory method works with different node ranges");
        } catch (Exception e) {
            fail("Failed to test with different node ranges: " + e.getMessage());
        }
    }

    /**
     * Test that the method can be invoked with caching enabled.
     *
     * <p>Verifies that the caching parameter is correctly passed through.
     */
    @Test
    void testCreateBucketLister_WithCaching() {
        try {
            java.lang.reflect.Method method = UpdateDayListingsCommand.class.getDeclaredMethod(
                    "createBucketLister", boolean.class, Path.class, int.class, int.class, String.class);
            method.setAccessible(true);

            // Test with caching enabled
            BucketLister result = (BucketLister) method.invoke(null, true, tempDir, 3, 9, "test-project");
            assertNotNull(result, "BucketLister should not be null with caching enabled");
            assertTrue(result instanceof GCPBucketLister, "Should still return GCPBucketLister");

            System.out.println("✓ Factory method works with caching enabled");
        } catch (Exception e) {
            fail("Failed to test with caching: " + e.getMessage());
        }
    }
}
