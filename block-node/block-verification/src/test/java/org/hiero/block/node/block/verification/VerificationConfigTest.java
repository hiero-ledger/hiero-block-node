// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.swirlds.config.api.validation.ConfigViolationException;
import org.hiero.block.node.app.fixtures.TestConfigurationBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/// Tests for the validation of the [VerificationConfig].
@DisplayName("Verification Config Tests")
class VerificationConfigTest {
    /// Tests for the size of the active sessions buffer.
    @Nested
    @DisplayName("Active Sessions Buffer Size")
    class ActiveSessionsBufferSize {
        /// This test aims to assert that the active sessions buffer size defaults to
        /// 101, a prime as recommended for the limit, when it is not configured.
        @Test
        @DisplayName("activeSessionsBufferSize defaults to 101")
        void testActiveSessionsBufferSizeDefaultsToOneHundredAndOne() {
            final VerificationConfig config = new TestConfigurationBuilder()
                    .withConfigDataType(VerificationConfig.class)
                    .getOrCreateConfig()
                    .getConfigData(VerificationConfig.class);
            assertThat(config.activeSessionsBufferSize()).isEqualTo(101);
        }

        /// This test aims to assert that the smallest valid active sessions buffer
        /// size, 13, is accepted.
        @Test
        @DisplayName("activeSessionsBufferSize accepts 13")
        void testAcceptsAnActiveSessionsBufferSizeOfThirteen() {
            final VerificationConfig config =
                    builderWithBufferSize("13").getOrCreateConfig().getConfigData(VerificationConfig.class);
            assertThat(config.activeSessionsBufferSize()).isEqualTo(13);
        }

        /// This test aims to assert that an active sessions buffer size below 13 is
        /// rejected when the configuration is built: the minimum keeps a full buffer
        /// large enough that the backup position of the eviction policy, at least the
        /// third session from the highest block down, has sessions on both sides of it.
        @ParameterizedTest
        @ValueSource(strings = {"12", "0", "-1"})
        @DisplayName("activeSessionsBufferSize below 13 is rejected")
        void testRejectsAnActiveSessionsBufferSizeBelowThirteen(final String bufferSize) {
            final TestConfigurationBuilder builder = builderWithBufferSize(bufferSize);
            assertThatExceptionOfType(ConfigViolationException.class).isThrownBy(builder::getOrCreateConfig);
        }
    }

    /// A configuration builder for the verification configuration with the given
    /// active sessions buffer size.
    private static TestConfigurationBuilder builderWithBufferSize(final String bufferSize) {
        return new TestConfigurationBuilder()
                .withConfigDataType(VerificationConfig.class)
                .withValue("verification.activeSessionsBufferSize", bufferSize);
    }
}
