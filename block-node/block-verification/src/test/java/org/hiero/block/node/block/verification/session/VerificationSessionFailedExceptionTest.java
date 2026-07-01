// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.node.base.SemanticVersion;
import java.util.List;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.junit.jupiter.api.Test;

class VerificationSessionFailedExceptionTest {

    private static final long BLOCK_NUM = 42L;
    private static final SessionFailureType FAILURE_TYPE = SessionFailureType.BAD_BLOCK_PROOF;
    private static final BlockSource SOURCE = BlockSource.PUBLISHER;

    @Test
    void constructor3Args_setsFieldsAndDefaultMessage() {
        final VerificationSessionFailedException ex =
                new VerificationSessionFailedException(BLOCK_NUM, FAILURE_TYPE, SOURCE);

        assertThat(ex).isInstanceOf(RuntimeException.class);
        assertThat(ex.getBlockNumber()).isEqualTo(BLOCK_NUM);
        assertThat(ex.getFailureType()).isEqualTo(FAILURE_TYPE);
        assertThat(ex.getBlockSource()).isEqualTo(SOURCE);
        assertThat(ex.getBlockItems()).isNull();
        assertThat(ex.getHapiVersion()).isNull();
        assertThat(ex.getMessage()).contains(String.valueOf(BLOCK_NUM)).contains(FAILURE_TYPE.name());
    }

    @Test
    void constructor4Args_withBlockItems_setsBlockItems() {
        final List<org.hiero.block.internal.BlockItemUnparsed> items = List.of();
        final VerificationSessionFailedException ex =
                new VerificationSessionFailedException(BLOCK_NUM, FAILURE_TYPE, SOURCE, items);

        assertThat(ex.getBlockNumber()).isEqualTo(BLOCK_NUM);
        assertThat(ex.getFailureType()).isEqualTo(FAILURE_TYPE);
        assertThat(ex.getBlockSource()).isEqualTo(SOURCE);
        assertThat(ex.getBlockItems()).isSameAs(items);
        assertThat(ex.getHapiVersion()).isNull();
    }

    @Test
    void constructor5Args_withBlockItemsAndHapiVersion_setsAllFields() {
        final List<org.hiero.block.internal.BlockItemUnparsed> items = List.of();
        final SemanticVersion hapiVersion =
                SemanticVersion.newBuilder().major(0).minor(73).patch(0).build();
        final VerificationSessionFailedException ex =
                new VerificationSessionFailedException(BLOCK_NUM, FAILURE_TYPE, SOURCE, items, hapiVersion);

        assertThat(ex.getBlockNumber()).isEqualTo(BLOCK_NUM);
        assertThat(ex.getFailureType()).isEqualTo(FAILURE_TYPE);
        assertThat(ex.getBlockSource()).isEqualTo(SOURCE);
        assertThat(ex.getBlockItems()).isSameAs(items);
        assertThat(ex.getHapiVersion()).isEqualTo(hapiVersion);
    }

    @Test
    void constructor4Args_withCustomMessage_usesCustomMessage() {
        final String customMessage = "custom error message for block";
        final VerificationSessionFailedException ex =
                new VerificationSessionFailedException(BLOCK_NUM, FAILURE_TYPE, SOURCE, customMessage);

        assertThat(ex.getBlockNumber()).isEqualTo(BLOCK_NUM);
        assertThat(ex.getFailureType()).isEqualTo(FAILURE_TYPE);
        assertThat(ex.getBlockSource()).isEqualTo(SOURCE);
        assertThat(ex.getMessage()).isEqualTo(customMessage);
        assertThat(ex.getBlockItems()).isNull();
        assertThat(ex.getHapiVersion()).isNull();
        assertThat(ex.getCause()).isNull();
    }

    @Test
    void constructor4Args_withThrowable_setsCause() {
        final RuntimeException cause = new RuntimeException("root cause");
        final VerificationSessionFailedException ex =
                new VerificationSessionFailedException(BLOCK_NUM, FAILURE_TYPE, SOURCE, cause);

        assertThat(ex.getBlockNumber()).isEqualTo(BLOCK_NUM);
        assertThat(ex.getFailureType()).isEqualTo(FAILURE_TYPE);
        assertThat(ex.getBlockSource()).isEqualTo(SOURCE);
        assertThat(ex.getCause()).isSameAs(cause);
        assertThat(ex.getBlockItems()).isNull();
        assertThat(ex.getHapiVersion()).isNull();
    }

    @Test
    void constructor5Args_withMessageAndThrowable_setsBoth() {
        final String customMessage = "detailed error";
        final IllegalStateException cause = new IllegalStateException("state error");
        final VerificationSessionFailedException ex =
                new VerificationSessionFailedException(BLOCK_NUM, FAILURE_TYPE, SOURCE, customMessage, cause);

        assertThat(ex.getBlockNumber()).isEqualTo(BLOCK_NUM);
        assertThat(ex.getFailureType()).isEqualTo(FAILURE_TYPE);
        assertThat(ex.getBlockSource()).isEqualTo(SOURCE);
        assertThat(ex.getMessage()).isEqualTo(customMessage);
        assertThat(ex.getCause()).isSameAs(cause);
        assertThat(ex.getBlockItems()).isNull();
        assertThat(ex.getHapiVersion()).isNull();
    }

    @Test
    void allSourceValues_areSupported() {
        for (final BlockSource source : BlockSource.values()) {
            final VerificationSessionFailedException ex =
                    new VerificationSessionFailedException(0L, SessionFailureType.UNKNOWN_ERROR, source);
            assertThat(ex.getBlockSource()).isEqualTo(source);
        }
    }

    @Test
    void allFailureTypes_areSupported() {
        for (final SessionFailureType type : SessionFailureType.values()) {
            final VerificationSessionFailedException ex = new VerificationSessionFailedException(0L, type, SOURCE);
            assertThat(ex.getFailureType()).isEqualTo(type);
        }
    }
}
