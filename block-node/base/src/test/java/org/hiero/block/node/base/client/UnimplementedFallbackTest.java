// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.pbj.runtime.grpc.GrpcException;
import com.hedera.pbj.runtime.grpc.GrpcStatus;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("UnimplementedFallback Tests")
class UnimplementedFallbackTest {

    @Test
    @DisplayName("Returns the result of the call and never runs the fallback on success")
    void successSkipsFallback() {
        final AtomicInteger fallbackCalls = new AtomicInteger();

        final String result = UnimplementedFallback.callOrFallback(() -> "new", () -> {
            fallbackCalls.incrementAndGet();
            return "old";
        });

        assertEquals("new", result);
        assertEquals(0, fallbackCalls.get());
    }

    @Test
    @DisplayName("Runs the fallback when the call fails with UNIMPLEMENTED")
    void unimplementedRunsFallback() {
        final String result = UnimplementedFallback.callOrFallback(
                () -> {
                    throw new GrpcException(GrpcStatus.UNIMPLEMENTED, "no such method");
                },
                () -> "old");

        assertEquals("old", result);
    }

    @Test
    @DisplayName("Runs the fallback when UNIMPLEMENTED is wrapped as the cause of another failure")
    void wrappedUnimplementedRunsFallback() {
        final String result = UnimplementedFallback.callOrFallback(
                () -> {
                    throw new RuntimeException("call failed", new GrpcException(GrpcStatus.UNIMPLEMENTED, "no"));
                },
                () -> "old");

        assertEquals("old", result);
    }

    @Test
    @DisplayName("Rethrows any other gRPC status without running the fallback")
    void otherStatusIsRethrown() {
        final AtomicInteger fallbackCalls = new AtomicInteger();
        final GrpcException failure = new GrpcException(GrpcStatus.UNAVAILABLE, "down");

        final GrpcException thrown = assertThrows(
                GrpcException.class,
                () -> UnimplementedFallback.callOrFallback(
                        () -> {
                            throw failure;
                        },
                        () -> {
                            fallbackCalls.incrementAndGet();
                            return "old";
                        }));

        assertSame(failure, thrown);
        assertEquals(0, fallbackCalls.get());
    }

    @Test
    @DisplayName("A failure of the fallback itself reaches the caller")
    void fallbackFailureIsRethrown() {
        final RuntimeException fallbackFailure = new RuntimeException("old call failed too");

        final RuntimeException thrown = assertThrows(
                RuntimeException.class,
                () -> UnimplementedFallback.callOrFallback(
                        () -> {
                            throw new GrpcException(GrpcStatus.UNIMPLEMENTED, "no");
                        },
                        () -> {
                            throw fallbackFailure;
                        }));

        assertSame(fallbackFailure, thrown);
    }

    @Test
    @DisplayName("isUnimplemented recognises the status in the cause chain and nothing else")
    void isUnimplemented() {
        assertTrue(UnimplementedFallback.isUnimplemented(new GrpcException(GrpcStatus.UNIMPLEMENTED, "x")));
        assertTrue(UnimplementedFallback.isUnimplemented(
                new IllegalStateException("outer", new GrpcException(GrpcStatus.UNIMPLEMENTED, "x"))));
        assertFalse(UnimplementedFallback.isUnimplemented(new GrpcException(GrpcStatus.INTERNAL, "x")));
        assertFalse(UnimplementedFallback.isUnimplemented(new RuntimeException("plain")));
        assertFalse(UnimplementedFallback.isUnimplemented(null));
    }
}
