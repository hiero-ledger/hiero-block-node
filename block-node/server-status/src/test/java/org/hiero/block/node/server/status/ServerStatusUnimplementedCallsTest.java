// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.server.status;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.hedera.pbj.runtime.grpc.GrpcException;
import com.hedera.pbj.runtime.grpc.GrpcStatus;
import org.hiero.block.api.AddressBookRequest;
import org.hiero.block.api.TssDataRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests that the calls defined ahead of their handlers answer {@code UNIMPLEMENTED}, which is the status a client uses
 * to fall back to {@code serverStatusDetail} (any other exception would reach the client as {@code INTERNAL}).
 */
class ServerStatusUnimplementedCallsTest {
    private final ServerStatusServicePlugin plugin = new ServerStatusServicePlugin();

    @Test
    @DisplayName("getAddressBook answers UNIMPLEMENTED until its handler exists")
    void getAddressBookIsUnimplemented() {
        final GrpcException exception =
                assertThrows(GrpcException.class, () -> plugin.getAddressBook(AddressBookRequest.DEFAULT));

        assertEquals(GrpcStatus.UNIMPLEMENTED, exception.status());
    }

    @Test
    @DisplayName("getTssData answers UNIMPLEMENTED until its handler exists")
    void getTssDataIsUnimplemented() {
        final GrpcException exception =
                assertThrows(GrpcException.class, () -> plugin.getTssData(TssDataRequest.DEFAULT));

        assertEquals(GrpcStatus.UNIMPLEMENTED, exception.status());
    }
}
