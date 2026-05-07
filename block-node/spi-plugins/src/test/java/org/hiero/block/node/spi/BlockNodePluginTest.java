// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.pbj.runtime.grpc.GrpcException;
import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.webserver.http.HttpService;
import java.util.List;
import org.hiero.block.api.TssData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Test class for {@link BlockNodePlugin} interface and its default methods.
 */
public class BlockNodePluginTest {
    private static final ServiceInterface testServiceInterface = new ServiceInterface() {
        @NonNull
        @Override
        public String serviceName() {
            return "";
        }

        @NonNull
        @Override
        public String fullName() {
            return "";
        }

        @NonNull
        @Override
        public List<Method> methods() {
            return List.of();
        }

        @NonNull
        @Override
        public Pipeline<? super Bytes> open(
                @NonNull Method method, @NonNull RequestOptions opts, @NonNull Pipeline<? super Bytes> responses)
                throws GrpcException {
            return responses;
        }
    };
    private static final HttpService testHttpService = rules -> {};

    private static class TestApplicationStateFacilityDefault implements ApplicationStateFacility {
        @Override
        public void updateTssData(TssData tssData) {
            // do nothing
        }

        @Override
        public void updateAddressBook(NodeAddressBook nodeAddressBook) {
            // do nothing
        }
    }

    @Test
    @DisplayName("Test default ApplicationStateFacility.updateTssData()")
    void testDefaultUpdateTssData() {
        TestApplicationStateFacilityDefault testApplicationStateFacilityDefault =
                new TestApplicationStateFacilityDefault();
        try {
            testApplicationStateFacilityDefault.updateTssData(null);
        } catch (Exception e) {
            fail(e);
        }
    }

    private static class TestApplicationStateFacility implements ApplicationStateFacility {
        TssData tssData = null;

        @Override
        public void updateTssData(TssData tssData) {
            this.tssData = tssData;
        }

        @Override
        public void updateAddressBook(NodeAddressBook nodeAddressBook) {
            // do nothing
        }
    }

    @Test
    @DisplayName("Test ApplicationStateFacility.updateTssData()")
    void testUpdateTssData() {
        TestApplicationStateFacility testApplicationStateFacility = new TestApplicationStateFacility();
        testApplicationStateFacility.updateTssData(null);
        assertEquals(null, testApplicationStateFacility.tssData);
    }

    private static class TestBlockNodePlugin implements BlockNodePlugin {
        BlockNodeContext context;

        @Override
        public void init(@NonNull BlockNodeContext context, @NonNull ServiceBuilder serviceBuilder) {
            this.context = context;
            serviceBuilder.registerGrpcService(testServiceInterface);
            serviceBuilder.registerHttpService("foo", testHttpService);
        }
    }

    @Test
    @DisplayName("Test default name method")
    void testDefaultName() {
        BlockNodePlugin plugin = new TestBlockNodePlugin();
        assertEquals("TestBlockNodePlugin", plugin.name());
    }

    @Test
    @DisplayName("Test default configDataTypes method")
    void testDefaultConfigDataTypes() {
        BlockNodePlugin plugin = new TestBlockNodePlugin();
        List<Class<? extends Record>> configDataTypes = plugin.configDataTypes();
        assertNotNull(configDataTypes);
        assertEquals(0, configDataTypes.size());
    }

    @Test
    @DisplayName("Test default init method")
    void testDefaultInit() {
        BlockNodePlugin plugin = new TestBlockNodePlugin();
        plugin.init(null, new ServiceBuilder() {

            @Override
            public void registerHttpService(String path, HttpService... service) {
                assertEquals("foo", path);
                assertEquals(1, service.length);
                assertEquals(testHttpService, service[0]);
            }

            @Override
            public void registerGrpcService(@NonNull ServiceInterface service) {
                assertEquals(testServiceInterface, service);
            }
        });
    }

    @Test
    @DisplayName("Test default start method")
    void testDefaultStart() {
        BlockNodePlugin plugin = new TestBlockNodePlugin();
        plugin.start();
        // No exception means the default implementation is a no-op
    }

    @Test
    @DisplayName("Test default stop method")
    void testDefaultStop() {
        BlockNodePlugin plugin = new TestBlockNodePlugin();
        plugin.stop();
        // No exception means the default implementation is a no-op
    }

    @Test
    @DisplayName("Test default onContextUpdate method")
    void testDefaultOnContextUpdate() {
        TestBlockNodePlugin plugin = new TestBlockNodePlugin();
        plugin.init(null, new ServiceBuilder() {

            @Override
            public void registerHttpService(String path, HttpService... service) {
                assertEquals("foo", path);
                assertEquals(1, service.length);
                assertEquals(testHttpService, service[0]);
            }

            @Override
            public void registerGrpcService(@NonNull ServiceInterface service) {
                assertEquals(testServiceInterface, service);
            }
        });

        BlockNodeContext context =
                new BlockNodeContext(null, null, null, null, null, null, null, null, null, null, null);

        plugin.onContextUpdate(context);

        assertNull(plugin.context);
    }
}
