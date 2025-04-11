// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi;

import java.util.ServiceLoader;
import java.util.stream.Stream;

/**
 * The ServiceLoaderFunction class is used to load services using the Java ServiceLoader. Or alternatively a custom
 * implementation of when testing.
 */
public class ServiceLoaderFunction {
    /**
     * Load services of the given class. This method will use the Java ServiceLoader to load the services. Unless
     * overridden by a custom implementation.
     *
     * @param serviceClass the class of the service to load
     * @param <C>          the type of the service
     * @return a stream of services of the given class
     */
    public <C> Stream<? extends C> loadServices(Class<C> serviceClass) {
        return ServiceLoader.load(serviceClass).stream().map(ServiceLoader.Provider::get);
    }
}
