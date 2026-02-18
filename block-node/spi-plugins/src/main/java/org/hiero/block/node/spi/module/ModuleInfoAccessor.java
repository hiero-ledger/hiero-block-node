// SPDX-License-Identifier: Apache-2.0
//  SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.module;

import com.hedera.hapi.node.base.SemanticVersion;
import java.lang.module.ModuleDescriptor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The ModuleInfoAccessor provides access to the module-info for a class. ModuleInfoAccessor caches module-info so that
 * it can be accessed efficiently by modules with more than one class.
 */
public final class ModuleInfoAccessor {

    /**
     * Only process the module-infos once and cache them for later use.
     */
    private static final ConcurrentHashMap<String, ModuleInfoAccessor> moduleInfos = new ConcurrentHashMap<>();

    /**
     * The name of the module
     */
    private final String name;

    /**
     * The {@link SemanticVersion} of this module
     */
    private final SemanticVersion version;

    /**
     * A map of provided services from this module by class name
     */
    private final HashMap<String, Set<String>> providedMap = new HashMap<>();

    /**
     * Private constructor so that we only create on instance per module
     * @param module The module whose module-info we need to gather.
     */
    private ModuleInfoAccessor(final Module module) {
        final ModuleDescriptor moduleDescriptor = module.getDescriptor();
        name = module.getName();
        version = SemanticVersionUtility.from(moduleDescriptor);
        for (ModuleDescriptor.Provides provides : moduleDescriptor.provides()) {
            for (String provider : provides.providers()) {
                providedMap.computeIfAbsent(provider, (k -> new HashSet<>())).add(provides.service());
            }
        }
    }

    /**
     * Get the {@link ModuleInfoAccessor} associated with this class.
     * @param clazz The {@link Class} whose module-info we need to gather.
     * @return {@link ModuleInfoAccessor} associated with this class.
     */
    public static ModuleInfoAccessor getInstance(final Class<?> clazz) {
        return moduleInfos.computeIfAbsent(
                clazz.getModule().getName(), (k -> new ModuleInfoAccessor(clazz.getModule())));
    }

    /**
     * Get the module name associated with this {@link ModuleInfoAccessor}
     * @return the {@link String} name
     */
    public String name() {
        return name;
    }

    /**
     * Get the {@link SemanticVersion} associated with this {@link ModuleInfoAccessor}
     * @return the {@link SemanticVersion}
     */
    public SemanticVersion version() {
        return version;
    }

    /**
     * Get the {@link List<String>} of services provided by this {@link Class} associated with this {@link ModuleInfoAccessor}
     * @return the {@link List<String>} of services for this {@link Class}. An empty list will be provided if no services exist.
     */
    public List<String> provides(final Class<?> clazz) {
        return new ArrayList<>(providedMap.getOrDefault(clazz.getName(), new HashSet<>()));
    }
}
