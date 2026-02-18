// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.module;

import com.hedera.hapi.node.base.SemanticVersion;
import java.lang.module.ModuleDescriptor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.hiero.block.common.utils.StringUtilities;

/**
 * A class to facilitate constructing a SemanticVersion
 */
public final class SemanticVersionUtility {
    private static final Pattern SEMVER_PATTERN = Pattern.compile(
            "^(?<major>0|[1-9]\\d*)\\.(?<minor>0|[1-9]\\d*)\\.(?<patch>0|[1-9]\\d*)(-(?<preRelease>[0-9a-zA-Z]+)(\\+(?<build>[0-9a-zA-Z]+))?)?$");

    /**
     * Private constructor. Instantiation not necessary as only static methods should be defined here
     */
    private SemanticVersionUtility() {
        // This class does not need instantiation
    }

    /**
     * This method returns the {@link SemanticVersion} of a {@link Class}
     *
     * @param clazz a {@link Class} to look up its module version and convert to a SemanticVersion
     * @return {@link SemanticVersion} if the given {@link Class} has a valid version otherwise null
     */
    public static SemanticVersion from(final Class<?> clazz) {
        return from(clazz.getModule().getDescriptor());
    }

    /**
     * This method returns the {@link SemanticVersion} of a ModuleDescriptor
     *
     * @param moduleDescriptor a {@link ModuleDescriptor} to look up version and convert to a SemanticVersion
     * @return {@link SemanticVersion} if the given {@link Class} has a valid version otherwise null
     */
    public static SemanticVersion from(final ModuleDescriptor moduleDescriptor) {
        return from(moduleDescriptor.rawVersion().orElse(""));
    }

    /**
     * This method parses an input {@link String} tries to create a {@link SemanticVersion}.
     *
     * @param input a {@link String} to parse into a SemanticVersion
     * @return {@link SemanticVersion} if the given {@link String} is a valid SemanticVersion otherwise null
     */
    public static SemanticVersion from(final String input) {
        if (input == null || input.length() < 5) return null;

        final String versionString = (input.charAt(0) == 'v' || input.charAt(0) == 'V') ? input.substring(1) : input;
        final Matcher matcher = SEMVER_PATTERN.matcher(versionString);

        if (!matcher.matches()) return null;

        // Grab values from matcher groups
        final int major = Integer.parseInt(matcher.group("major"));
        final int minor = Integer.parseInt(matcher.group("minor"));
        final int patch = Integer.parseInt(matcher.group("patch"));
        final String preRelease = matcher.group("preRelease");
        final String build = matcher.group("build");

        // Build the SemanticVersion from the matcher values
        final SemanticVersion.Builder semanticVersionBuilder = new SemanticVersion.Builder();
        semanticVersionBuilder.major(major).minor(minor).patch(patch);
        if (!StringUtilities.isBlank(preRelease)) semanticVersionBuilder.pre(preRelease);
        if (!StringUtilities.isBlank(build)) semanticVersionBuilder.build(build);

        return semanticVersionBuilder.build();
    }
}
