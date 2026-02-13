// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.utils;

import com.hedera.hapi.node.base.SemanticVersion;
import java.io.InputStream;
import java.net.URL;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A class to facilitate constructing a SemanticVersion
 */
public final class SemanticVersionUtilities {
    private static final Pattern SEMVER_PATTERN = Pattern.compile(
            "^(?<major>0|[1-9]\\d*)\\.(?<minor>0|[1-9]\\d*)\\.(?<patch>0|[1-9]\\d*)(?:-(?<preRelease>(?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\\.(?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\\+(?<build>[0-9a-zA-Z-]+(?:\\.[0-9a-zA-Z-]+)*))?$");

    /**
     * Private constructor. Instantiation not necessary as only static methods should be defined here
     */
    private SemanticVersionUtilities() {
        // This class does not need instantiation
    }

    /**
     * This method parses an input {@link String} tries to create a {@link SemanticVersion}.
     *
     * @param clazz a {@link Class} to look up its version and convert to a SemanticVersion
     * @return {@link SemanticVersion} if the given {@link Class} has a valid version otherwise null
     */
    public static SemanticVersion from(final Class clazz) {
        String version = null;

        final Package appPackage = clazz.getPackage();
        if (appPackage != null) {
            version = appPackage.getImplementationVersion();
        }

        // More robust approach by manually reading the manifest
        if (version == null) {
            String className = clazz.getSimpleName() + ".class";
            String classPath = clazz.getResource(className).toString();
            if (classPath.startsWith("jar")) {
                // Extract the manifest URL
                String manifestPath = classPath.substring(0, classPath.lastIndexOf("!") + 1) + "/META-INF/MANIFEST.MF";
                try (InputStream is = new URL(manifestPath).openStream()) {
                    Manifest manifest = new Manifest(is);
                    Attributes mainAttribs = manifest.getMainAttributes();
                    version = mainAttribs.getValue("Implementation-Version");
                } catch (Exception e) {
                    System.err.println("Could not read manifest for version lookup: " + e.getMessage());
                }
            }
        }

        return from(version);
    }

    /**
     * This method parses an input {@link String} tries to create a {@link SemanticVersion}.
     *
     * @param input a {@link String} to parse into a SemanticVersion
     * @return {@link SemanticVersion} if the given {@link String} is a valid SemanticVersion otherwise null
     */
    public static SemanticVersion from(String input) {
        if (input == null || input.length() < 5) return null;

        if (input.charAt(0) == 'v' || input.charAt(0) == 'V') {
            input = input.substring(1);
        }

        final Matcher matcher = SEMVER_PATTERN.matcher(input);

        if (!matcher.matches()) return null;

        // Grab values from matcher groups
        int major = Integer.parseInt(matcher.group("major"));
        int minor = Integer.parseInt(matcher.group("minor"));
        int patch = Integer.parseInt(matcher.group("patch"));
        String preRelease = matcher.group("preRelease");
        String build = matcher.group("build");

        // Build the SemanticVersion from the matcher values
        SemanticVersion.Builder semanticVersionBuilder = new SemanticVersion.Builder();
        semanticVersionBuilder.major(major).minor(minor).patch(patch);
        if (!StringUtilities.isBlank(preRelease)) semanticVersionBuilder.pre(preRelease);
        if (!StringUtilities.isBlank(build)) semanticVersionBuilder.build(build);

        return semanticVersionBuilder.build();
    }
}
