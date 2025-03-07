// SPDX-License-Identifier: Apache-2.0
plugins { id("org.hiero.gradle.build") version "0.3.6" }

rootProject.name = "hiero-block-node"

// Downgrade 'dependency-analysis-gradle-plugin' as 2.8.0 delivers unexpected results
// we need to investigate
buildscript {
    dependencies.constraints {
        classpath("com.autonomousapps:dependency-analysis-gradle-plugin:2.7.0!!")
    }
}

javaModules {
    directory(".") {
        group = "com.hedera.block"
        module("tools") // no 'module-info' yet
        module("suites")
        module("simulator")
        module("common")
        module("stream")
    }
    directory("block-node") {
        group = "com.hedera.block"
        module("server") { artifact = "block-node-server" }
        module("base") { artifact = "block-node-base" }
        module("persistence") { artifact = "service-persistence" }
        module("persistence-api") { artifact = "service-persistence-api" }
        module("verification") { artifact = "service-verification" }
        module("verification-api") { artifact = "service-verification-api" }
        // module("Messaging") { artifact = "facility-messaging" }
        // module("Configuration") { artifact = "facility-configuration" }
        // module("Registrar") { artifact = "facility-registrar" }
        // module("Facilities-API") { artifact = "facilities-api" }
    }
}
