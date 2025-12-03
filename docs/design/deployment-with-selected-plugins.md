# Deployment with Selected Plugins

## Table of Contents

1. [Purpose](#purpose)
2. [Goals](#goals)
3. [Design](#design)

## Purpose

We need the ability to deliver images customized to archive-only nodes,
partial history nodes, and full history nodes. We also need to support easily
adding or removing plugins as part of a deployment process, including adding
future community or user developed plugins.

## Goals

1. Build with all Hiero-defined plugins in the block node repo for E2E testing.
2. Build with only the minimum required plugins for use by Helm chart deployments
   that add plugins dynamically based on chart configuration.
3. Modify the build to publish all current plugin modules to Maven Central
   as individual libraries so that they can be downloaded individually during Helm chart deployments.

## Design

Create a plugin-free OCI image with a well-defined extension folder
where plugin jars can be added during deployment.
Define helm chart values files for the four current deployments
(archiver, partial history, full history, kitchen-sink/testing)

We cannot reasonably create OCI images for every possible combination of plugins,
and we want to support third-party plugins or private plugins.
To do that we will add a volume extension point in the OCI image(s)
where an operator can place additional plugins to be loaded.
The cleanest way to support that at deployment would be to enable the Helm chart
to add arbitrary jars to that extension location based on value files, so an operator
just lists the required jars in their values file and those plugins get loaded into their deployment.
