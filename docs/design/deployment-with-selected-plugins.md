# Deployment with Selected Plugins

## Table of Contents

1. [Purpose](#purpose)
2. [Goals](#goals)
3. [Design](#design)
4. [Acceptance Tests](#acceptance-tests)

## Purpose

We need the ability to deliver 2 images - one with all the plugins in the block node
and one with minimum required plugins. We also need to support easily
adding or removing plugins as part of a deployment process, including adding
future community or user developed plugins.

## Goals

1. Build with all Hiero-defined plugins in the block node repo for E2E testing.
2. Build with only the minimum required plugins for use by Helm chart deployments
   that add plugins dynamically based on chart configuration.
3. TBD(2 options for now)

- Modify the build to publish all current plugin modules to Maven Central
  as individual libraries so that they can be downloaded individually during Helm chart deployments. (This is not small task
  since currently all plugins are built and published as one docker image)

- Modify the build to publish 2 OCI images - one with all plugins and one with only the minimum required plugins.
  There will be a mechanism for copying plugins from the full image to the minimal image based on plugin selection(This is easier but not optimal)

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

## Acceptance Tests

**Verify e2e tests are passing after the changes**
1. Build and publish the full OCI image with all plugins.
2. Run the existing e2e tests using the full image.
3. Verify that all e2e tests pass successfully.

**Verify that the minimal image can be deployed with only the required plugins**
1. Build and publish the minimal OCI image with only required plugins.
2. Deploy the minimal image using the Helm chart with no additional plugins.
3. Verify that the deployment is successful and the application starts correctly.

**Verify that additional plugins can be added to the minimal image during deployment**
1. Prepare a set of additional plugin jars to be added.
2. Deploy the minimal image using the Helm chart, specifying the additional plugins in the values file.
3. Verify that the deployment is successful and the application starts correctly with the additional plugins loaded.
