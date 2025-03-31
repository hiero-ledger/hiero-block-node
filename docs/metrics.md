# Metrics

This document describes the metrics that are available in the system, its purpose, and how to use them.

## Table of Contents

1. [Overview](#overview)
2. [Purpose](#purpose)
3. [Configuration](#configuration)
   1. [Prometheus](#prometheus)
4. [Usage](#usage)
   1. [Local Development](#local-development)
5. [Existing Metrics](#existing-metrics)

## Overview

We are using Prometheus to collect metrics from the system, through the use of [Hiero Platform SDK - Metrics](https://github.com/hiero-ledger/hiero-consensus-node/blob/main/platform-sdk/docs/base/metrics/metrics.md).

## Purpose

The purpose of metrics is to provide a way to measure the performance of the system. Metrics are used to monitor the system and to detect any issues that may arise. Metrics can be used to identify bottlenecks, track the performance of the system over time, and to help diagnose problems.

## Configuration

### Prometheus

Prefix: prometheus, ie. `prometheus.configKey`

| ConfigKey                 | Description                                                                           | Default |
|:--------------------------|:--------------------------------------------------------------------------------------|--------:|
| enableEndpoint            | either `true` or `false`. Enables or disables the endpoint for metrics                |    true |
| endpointPortNumber        | Port of the Prometheus endpoint                                                       |    9999 |
| endpointMaxBacklogAllowed | The maximum number of incoming TCP connections which the system will queue internally |       1 |

## Usage

All classes that need to observe metrics can get them from the BlockNodeContext.

If a new metric is needed, it can be added to MetricsService, and then used in the classes that need it. This is to avoid having to define the metrics in several places.
MetricsService centralizes the creation of metrics and provides a way to access them from any other class.

To check the metrics you can access the Prometheus endpoint at `http://localhost:9999/metrics`.

### Local Development

For developers, when using the gradle task `startDockerContainer` it will automatically start a prometheus and grafana services preconfigured locally with credentials: username `admin` and password  `admin` and the dashboard already provisioned with the current metrics and widgets.

Dashboard is called `Block-Node Server Dashboard` and its source is kept on folder: `server/docker/metrics/dashboards` as: `block-node-server.json`.

When doing changes to the dashboard on grafana is important to copy the json to clipboard and commit the changes on the above file, so dashboard is updated with the latest widgets for the new metrics added.

If needed to create another dashboard is possible to include it by adding it to the same folder.

## Existing Metrics

All metrics have `hedera_block_node` prefix.

| Metric Name             | Description                           |    Type |
|:------------------------|:--------------------------------------|--------:|
| live_block_items        | The number of block items received    | Counter |
| blocks_persisted        | the number of blocks persisted        | Counter |
| subscribers             | The number of subscribers             |   Gauge |
| single_blocks_retrieved | the number of single blocks requested | Counter |
