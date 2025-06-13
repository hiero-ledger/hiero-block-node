# Retention Policy Design Document

## Table of Contents

1. [Purpose](#purpose)
2. [Goals](#goals)
3. [Terms](#terms)
4. [Design](#design)
5. [Configuration](#configuration)
6. [Metrics](#metrics)
7. [Acceptance Tests](#acceptance-tests)

## Purpose

The Retention Policy is a threshold, measured in amount of blocks (block count),
that defines how many blocks the Block-Node should keep in its storage.
Effectively, this policy will allow us to achieve rolling history.

## Goals

There are a few things that the Retention Policy should achieve:
1. Each Block-Node will give access to the Retention Policy as it is defined as
a core concept of the Block-Node.
2. The Retention Policy should be configurable, allowing users to set the
desired block count threshold.
3. The Retention Policy is **NOT** a hard limit, meaning that the Block-Node
will keep accepting blocks even if the threshold is reached.
4. Each Persistence Plugin is responsible for respecting the Retention
Policy and removing blocks that exceed the threshold.
5. Each Persistence Plugin will choose its own level of granularity
for the Retention Policy, meaning that it can remove blocks in batches or
one by one, or for example if the blocks are stored in a zip that contains
'x' amount of blocks, it can delete the whole zip whenever the threshold
passes at least one 'x' above. This really depends on implementation.
6. The unit of the Retention Policy is block count and nothing else.
7. The Retention Policy is all about limiting storage space. This being said,
cleanup should be done when new data is received and not based on time. There
is no reason to clean data that is already there if no new data comes in.

## Terms

<dl>
  <dt>Retention Policy</dt>
  <dd>A configurable threshold, measured in amount of blocks (block count) that
      defines the maximum amount of blocks that can be stored (rolling history).
      The policy should not block new data incoming.</dd>
</dl>

## Design

The Retention Policy is a core concept of the Block-Node. It should be a very
simple mechanism that will simply allow Persistence Plugins to be able to make
decisions about what to clean up.

Simply, the Retention Policy will be a configuration parameter that will be
available to all Plugins in the Block-Node via the system-wide server
configuration. Persistence Plugins should respect this policy.

## Configuration

- the policy configurable parameter, amount of blocks to keep in storage
  - `-1` for default value which means no limit
  - available system-wide via the server configuration

## Metrics

It is advised that each Persistence Plugin should keep live metrics about the
current state of its storage, like the current amount of blocks in storage,
data in bytes stored, etc.

## Acceptance Tests

Acceptance tests for the Retention Policy should mean that whenever we configure
the Retention Policy to a certain value, the Block-Node should respect it and
keep the storage within the reasonable limit, but has no problem to keep
accepting new blocks even if the limit is reached, no matter how fast the new
data comes along.
