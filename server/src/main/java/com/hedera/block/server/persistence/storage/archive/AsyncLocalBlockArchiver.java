// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.archive;

/**
 * An interface that defines an asynchronous local block archiver. Each archiver
 * is a runnable that archives blocks to local storage.
 */
public interface AsyncLocalBlockArchiver extends Runnable {}
