// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.archive;

/// The outcome of a completed upload task.
enum UploadResult {
    /// All blocks were successfully archived and persisted notifications were sent.
    SUCCESS,
    /// A part upload failed; false persisted notifications were sent for the affected blocks.
    FAILED
}
