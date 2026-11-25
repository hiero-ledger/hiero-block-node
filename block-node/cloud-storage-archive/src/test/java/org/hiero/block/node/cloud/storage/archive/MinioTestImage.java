// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.cloud.storage.archive;

/// Shared S3-compatible (MinIO fork) container image used by the archive tests.
final class MinioTestImage {
    /// Pinned `pgsty/silo` image, used in place of `minio/minio:latest`.
    static final String IMAGE =
            "docker.io/pgsty/silo@sha256:635197cb9f36d01bee221d34d1c7d7960f6a95c48b0b6c01d99cd13bdae51a46";

    private MinioTestImage() {}
}
