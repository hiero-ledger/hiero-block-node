# Server Configuration

The components of the Hiero Block Node all support loading configuration via the
environment.

## Default Values

The default configuration allows users to quickly get up and running without having to configure anything. This provides
ease of use at the trade-off of some insecure default configuration. Most configuration settings have appropriate
defaults and can be left unchanged. It is recommended to browse the properties below and adjust to your needs.

| Environment Variable                       | Description                                                                                  | Default Value                         |
|:-------------------------------------------|:---------------------------------------------------------------------------------------------|:--------------------------------------|
| PERSISTENCE_STORAGE_LIVE_ROOT_PATH         | The root path for the live storage.                                                          | /opt/hashgraph/blocknode/data/live    |
| PERSISTENCE_STORAGE_ARCHIVE_ROOT_PATH      | The root path for the archive storage.                                                       | /opt/hashgraph/blocknode/data/archive |
| PERSISTENCE_STORAGE_TYPE                   | Type of the persistence storage                                                              | BLOCK_AS_LOCAL_FILE                   |
| PERSISTENCE_STORAGE_COMPRESSION            | Compression algorithm used during persistence (could be none as well)                        | ZSTD                                  |
| PERSISTENCE_STORAGE_COMPRESSION_LEVEL      | Compression level to be used by the compression algorithm                                    | 3                                     |
| PERSISTENCE_STORAGE_ARCHIVE_ENABLED        | Whether to enable archiving of blocks                                                        | true                                  |
| PERSISTENCE_STORAGE_ARCHIVE_GROUP_SIZE     | The size of the group of blocks to be archived at once                                       | 1_000                                 |
| PERSISTENCE_STORAGE_EXECUTOR_TYPE          | Type of executor for async writers (THREAD_POOL, SINGLE_THREAD, FORK_JOIN)                   | THREAD_POOL                           |
| PERSISTENCE_STORAGE_THREAD_COUNT           | Number of threads for thread pool executor (1-16)                                            | 6                                     |
| PERSISTENCE_STORAGE_THREAD_KEEP_ALIVE_TIME | Keep-alive time in seconds for idle threads in thread pool                                   | 60                                    |
| PERSISTENCE_STORAGE_USE_VIRTUAL_THREADS    | Whether to use virtual threads (Java 21 feature) instead of platform threads                 | false                                 |
| PERSISTENCE_STORAGE_EXECUTION_QUEUE_LIMIT  | Maximum queue size for pending tasks (64-2048)                                               | 1024                                  |
| CONSUMER_MAX_BLOCK_ITEM_BATCH_SIZE         | Maximum size of block item batches streamed to a client for closed-range historical requests | 1000                                  |
| CONSUMER_TIMEOUT_THRESHOLD_MILLIS          | Time to wait for subscribers before disconnecting in milliseconds                            | 1500                                  |
| SERVICE_DELAY_MILLIS                       | Service shutdown delay in milliseconds                                                       | 500                                   |
| MEDIATOR_RING_BUFFER_SIZE                  | Size of the ring buffer used by the mediator (must be a power of 2)                          | 67108864                              |
| NOTIFIER_RING_BUFFER_SIZE                  | Size of the ring buffer used by the notifier (must be a power of 2)                          | 2048                                  |
| SERVER_PORT                                | The port the server will listen on                                                           | 8080                                  |
| SERVER_MAX_MESSAGE_SIZE_BYTES              | The maximum size of a message frame in bytes                                                 | 1048576                               |
| VERIFICATION_ENABLED                       | Enables or disables the block verification process                                           | true                                  |
| VERIFICATION_SESSION_TYPE                  | The type of BlockVerificationSession to use, either `ASYNC` or `SYNC`                        | ASYNC                                 |
| VERIFICATION_HASH_COMBINE_BATCH_SIZE       | The number of hashes to combine into a single hash during verification                       | 32                                    |
