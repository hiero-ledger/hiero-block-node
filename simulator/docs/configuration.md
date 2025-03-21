# Configuration

There are 3 configuration sets:
1. [BlockStreamConfig](#blockstreamconfig): contains the configuration for the Block Stream Simulator logic.
1. [GeneratorConfig](#generatorconfig): contains the configuration for the Block Stream Simulator generation module.
1. [GrpcConfig](#grpcconfig): contains the configuration for the gRPC communication with the Block-Node.

## BlockStreamConfig

Uses the prefix `blockStream` so all properties should start with `blockStream.`

| Key | Description | Default Value |
|:---|:---|---:|
| `simulatorMode` | The desired simulator mode to use, it can be either `PUBLISHER` or `CONSUMER`. | `PUBLISHER` |
| `delayBetweenBlockItems` | The delay between each block item in nanoseconds, only applicable when streamingMode=CONSTANT_RATE | `1_500_000` |
| `maxBlockItemsToStream` | exit condition for the simulator and the circular implementations such as `BlockAsDir` or `BlockAsFile` implementations | `10_000` |
| `streamingMode` | can either be `CONSTANT_RATE` or `MILLIS_PER_BLOCK` | `CONSTANT_RATE` |
| `millisecondsPerBlock` | if streamingMode is `MILLIS_PER_BLOCK` this will be the time to wait between blocks in milliseconds | `1_000` |
| `blockItemsBatchSize` | the number of block items to send in a single batch, however if a block has less block items, it will send all the items in a block | `1_000` |

## GeneratorConfig

Uses the prefix `generator` so all properties should start with `generator.`

| Key | Description | Default Value |
|:---|:---|---:|
| `generationMode` | The desired generation Mode to use, it can only be `DIR` or `CRAFT` | `DIR` |
| `folderRootPath` | If the generationMode is DIR this will be used as the source of the recording to stream to the Block-Node | `` |
| `managerImplementation` | The desired implementation of the BlockStreamManager to use, it can only be `BlockAsDirBlockStreamManager`, `BlockAsFileBlockStreamManager` or `BlockAsFileLargeDataSets` | `BlockAsFileBlockStreamManager` |
| `paddedLength` | on the `BlockAsFileLargeDataSets` implementation, the length of the padded left zeroes `000001.blk.gz` | 36 |
| `fileExtension` | on the `BlockAsFileLargeDataSets` implementation, the extension of the files to be streamed | `.blk.gz` |

## GrpcConfig

Uses the prefix `grpc` so all properties should start with `grpc.`

| Key | Description | Default Value |
|:---|:---|---:|
| `serverAddress` | The host of the Block-Node | `localhost` |
| `port` | The port of the Block-Node | `8080` |

## PrometheusConfig

Uses the prefix `prometheus` so all properties should start with `prometheus.`

| Key | Description | Default Value |
|:---|:---|--------------:|
| `endpointEnabled` | Whether Prometheus endpoint is enabled |       `false` |
| `endpointPortNumber` | Port number for Prometheus endpoint |        `9998` |