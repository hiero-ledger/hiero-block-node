import { Client, StatusOK, Stream } from 'k6/net/grpc';
import { check, fail } from 'k6';
import { SharedArray } from 'k6/data';
import {ServerStatusRequest, SubscribeBlockStreamRequest} from "../lib/grpc.js";
import {Trend} from "k6/metrics";
import { instance } from 'k6/execution';

// setup options
export const options = {
    scenarios: {
        default: {
            executor: 'shared-iterations',
            vus: 1,
            iterations: 1,
            maxDuration: '10s', // Set this slightly higher than your gRPC timeout
            gracefulStop: `20s`
        },
    },
    thresholds: { // todo make these good defaults, we need these to display tags in the result, but also to ping us if they go over
        'grpc_req_duration{name:block_node_server_status}': ['p(95)<300'],
        'grpc_req_duration{name:subscribe_block_stream}': ['p(95)<3000'],
        'checks': [{ threshold: 'rate == 1.0', abortOnFail: true }], // checks defined in this test must fail the test
    },
};

// load test configuration data
const data = new SharedArray('BN Test Configs', function () {
    return JSON.parse(open('./../data.json')).configs;
})[0];

// initialize gRPC client
const client = new Client();
client.load([data.protobufPath],
    'block-node/api/node_service.proto',
    'block-node/api/block_access_service.proto',
    'block-node/api/block_stream_subscribe_service.proto');

// this trend measures the reception of a full block along with its endOfBlock message
const trend = new Trend('full_block_stream_duration', true);

// run test
export default async () => {
    client.connect(data.blockNodeUrl, {
        plaintext: true
    });
    const serverStatusParams = {
        tags: {name: 'block_node_server_status'}
    }
    const response = new ServerStatusRequest(client).invoke(serverStatusParams);
    const firstAvailableBlock = BigInt(response.message.firstAvailableBlock);
    const lastAvailableBlock = BigInt(response.message.lastAvailableBlock);
    console.log(`First Available Block: ${firstAvailableBlock}, Latest Block: ${lastAvailableBlock}`);
    // decide how many blocks to stream based on availability
    let blockDelta = 0n;
    if (response.message.firstAvailableBlock === '18446744073709551615') {
        client.close();
        fail(`No blocks to stream, exiting test fail.`);
    }
    else if (firstAvailableBlock === lastAvailableBlock) {
        console.log(`Block Node only has one block.`);
    } else if ((lastAvailableBlock - firstAvailableBlock) < data.smokeTestConfigs.numOfBlocksToStream) {
        blockDelta = lastAvailableBlock - firstAvailableBlock;
        console.log(`Block Node has only ${blockDelta + 1n} blocks to stream.`);
    } else {
        blockDelta = BigInt(data.smokeTestConfigs.numOfBlocksToStream);
        console.log(`Block Node has sufficient blocks to stream ${data.smokeTestConfigs.numOfBlocksToStream} blocks.`);
    }
    // stream block from subscribe API
    const subscribeParams = {
        tags: {name: 'subscribe_block_stream'}
    }
    const timingMap = new Map();
    const stream = new SubscribeBlockStreamRequest(client).invoke(subscribeParams);
    let streamEndedSuccessfully = false;
    let successStatusReceived = false;
    // count number of end of blocks received
    const streamEnded = new Promise((resolve, reject) => {
        stream.on('data', (subscribeStreamResponse) => {
            if (subscribeStreamResponse.blockItems) {
                const receivedAt = instance.currentTestRunDuration;
                const header = subscribeStreamResponse.blockItems.blockItems[0].blockHeader;
                if (header) {
                    // we want to put in the map only if a header is present because a block
                    // could be sent in multiple chunks, we only care about the block received in full, starting from
                    // the first chunk (must include header) to the endOfBlock message
                    const bn = header.number;
                    timingMap.set(parseInt(bn), receivedAt);
                }
            } else if (subscribeStreamResponse.endOfBlock) {
                const receivedAt = instance.currentTestRunDuration;
                const bn = subscribeStreamResponse.endOfBlock.blockNumber;
                const startedAt = timingMap.get(parseInt(bn));
                trend.add(receivedAt - startedAt); // This should be accurate for more that millis (micros, nanos)
                timingMap.delete(parseInt(bn));
            } else if (subscribeStreamResponse.status) {
                // success status is something expected, this is not something this
                // test should measure, as we want averages for individual blocks streamed
                if (subscribeStreamResponse.status !== `SUCCESS`) {
                    console.log(`Received Non-Success Status: ${subscribeStreamResponse.status}`);
                } else {
                    successStatusReceived = true;
                }
            }
            else {
                console.log(`Unknown Stream Response: , ${JSON.stringify(subscribeStreamResponse)}`);
            }
        });
        stream.on('error', (err) => {
            console.log('Stream Error: ' + JSON.stringify(err));
            reject(err);
        });
        stream.on('end', () => {
            console.log('Stream Ended');
            streamEndedSuccessfully = true;
            resolve();
        });
    });
    stream.write({
        "start_block_number": firstAvailableBlock.toString(),
        "end_block_number": (firstAvailableBlock + blockDelta).toString(),
    });
    stream.end();
    try {
        // Wait for the stream to finish
        await streamEnded;
    } catch (err) {
        console.error(`Promise rejected with error: ${JSON.stringify(err)}`);
    } finally {
        // This k6 'check' will show up in your final summary table
        check(streamEndedSuccessfully, {
            'gRPC stream finished successfully': (s) => s === true,
        });
        check(successStatusReceived, {
            'gRPC stream received SUCCESS status': (s) => s === true,
        });
        client.close();
    }
};
