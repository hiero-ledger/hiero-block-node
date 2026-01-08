import { Client, StatusOK, Stream } from 'k6/net/grpc';
import { check, sleep } from 'k6';
import { SharedArray } from 'k6/data';
import {ServerStatusRequest, SubscribeBlockStreamRequest} from "../lib/grpc.js";
import {Trend} from "k6/metrics";
import { instance } from 'k6/execution';

// setup options
export const options = {
    thresholds: { // todo make these good defaults, we need these to display tags in the result, but also to ping us if they go over
        'grpc_req_duration{name:block_node_server_status}': ['p(95)<300'],
        'grpc_req_duration{name:subscribe_block_stream}': ['p(95)<2000'],
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
export default () => {
    client.connect(data.blockNodeUrl, {
        plaintext: true
    });
    const serverStatusParams = {
        tags: {name: 'block_node_server_status'}
    }
    const response = new ServerStatusRequest(client).invoke(serverStatusParams);
    check(response, {
        'status is OK': (r) => r && r.status === StatusOK,
    });
    const firstAvailableBlock = BigInt(response.message.firstAvailableBlock);
    const lastAvailableBlock = BigInt(response.message.lastAvailableBlock);
    console.log(`First Available Block: ${firstAvailableBlock}, Latest Block: ${lastAvailableBlock}`);
    console.log(JSON.stringify(response.message));
    // decide how many blocks to stream based on availability
    let blockDelta = 0n;
    if (response.message.firstAvailableBlock === '18446744073709551615') {
        console.log(`No blocks to stream, exiting test.`);
        client.close();
        return;
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
                console.log(`Stream Response: BlockHeader for Block ${JSON.stringify(bn)}`);
            }
        } else if (subscribeStreamResponse.endOfBlock) {
            const receivedAt = instance.currentTestRunDuration;
            const bn = subscribeStreamResponse.endOfBlock.blockNumber;
            const startedAt = timingMap.get(parseInt(bn));
            trend.add(receivedAt - startedAt); // This should be accurate for more that millis (micros, nanos)
            timingMap.delete(parseInt(bn));
            console.log(`Stream Response: endOfBlock for Block ${JSON.stringify(bn)}`);
        } else {
            console.log(`Unknown Stream Response: , ${JSON.stringify(subscribeStreamResponse)}`);
        }
    });
    stream.on('error', (err) => {
        console.log('Stream Error: ' + JSON.stringify(err));
    });
    stream.on('end', () => {
        client.close();
        console.log('Stream ended.');
    });
    stream.write({
        "start_block_number": firstAvailableBlock.toString(),
        "end_block_number": (firstAvailableBlock + blockDelta).toString(),
    });
    stream.end();
    sleep(1);
    client.close();
};
