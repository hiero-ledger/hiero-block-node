import { Client, StatusOK, Stream } from 'k6/net/grpc';
import { check, sleep } from 'k6';
import { SharedArray } from 'k6/data';

// load test configuration data
const data = new SharedArray('BN Test Configs', function () {
    return JSON.parse(open('./../data.json')).configs;
})[0];

const client = new Client();
client.load([data.protobufPath],
    'block-node/api/node_service.proto',
    'block-node/api/block_access_service.proto',
    'block-node/api/block_stream_subscribe_service.proto');

export default () => {
    client.connect(data.blockNodeUrl, {
        plaintext: true
    });

    const response = client.invoke('org.hiero.block.api.BlockNodeService/serverStatus', {});

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
        console.log(`Block Node has only ${BlockDelta + 1n} blocks to stream.`);
    } else {
        blockDelta = BigInt(data.smokeTestConfigs.numOfBlocksToStream);
        console.log(`Block Node has sufficient blocks to stream ${data.smokeTestConfigs.numOfBlocksToStream} blocks.`);
    }

    // stream block from subscribe API
    const stream = new Stream(client, 'org.hiero.block.api.BlockStreamSubscribeService/subscribeBlockStream');
    stream.on('data', (subscribeStreamResponse) => {

        if (subscribeStreamResponse.blockItems) {
            console.log(`Stream Response: BlockHeader for Block ${JSON.stringify(subscribeStreamResponse.blockItems.blockItems[0].blockHeader.number)}`);
        } else if (subscribeStreamResponse.endOfBlock) {
            console.log(`Stream Response: endOfBlock for Block ${JSON.stringify(subscribeStreamResponse.endOfBlock.blockNumber)}`);
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
