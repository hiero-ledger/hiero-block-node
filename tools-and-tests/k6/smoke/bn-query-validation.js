import { Client, StatusOK } from 'k6/net/grpc';
import { check, sleep } from 'k6';
import { SharedArray } from 'k6/data';

// Configure k6 VUs scheduling and iterations
// export const options = {
//     scenarios: {
//         shared_iter_scenario: {
//             executor: 'shared-iterations',
//             vus: 10,
//             iterations: 100,
//             startTime: '0s',
//         },
//         per_vu_scenario: {
//             executor: 'per-vu-iterations',
//             vus: 10,
//             iterations: 10,
//             startTime: '10s',
//         },
//     },
// };

// load test configuration data
const data = new SharedArray('BN Test Configs', function () {
    return JSON.parse(open('./data.json')).configs;
})[0];


const client = new Client();
client.load([data.protobufPath],
    'block-node/api/node_service.proto',
    'block-node/api/block_access_service.proto',
    'block-node/api/block_stream_subscribe_service.proto');

export default () => {
    client.connect(data.smokeTestConfigs.blockNodeUrl, {
        plaintext: true
    });

    const response = client.invoke('org.hiero.block.api.BlockNodeService/serverStatus', {});

    check(response, {
        'status is OK': (r) => r && r.status === StatusOK,
    });

    const firstAvailableBlock = response.message.firstAvailableBlock;
    const lastAvailableBlock = response.message.lastAvailableBlock;

    console.log(`First Available Block: ${firstAvailableBlock}, Latest Block: ${lastAvailableBlock}`);
    console.log(JSON.stringify(response.message));

    if (firstAvailableBlock === '18446744073709551615') {
        console.log(`No blocks to fetch, exiting test.`);
        client.close();
        return;
    } else {
        const firstAvailableBlockResponse = client.invoke('org.hiero.block.api.BlockAccessService/getBlock', {
            block_number: firstAvailableBlock,
        });

        check(firstAvailableBlockResponse, {
            'block fetch status is OK': (r) => r && r.status === StatusOK,
            'fetched block number is correct': (r) => r && r.message.block.items[0].blockHeader.number === firstAvailableBlock,
        });

        console.log(`Fetched Block '${firstAvailableBlock}' with size '${firstAvailableBlockResponse.message.block.items.length}' items`);

        const lastAvailableBlockResponse = client.invoke('org.hiero.block.api.BlockAccessService/getBlock', {
            block_number: lastAvailableBlock,
        });

        check(lastAvailableBlockResponse, {
            'block fetch status is OK': (r) => r && r.status === StatusOK,
            'fetched block number is correct': (r) => r && r.message.block.items[0].blockHeader.number === lastAvailableBlock,
        });

        console.log(`Fetched Block '${lastAvailableBlock}' with size '${lastAvailableBlockResponse.message.block.items.length}' items`);
    }

    client.close();
    sleep(1);
};
