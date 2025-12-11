import { Client, StatusOK } from 'k6/net/grpc';
import { check, sleep } from 'k6';
import { SharedArray } from 'k6/data';
import {GetBlockRequest, ServerStatusRequest} from "../lib/grpc.js";

// setup options
export const options = {
    thresholds: { // todo make these good defaults, we need these to display tags in the result, but also to ping us if they go over
        'grpc_req_duration{name:block_node_server_status}': ['p(95)<300'],
        'grpc_req_duration{name:get_first_block}': ['p(95)<300'],
        'grpc_req_duration{name:get_last_block}': ['p(95)<300'],
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

// run test
export default () => {
    client.connect(data.blockNodeUrl, {
        plaintext: true
    });
    const serverStatusParams = {
        tags: {name: 'block_node_server_status'}
    };
    const response = new ServerStatusRequest(client).invoke(serverStatusParams);
    check(response, {
        'status is OK': (r) => r && r.status === StatusOK,
    });
    const firstAvailableBlock = response.message.firstAvailableBlock;
    const lastAvailableBlock = response.message.lastAvailableBlock;
    console.log(`First Available Block: ${firstAvailableBlock}, Latest Block: ${lastAvailableBlock}`);
    console.log(JSON.stringify(response.message));
    if (firstAvailableBlock === '18446744073709551615') {
        console.log(`No blocks to fetch, exiting test.`);
    } else {
        const getFirstBlockRequestParams = {
            tags: {name: 'get_first_block'},
        };
        const firstAvailableBlockResponse = new GetBlockRequest(client).invoke(firstAvailableBlock, getFirstBlockRequestParams);
        check(firstAvailableBlockResponse, {
            'block fetch status is OK': (r) => r && r.status === StatusOK,
            'fetched block number is correct': (r) => r && r.message.block.items[0].blockHeader.number === firstAvailableBlock,
        });
        console.log(`Fetched Block '${firstAvailableBlock}' with size '${firstAvailableBlockResponse.message.block.items.length}' items`);
        const getLastBlockRequestParams = {
            tags: {name: 'get_last_block'},
        }
        const lastAvailableBlockResponse = new GetBlockRequest(client).invoke(lastAvailableBlock, getLastBlockRequestParams);
        check(lastAvailableBlockResponse, {
            'block fetch status is OK': (r) => r && r.status === StatusOK,
            'fetched block number is correct': (r) => r && r.message.block.items[0].blockHeader.number === lastAvailableBlock,
        });
        console.log(`Fetched Block '${lastAvailableBlock}' with size '${lastAvailableBlockResponse.message.block.items.length}' items`);
    }
    client.close();
    sleep(1);
};
