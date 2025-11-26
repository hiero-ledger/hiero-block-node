import { Client, StatusOK } from 'k6/net/grpc';
import { check, sleep } from 'k6';
import { SharedArray } from 'k6/data';

// Configure k6 VUs scheduling and iterations
export const options = {
    stages: [
        { duration: '2m', target: 40 }, // traffic ramp-up from 1 to 40 users (all CNs and a shadow MN) over 5 minutes.
        { duration: '3m', target: 100 }, // stay at 100 users for 3 minutes
        { duration: '1m', target: 0 }, // ramp-down to 0 users
    ],
};

// load test configuration data
const data = new SharedArray('BN Test Configs', function () {
    return JSON.parse(open('./../data.json')).configs;
})[0];

const client = new Client();
client.load([data.protobufPath], 'block-node/api/node_service.proto');

export default () => {
    client.connect(data.blockNodeUrl, {
        plaintext: true
    });

    const response = client.invoke('org.hiero.block.api.BlockNodeService/serverStatus', {});

    check(response, {
        'status is OK': (r) => r && r.status === StatusOK,
    });

    console.log(JSON.stringify(response.message));

    client.close();
    sleep(1);
};
