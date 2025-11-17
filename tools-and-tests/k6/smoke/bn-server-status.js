import grpc from 'k6/net/grpc';
import { check, sleep } from 'k6';

export const options = {
    scenarios: {
        shared_iter_scenario: {
            executor: 'shared-iterations',
            vus: 10,
            iterations: 100,
            startTime: '0s',
        },
        per_vu_scenario: {
            executor: 'per-vu-iterations',
            vus: 10,
            iterations: 10,
            startTime: '10s',
        },
    },
};

// Download quickpizza.proto for grpc-quickpizza.grafana.com, located at:
// https://raw.githubusercontent.com/grafana/quickpizza/refs/heads/main/proto/quickpizza.proto
// and put it in the same folder as this script.
const client = new grpc.Client();
client.load(['./block-node-protobuf-0.23.1'], 'block-node/api/node_service.proto');

export default () => {
    client.connect('localhost:40840', {
        plaintext: true
    });

    // const data = { ingredients: ['Cheese'], dough: 'Thick' };
    const response = client.invoke('org.hiero.block.api.BlockNodeService/serverStatus', {});

    check(response, {
        'status is OK': (r) => r && r.status === grpc.StatusOK,
    });

    console.log(JSON.stringify(response.message));

    client.close();
    sleep(1);
};
