#!/bin/bash

# Variable to track whether shutdown has been called
SHUTDOWN_CALLED=0

# Function to shut down the background processes
shutdown() {
    if [[ $SHUTDOWN_CALLED -eq 1 ]]; then
        return
    fi
    SHUTDOWN_CALLED=1

    echo "Shutting down background processes..."

    if [[ -n "$PRODUCER_PID" ]]; then
        if kill -0 $PRODUCER_PID 2>/dev/null; then
            kill $PRODUCER_PID
            wait $PRODUCER_PID 2>/dev/null
            echo "Producer process $PRODUCER_PID terminated."
        else
            echo "Error: Producer process $PRODUCER_PID has already terminated."
            echo "Producer logs:"
            cat producer.log
            exit 1
        fi
    fi

    if [[ -n "$CONSUMER_PID" ]]; then
        if kill -0 $CONSUMER_PID 2>/dev/null; then
            kill $CONSUMER_PID
            wait $CONSUMER_PID 2>/dev/null
            echo "Consumer process $CONSUMER_PID terminated."
        else
            echo "Error: Consumer process $CONSUMER_PID has already terminated."
            echo "Consumer logs:"
            cat consumer.log
            exit 1
        fi
    fi
}

# Trap any exit signal to ensure everything is cleaned up
trap "shutdown; exit 1" ERR SIGINT SIGTERM


# 1. Verify that the logs have the startup pattern (only if log file is provided)
LOG_FILE=$1
STARTUP_PATTERN="Block Node Server started at port"

if [[ -n "$LOG_FILE" ]]; then
    if grep -q "$STARTUP_PATTERN" "$LOG_FILE"; then
        echo "Startup pattern found in logs."
    else
        echo "Startup pattern not found in logs."
        exit 1
    fi
else
    echo "No log file provided, skipping startup pattern check. LOG_FILE is: $LOG_FILE"
fi

# 2. Start the consumer script with parameters 1 1000 and save logs to consumer.log
./consumer.sh 1 1000 > consumer.log 2>&1 &
CONSUMER_PID=$!
echo "Started consumer with PID $CONSUMER_PID, logging to consumer.log"

# 3. Start the producer script with parameter 1 and save logs to producer.log
./producer.sh 1 > producer.log 2>&1 &
PRODUCER_PID=$!
echo "Started producer with PID $PRODUCER_PID, logging to producer.log"

# Sleep time after starting the consumer
sleep 5  # Adjust sleep time as needed

# 4. Run the get-block script with parameter 1 and save logs to get-block.log
if ! ./get-block.sh 1 > get-block.log 2>&1; then
    echo "get-block.sh failed."
    echo "get-block logs:"
    cat get-block.log
    shutdown
    exit 1
fi
echo "get-block.sh executed successfully."

# 5. Call the endpoints /health/livez and /health/readyz
SERVER_URL="http://localhost:8080"
LIVENESS_ENDPOINT="/healthz/livez"
READINESS_ENDPOINT="/healthz/readyz"

if ! curl -f $SERVER_URL$LIVENESS_ENDPOINT; then
    echo "$LIVENESS_ENDPOINT failed."
    shutdown
    exit 1
fi
echo "$LIVENESS_ENDPOINT endpoint is healthy."

if ! curl -f $SERVER_URL$READINESS_ENDPOINT; then
    echo "$READINESS_ENDPOINT endpoint failed."
    shutdown
    exit 1
fi
echo "$READINESS_ENDPOINT endpoint is ready."

# 6. Shut everything down
shutdown

# 7. Return success
echo "Smoke test completed successfully."
exit 0
