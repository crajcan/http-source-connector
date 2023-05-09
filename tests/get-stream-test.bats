#!/usr/bin/env bats

load './bats-helpers/bats-assert/load'
load './bats-helpers/bats-support/load'

setup() {
    cargo build -p mock-http-server
    ./target/debug/mock-http-server & disown
    MOCK_PID=$!
    CONFIG_FILE=$(mktemp)
    cp ./tests/get-stream-test-config.yaml $CONFIG_FILE
    UUID=$(uuidgen | awk '{print tolower($0)}')
    TOPIC=${UUID}-topic
    fluvio topic create $TOPIC

    sed -i.BAK "s/TOPIC/${TOPIC}/g" $CONFIG_FILE
    cat $CONFIG_FILE

    RESULTS_FILE=$(mktemp)

    cargo build -p http-source
    ./target/debug/http-source --config $CONFIG_FILE & disown
    CONNECTOR_PID=$!
}

teardown() {
    fluvio topic delete $TOPIC
    kill $MOCK_PID
    kill $CONNECTOR_PID
}

@test "http-connector-get-stream-test" {
    echo "Starting consumer on topic $TOPIC"
    sleep 13

    # Consume from topic and pipe into results file
    fluvio consume -B -d $TOPIC > $RESULTS_FILE &

    # send get requests to mock server
    curl -s http://localhost:8080/get
    curl -s http://localhost:8080/get
    sleep 1

    # assert that the results file contains the expected output
    cat $RESULTS_FILE
    assert_output --partial "event: get request(s) received\ndata:{ \"gets\": 1, \"posts\": 0 }"
    assert_output --partial "event: get request(s) received\ndata:{ \"gets\": 2, \"posts\": 0 }"
}