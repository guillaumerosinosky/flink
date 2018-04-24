#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

source "$(dirname "$0")"/common.sh

QUERYABLE_STATE_SERVER_JAR=${TEST_INFRA_DIR}/../../flink-end-to-end-tests/flink-queryable-state-test/target/QueryableStateEmailApp.jar
QUERYABLE_STATE_CLIENT_JAR=${TEST_INFRA_DIR}/../../flink-end-to-end-tests/flink-queryable-state-test/target/QueryableStateEmailClient.jar

#####################
# Test that queryable state works as expected with HA mode when restarting a taskmanager
#
# The general outline is like this:
# 1. start cluster in HA mode with 1 TM
# 2. start a job that exposes queryable state from a mapstate with increasing num. of keys
# 3. query the state with a queryable state client and expect no error to occur
# 4. stop the TM
# 5. check how many keys were in our mapstate at the time of the latest snapshot
# 6. start a new TM
# 7. query the state with a queryable state client and retrieve the number of elements
#    in the mapstate
# 8. expect the number of elements in the mapstate after restart of TM to be > number of elements
#    at last snapshot
#
# Globals:
#   QUERYABLE_STATE_SERVER_JAR
#   QUERYABLE_STATE_CLIENT_JAR
# Arguments:
#   None
# Returns:
#   None
#####################
function run_test() {
    local EXIT_CODE=0
    local PARALLELISM=1 # parallelism of queryable state app
    local PORT="9069" # port of queryable state server

    clean_out_files # to ensure there are no files accidentally left behind by previous tests
    link_queryable_state_lib
    start_ha_cluster

    local JOB_ID=$(${FLINK_DIR}/bin/flink run \
        -p ${PARALLELISM} \
        -d ${QUERYABLE_STATE_SERVER_JAR} \
        --state-backend "rocksdb" \
        --tmp-dir file://${TEST_DATA_DIR} \
        | awk '{print $NF}' | tail -n 1)

    wait_job_running ${JOB_ID}

    sleep 20 # sleep a little to have some state accumulated

    echo SERVER: $(get_queryable_state_server_ip)
    echo PORT: $(get_queryable_state_proxy_port)

    java -jar ${QUERYABLE_STATE_CLIENT_JAR} \
        --host $(get_queryable_state_server_ip) \
        --port $(get_queryable_state_proxy_port) \
        --iterations 1 \
        --job-id ${JOB_ID}

    if [ $? != 0 ]; then
        echo "An error occured when executing queryable state client"
        exit 1
    fi

    ${FLINK_DIR}/bin/taskmanager.sh stop
    latest_snapshot_count=$(cat $FLINK_DIR/log/*out* | grep "on snapshot" | tail -n 1 | awk '{print $4}')
    echo "Latest snapshot count was ${latest_snapshot_count}"

    sleep 65 # this is a little longer than the heartbeat timeout so that the TM is gone

    ${FLINK_DIR}/bin/taskmanager.sh start

    wait_for_tm

    sleep 20 # sleep a little to have state restored

    local num_entries_in_map_state_after=$(java -jar ${QUERYABLE_STATE_CLIENT_JAR} \
        --host $(get_queryable_state_server_ip) \
        --port $(get_queryable_state_proxy_port) \
        --iterations 1 \
        --job-id ${JOB_ID} | grep "MapState has" | awk '{print $3}')

    echo "after: $num_entries_in_map_state_after"

    if ((latest_snapshot_count > num_entries_in_map_state_after)); then
        echo "An error occured"
        EXIT_CODE=1
    fi

    exit ${EXIT_CODE}
}

function test_cleanup {
    clean_out_files
    unlink_queryable_state_lib
    stop_cluster
    cleanup
}

trap test_cleanup EXIT
run_test
