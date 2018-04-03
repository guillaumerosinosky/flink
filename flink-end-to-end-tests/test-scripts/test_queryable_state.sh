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

# setup
echo "Moving flink-queryable-state-runtime from opt/ to lib/"
mv ${FLINK_DIR}/opt/flink-queryable-state-runtime* ${FLINK_DIR}/lib/
fail_on_non_zero_exit_code $? "Could not move queryable state runtime to /lib, aborting tests."

start_cluster

TEST_PROGRAM_JAR=${TEST_INFRA_DIR}/../../flink-end-to-end-tests/flink-queryable-state-test/target/QueryableStateEmailApp.jar   # app with queryable state
CLIENT_JAR=${TEST_INFRA_DIR}/../../flink-end-to-end-tests/flink-queryable-state-test/target/QueryableStateEmailClient.jar      # jar that queries the state
STREAM_KEY="some_key"                                                                               # key of which the state is queried
STATE_NAME="value-state"                                                                            # state that gets queried
PORT="9069"                                                                                         # port to query from
QUERY_NAME="current-value-query"                                                                    # name of the query

# start app with queryable state and wait for it to be available
JOB_ID=$(${FLINK_DIR}/bin/flink run -p 1 -d ${TEST_PROGRAM_JAR} --state-backend $1 --tmp-dir file://${TEST_DATA_DIR} | awk '{print $NF}' | tail -n 1)

expect_in_taskmanager_logs "Flat Map.*switched from DEPLOYING to RUNNING" 10

# get host ip address of queryable state from log files
HOST=$(cat ${FLINK_DIR}/log/flink*taskexecutor*log | grep "Started Queryable State Server" \
    | awk '{split($11, a, "/"); split(a[2], b, ":"); print b[1]}')

# run the client and query state the first time
first_result=$(java -jar ${CLIENT_JAR} \
    --host ${HOST} \
    --port ${PORT} \
    --job-id ${JOB_ID})

EXIT_CODE=$?

# cancel that job that we ran as a daemon
${FLINK_DIR}/bin/flink cancel ${JOB_ID}
expect_in_taskmanager_logs "Un-registering task and sending final execution state CANCELED to JobManager for task Flat" 10

# clean up
echo "Moving flink-queryable-state-runtime from lib/ to opt/"
mv ${FLINK_DIR}/lib/flink-queryable-state-runtime* ${FLINK_DIR}/opt/

# Exit
exit ${EXIT_CODE}
