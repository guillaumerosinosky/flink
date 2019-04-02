/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.replication.BiasAlgorithm;
import org.apache.flink.streaming.runtime.io.replication.Chainable;
import org.apache.flink.streaming.runtime.io.replication.Deduplication;
import org.apache.flink.streaming.runtime.io.replication.LeaderBasedReplication;
import org.apache.flink.streaming.runtime.io.replication.LogicalChannelMapper;
import org.apache.flink.streaming.runtime.io.replication.OneInputStreamOperatorAdapter;
import org.apache.flink.streaming.runtime.io.replication.OrderBroadcaster;
import org.apache.flink.streaming.runtime.io.replication.OrderBroadcasterImpl;
import org.apache.flink.streaming.runtime.io.replication.Utils;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Input reader for {@link org.apache.flink.streaming.runtime.tasks.OneInputStreamTask}.
 *
 * <p>This internally uses a {@link StatusWatermarkValve} to keep track of {@link Watermark} and
 * {@link StreamStatus} events, and forwards them to event subscribers once the
 * {@link StatusWatermarkValve} determines the {@link Watermark} from all inputs has advanced, or
 * that a {@link StreamStatus} needs to be propagated downstream to denote a status change.
 *
 * <p>Forwarding elements, watermarks, or status status elements must be protected by synchronizing
 * on the given lock object. This ensures that we don't call methods on a
 * {@link OneInputStreamOperator} concurrently with the timer callback or other things.
 *
 * @param <IN> The type of the record that can be read with this record reader.
 */
@Internal
public class StreamInputProcessor<IN> {

	private static final Logger LOG = LoggerFactory.getLogger(StreamInputProcessor.class);

	private final RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDeserializers;

	private final Chainable first;

	private final LeaderBasedReplication inputOrdering;

	private final CuratorFramework curator;

	private RecordDeserializer<DeserializationDelegate<StreamElement>> currentRecordDeserializer;

	private final DeserializationDelegate<StreamElement> deserializationDelegate;

	private final CheckpointBarrierHandler barrierHandler;

	/** Number of input channels the valve needs to handle. */
	private final int numInputChannels;

	/**
	 * The channel from which a buffer came, tracked so that we can appropriately map
	 * the watermarks and watermark statuses to channel indexes of the valve.
	 */
	private int currentChannel = -1;

	private final OneInputStreamOperator<IN, ?> streamOperator;

	// ---------------- Metrics ------------------

	private boolean isFinished;
	private final LeaderSelector leaderSelector;

	@SuppressWarnings("unchecked")
	public StreamInputProcessor(
		InputGate[] inputGates,
		TypeSerializer<IN> inputSerializer,
		StreamTask<?, ?> checkpointedTask,
		CheckpointingMode checkpointMode,
		Object checkpointLock,
		IOManager ioManager,
		Configuration taskManagerConfig,
		StreamStatusMaintainer streamStatusMaintainer,
		OneInputStreamOperator<IN, ?> streamOperator,
		TaskIOMetricGroup metrics,
		WatermarkGauge watermarkGauge,
		RpcService rpcService,
		ExecutionAttemptID executionAttempt,
		String replicaGroup
	) throws Exception {

		checkNotNull(checkpointLock);

		InputGate inputGate = InputGateUtil.createInputGate(inputGates);

		this.barrierHandler = InputProcessorUtil.createCheckpointBarrierHandler(
			checkpointedTask, checkpointMode, ioManager, inputGate, taskManagerConfig);

		StreamElementSerializer<IN> ser = new StreamElementSerializer<>(inputSerializer);
		this.deserializationDelegate = new NonReusingDeserializationDelegate<>(ser);

		// Initialize one deserializer per input channel
		this.recordDeserializers = new SpillingAdaptiveSpanningRecordDeserializer[inputGate.getNumberOfInputChannels()];

		for (int i = 0; i < recordDeserializers.length; i++) {
			recordDeserializers[i] = new SpillingAdaptiveSpanningRecordDeserializer<>(
				ioManager.getSpillingDirectoriesPaths());
		}

		this.numInputChannels = inputGate.getNumberOfInputChannels();

		this.streamOperator = checkNotNull(streamOperator);

		metrics.gauge("checkpointAlignmentTime", barrierHandler::getAlignmentDurationNanos);

		int numLogicalChannels = Utils.numLogicalChannels(inputGate.getUpstreamReplicationFactor());

		this.first = new LogicalChannelMapper(inputGate.getUpstreamReplicationFactor());

		boolean useBias = false;

		this.curator = CuratorFrameworkFactory.newClient("localhost:2181", new ExponentialBackoffRetry(1000, 3));
		OrderBroadcaster b = new OrderBroadcasterImpl(curator, rpcService, executionAttempt, replicaGroup);
		LeaderBasedReplication leaderBasedReplication = new LeaderBasedReplication(numLogicalChannels, b, 200);

		LOG.info("Setting up leader selection for replica group {}", replicaGroup);
		leaderSelector = new LeaderSelector(curator, "/flink/leader/" + replicaGroup, leaderBasedReplication);
		leaderSelector.start();
		Chainable orderingAlgorithm = (useBias)
			? new BiasAlgorithm(numLogicalChannels)
			: leaderBasedReplication;

		// TODO: Thesis - Fix this and allow BiasAlgorithm to work with it also
		this.inputOrdering = (LeaderBasedReplication) orderingAlgorithm;

		first.setNext(new Deduplication(numLogicalChannels))
			.setNext(orderingAlgorithm)
			.setNext(new OneInputStreamOperatorAdapter(
				this.streamOperator,
				watermarkGauge, streamStatusMaintainer, numLogicalChannels,
				checkpointLock
			));
	}

	public boolean processInput() throws Exception {

		if (isFinished) {
			return false;
		}

		// do this until we processed on full record or reached the end
		while (true) {

			if (currentRecordDeserializer != null) {
				DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);

				if (result.isBufferConsumed()) {
					currentRecordDeserializer.getCurrentBuffer().recycleBuffer();
					currentRecordDeserializer = null;
				}

				if (result.isFullRecord()) {
					StreamElement element = deserializationDelegate.getInstance();
					boolean isRecord = element.isRecord();

					this.first.accept(element, currentChannel);

					if (isRecord) {
						return true;
					} else {
						continue;
					}
				}
			}

			final BufferOrEvent bufferOrEvent = barrierHandler.getNextNonBlocked();
			if (bufferOrEvent != null) {
				if (bufferOrEvent.isBuffer()) {
					currentChannel = bufferOrEvent.getChannelIndex();
					currentRecordDeserializer = recordDeserializers[currentChannel];
					currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
				}
				else {
					// Event received
					final AbstractEvent event = bufferOrEvent.getEvent();
					if (event.getClass() != EndOfPartitionEvent.class) {
						throw new IOException("Unexpected event: " + event);
					}
				}
			} else {
				isFinished = true;
				if (!barrierHandler.isEmpty()) {
					throw new IllegalStateException("Trailing data in checkpoint barrier handler.");
				}
				this.first.endOfStream();
				return false;
			}
		}
	}

	public void cleanup() throws IOException {
		// clear the buffers first. this part should not ever fail
		for (RecordDeserializer<?> deserializer : recordDeserializers) {
			Buffer buffer = deserializer.getCurrentBuffer();
			if (buffer != null && !buffer.isRecycled()) {
				buffer.recycleBuffer();
			}
			deserializer.clear();
		}

		// cleanup the barrier handler resources
		barrierHandler.cleanup();
		leaderSelector.close();
		curator.close();
	}

	public void triggerAcceptInputOrdering(List<Integer> newBatch) throws Exception {
		this.inputOrdering.acceptOrdering(newBatch);
	}
}
