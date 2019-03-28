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
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.replication.BiasAlgorithm;
import org.apache.flink.streaming.runtime.io.replication.Deduplication;
import org.apache.flink.streaming.runtime.io.replication.OneInputStreamOperatorAdapter;
import org.apache.flink.streaming.runtime.io.replication.LogicalChannelMapper;
import org.apache.flink.streaming.runtime.io.replication.TwoInputStreamOperatorAdapter;
import org.apache.flink.streaming.runtime.io.replication.Utils;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Input reader for {@link org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask}.
 *
 * <p>This internally uses a {@link StatusWatermarkValve} to keep track of {@link Watermark} and
 * {@link StreamStatus} events, and forwards watermarks to event subscribers once the
 * {@link StatusWatermarkValve} determines the watermarks from all inputs has advanced, or changes
 * the task's {@link StreamStatus} once status change is toggled.
 *
 * <p>Forwarding elements, watermarks, or status status elements must be protected by synchronizing
 * on the given lock object. This ensures that we don't call methods on a
 * {@link TwoInputStreamOperator} concurrently with the timer callback or other things.
 *
 * @param <IN1> The type of the records that arrive on the first input
 * @param <IN2> The type of the records that arrive on the second input
 */
@Internal
public class StreamTwoInputProcessor<IN1, IN2> {

	private final RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDeserializers;

	private final DeserializationDelegate<StreamElement> deserializationDelegate1;
	private final DeserializationDelegate<StreamElement> deserializationDelegate2;

	private RecordDeserializer<DeserializationDelegate<StreamElement>> currentRecordDeserializer;

	private final CheckpointBarrierHandler barrierHandler;

	private int currentChannel = -1;

	private final int numInputChannels1;

	private boolean isFinished;

	private final LogicalChannelMapper mapper;

	@SuppressWarnings("unchecked")
	public StreamTwoInputProcessor(
			Collection<InputGate> inputGates1,
			Collection<InputGate> inputGates2,
			TypeSerializer<IN1> inputSerializer1,
			TypeSerializer<IN2> inputSerializer2,
			TwoInputStreamTask<IN1, IN2, ?> checkpointedTask,
			CheckpointingMode checkpointMode,
			Object lock,
			IOManager ioManager,
			Configuration taskManagerConfig,
			StreamStatusMaintainer streamStatusMaintainer,
			TwoInputStreamOperator<IN1, IN2, ?> streamOperator,
			TaskIOMetricGroup metrics,
			WatermarkGauge input1WatermarkGauge,
			WatermarkGauge input2WatermarkGauge) throws IOException {

		final InputGate inputGate = InputGateUtil.createInputGate(inputGates1, inputGates2);

		this.barrierHandler = InputProcessorUtil.createCheckpointBarrierHandler(
			checkpointedTask, checkpointMode, ioManager, inputGate, taskManagerConfig);

		StreamElementSerializer<IN1> ser1 = new StreamElementSerializer<>(inputSerializer1);
		this.deserializationDelegate1 = new NonReusingDeserializationDelegate<>(ser1);

		StreamElementSerializer<IN2> ser2 = new StreamElementSerializer<>(inputSerializer2);
		this.deserializationDelegate2 = new NonReusingDeserializationDelegate<>(ser2);

		// Initialize one deserializer per input channel
		this.recordDeserializers = new SpillingAdaptiveSpanningRecordDeserializer[inputGate.getNumberOfInputChannels()];

		for (int i = 0; i < recordDeserializers.length; i++) {
			recordDeserializers[i] = new SpillingAdaptiveSpanningRecordDeserializer<>(
				ioManager.getSpillingDirectoriesPaths());
		}

		int numInputChannels1 = 0;
		for (InputGate gate: inputGates1) {
			numInputChannels1 += gate.getNumberOfInputChannels();
		}

		this.numInputChannels1 = numInputChannels1;

		metrics.gauge("checkpointAlignmentTime", barrierHandler::getAlignmentDurationNanos);

		int numChannels = Utils.numLogicalChannels(inputGate.getUpstreamReplicationFactor());

		// build up processing chain
		LogicalChannelMapper mapper = new LogicalChannelMapper(inputGate.getUpstreamReplicationFactor());

		Deduplication deduplication = new Deduplication(numChannels);
		BiasAlgorithm biasAlgorithm = new BiasAlgorithm(numChannels);
		TwoInputStreamOperatorAdapter operatorAdapter = new TwoInputStreamOperatorAdapter(
			streamOperator,
			inputGate,
			inputGates1,
			inputGates2,
			input1WatermarkGauge,
			input2WatermarkGauge,
			streamStatusMaintainer,
			lock
		);

		mapper
			.setNext(deduplication)
			.setNext(biasAlgorithm)
			.setNext(operatorAdapter);

		this.mapper = mapper;
	}

	public boolean processInput() throws Exception {
		if (isFinished) {
			return false;
		}

		while (true) {
			if (currentRecordDeserializer != null) {
				DeserializationResult result;
				boolean isFirstInput = currentChannel < numInputChannels1;

				if (isFirstInput) {
					result = currentRecordDeserializer.getNextRecord(deserializationDelegate1);
				} else {
					result = currentRecordDeserializer.getNextRecord(deserializationDelegate2);
				}

				if (result.isBufferConsumed()) {
					currentRecordDeserializer.getCurrentBuffer().recycleBuffer();
					currentRecordDeserializer = null;
				}

				if (result.isFullRecord()) {

					StreamElement element = (isFirstInput)
						? deserializationDelegate1.getInstance()
						: deserializationDelegate2.getInstance();

					this.mapper.accept(element, currentChannel);

					if (element.isRecord()) {
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

				} else {
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
	}
}
