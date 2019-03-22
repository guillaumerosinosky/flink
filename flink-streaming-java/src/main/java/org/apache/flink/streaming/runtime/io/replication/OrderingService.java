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

package org.apache.flink.streaming.runtime.io.replication;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Hello.
 *
 * @param <IN>
 */
public class OrderingService<IN> implements StreamElementConsumer<StreamElement> {

	private final OneInputStreamOperator<IN, ?> operator;

	private final Object lock;

	private final StatusWatermarkValve statusWatermarkValve;

	private final Deduplication deduplicator;
	private final BiasAlgorithm merger;

	private final int replicationFactor;

	private static final Logger logger = LoggerFactory.getLogger(OrderingService.class);

	private Counter recordsIn;

	private void setupMetrics() {
		if (recordsIn == null) {
			try {
				recordsIn = ((OperatorMetricGroup) operator.getMetricGroup()).getIOMetricGroup().getNumRecordsInCounter();
			} catch (Exception e) {
				logger.warn("An exception occurred during the metrics setup.", e);
				recordsIn = new SimpleCounter();
			}
		}
	}

	@SuppressWarnings({"unchecked"})
	public OrderingService(
		OneInputStreamOperator in,
		Object lock,
		int numActualChannels,
		int logicalChannels,
		WatermarkGauge watermarkGauge,
		StreamStatusMaintainer maintainer
	) {

		logger.info("Instantiating with {} actual channels mapped to {} logical channels", numActualChannels, logicalChannels);

		this.operator = in;
		this.lock = lock;
		this.replicationFactor = numActualChannels / logicalChannels;
		this.deduplicator = new Deduplication(logicalChannels);
		this.merger = new BiasAlgorithm(logicalChannels, this); // TODO: Fix this (here goes the message generation rate)

		ForwardingValveOutputHandler outputHandler = new ForwardingValveOutputHandler(operator, lock, watermarkGauge, maintainer);
		this.statusWatermarkValve = new StatusWatermarkValve(logicalChannels, outputHandler);

		setupMetrics();
	}

	public void accept(StreamElement elem, int logicalChannel) throws Exception {
		if (elem.isWatermark()) {
			statusWatermarkValve.inputWatermark(elem.asWatermark(), logicalChannel);
		} else if (elem.isStreamStatus()) {
			statusWatermarkValve.inputStreamStatus(elem.asStreamStatus(), logicalChannel);
		} else if (elem.isLatencyMarker()) {
			synchronized (lock) {
				operator.processLatencyMarker(elem.asLatencyMarker());
			}
		} else {
			StreamRecord<IN> e = elem.asRecord();
			synchronized (lock) {
				recordsIn.inc();
				operator.setKeyContextElement1(e.asRecord());
				operator.processElement(e.asRecord());
			}
		}
	}

	public void process(StreamElement elem, int origin) throws Exception {
		int channel = logicalChannel(origin);

		if (this.deduplicator.isDuplicate(elem, channel)) {
			return;
		}

		this.merger.receive(elem, channel, elem.getSentTimestamp());
	}

	public void endOfStream() throws Exception {
		this.merger.endOfStream();
	}

	// TODO: Naming
	private int logicalChannel(int actualChannel) {
		return Math.floorDiv(actualChannel, replicationFactor);
	}

	private class ForwardingValveOutputHandler implements StatusWatermarkValve.ValveOutputHandler {
		private final OneInputStreamOperator<IN, ?> operator;
		private final Object lock;
		private final WatermarkGauge gauge;
		private final StreamStatusMaintainer maintainer;

		private ForwardingValveOutputHandler(
			final OneInputStreamOperator<IN, ?> operator,
			final Object lock,
			WatermarkGauge gauge,
			StreamStatusMaintainer maintainer
		) {
			this.operator = checkNotNull(operator);
			this.lock = checkNotNull(lock);
			this.gauge = checkNotNull(gauge);
			this.maintainer = checkNotNull(maintainer);
		}

		@Override
		public void handleWatermark(Watermark watermark) {
			try {
				synchronized (lock) {
					gauge.setCurrentWatermark(watermark.getTimestamp());
					operator.processWatermark(watermark);
				}
			} catch (Exception e) {
				throw new RuntimeException("Exception occurred while processing valve output watermark: ", e);
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public void handleStreamStatus(StreamStatus streamStatus) {
			try {
				synchronized (lock) {
					maintainer.toggleStreamStatus(streamStatus);
				}
			} catch (Exception e) {
				throw new RuntimeException("Exception occurred while processing valve output stream status: ", e);
			}
		}
	}
}
