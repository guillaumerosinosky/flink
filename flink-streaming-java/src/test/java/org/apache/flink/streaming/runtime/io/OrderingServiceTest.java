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

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.io.replication.BiasAlgorithm;
import org.apache.flink.streaming.runtime.io.replication.Merger;
import org.apache.flink.streaming.runtime.io.replication.OrderingService;
import org.apache.flink.streaming.runtime.io.replication.StreamElementConsumer;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;

import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class OrderingServiceTest {

	private static final WatermarkGauge watermarkGauge = new WatermarkGauge();
	private static final StreamStatusMaintainer streamStatusMaintainer = new StreamStatusMaintainer() {
		@Override
		public void toggleStreamStatus(StreamStatus streamStatus) {

		}

		@Override
		public StreamStatus getStreamStatus() {
			return null;
		}
	};

	private int logicalChannel(int actualChannel, int replicationFactor) {
		return Math.floorDiv(actualChannel, replicationFactor);
	}

	@Test
	public void testDeliversDeterministically() throws Exception {
		Random r = new Random();

		int numActualChannels = 8;
		int numLogicalChannels = 4;
		int numOrderingServices = 4;
		int numElementsTotal = 8000;

		List<OrderingService> services = new LinkedList<>();
		List<MockInputOperator> operators = new LinkedList<>();
		List<Queue<StreamElement>[]> perServiceQueues = new LinkedList<>();

		// setup services, operators and queues
		for (int i = 0; i < numOrderingServices; i++) {
			MockInputOperator mo = new MockInputOperator();
			OrderingService<Elem> o = new OrderingService<>(
				mo,
				new Object(),
				numActualChannels,
				numLogicalChannels,
				watermarkGauge, // TODO: Fix those
				streamStatusMaintainer
			);

			services.add(o);
			operators.add(mo);

			Queue[] queues = new Queue[numActualChannels];
			for (int j = 0; j < queues.length; j++) {
				queues[j] = new LinkedList();
			}

			perServiceQueues.add(queues);
		}

		// randomly write elements to queues

		int[] perChannelDedupTimestamp = new int[numLogicalChannels];
		int elementId = 0;

		for (int i = 0; i < numElementsTotal; i++) {

			int randomPhysicalChannel = r.nextInt(numActualChannels);
			int randomLogicalChannel = logicalChannel(randomPhysicalChannel, 2);

			int dedupTs = perChannelDedupTimestamp[randomLogicalChannel]++;
			long sentTimestamp = System.nanoTime();
			StreamElement e = new StreamRecord<>(String.format("{id: %d, sent: %d}", elementId++, sentTimestamp));
			e.setSentTimestamp(sentTimestamp);
			e.setDeduplicationTimestamp(dedupTs);

			for (Queue[] q : perServiceQueues) {
				q[randomLogicalChannel * 2].add(e);
				q[(randomLogicalChannel * 2) + 1].add(e);
			}
		}

		// randomly pick elements from queues and deliver them until all queues are emtpy
		boolean anyQueueNotEmpty = true;
		while (anyQueueNotEmpty) {

			for (int i = 0; i < services.size(); i++) {
				int randomPhysicalChannel = r.nextInt(numActualChannels);
				StreamElement e = perServiceQueues.get(i)[randomPhysicalChannel].poll();
				if (e != null) {
					services.get(i).process(e, randomPhysicalChannel);
				}
			}

			List<Queue> flattenedQueues = new LinkedList<>();
			for (Queue[] queues : perServiceQueues) {
				flattenedQueues.addAll(Arrays.asList(queues));
			}

			for (Queue q : flattenedQueues) {
				if (!q.isEmpty()) {
					break;
				} else {
					anyQueueNotEmpty = false;
				}
			}
		}

//		o1.endOfStream();
//		o2.endOfStream();

		for (int i = 0; i < operators.size() - 1; i++) {
			List<StreamRecord<String>> a = operators.get(i).received;
			List<StreamRecord<String>> b = operators.get(i + 1).received;
			int commonPrefix = Math.min(a.size(), b.size());

			Assert.assertEquals(a.subList(0, commonPrefix), b.subList(0, commonPrefix));
		}
	}


	private static class MockInputOperator extends AbstractStreamOperator<String> implements OneInputStreamOperator<String, String> {

		private List<StreamRecord<String>> received = new LinkedList<>();

		@Override
		public void processElement(StreamRecord<String> element) throws Exception {
			if (this.received.contains(element)) {
				throw new RuntimeException("Received duplicate element " + element + " with dedup ts " + element.getDeduplicationTimestamp());
			}
			this.received.add(element);
		}
	}

	@Test
	public void testPerformance() throws Exception {

		BufferedWriter buf = new BufferedWriter(new FileWriter("/tmp/delivery-time.csv"));

		int numProducers = 2;

		Merger b = new BiasAlgorithm(numProducers, (StreamElementConsumer<Elem>) (elem, channel) -> {
			long deliveredAt = System.currentTimeMillis();
			long delivered = System.nanoTime();
			long duration = delivered - elem.received;
			buf.write(elem.channel + "," + elem.created + "," + elem.received + "," + deliveredAt + "," + duration + "\n");
		});

		buf.write(b.getClass().getName() + "\n");

		BlockingQueue<Elem> queue = new LinkedBlockingQueue<>();

		Random r = new Random();
		for (int i = 0; i < numProducers; i++) {
			Thread t = new Thread(new VariableProducer(queue, 0, constant(10)));
			t.start();

			Thread t1 = new Thread(new VariableProducer(queue, 1, constant(1000)));
			t1.start();
		}

		AtomicBoolean stopped = new AtomicBoolean(false);

		Executors.newScheduledThreadPool(1).schedule(() -> stopped.set(true), 10, TimeUnit.SECONDS);

		while (!stopped.get() || !queue.isEmpty()) {
			Elem e = queue.poll();
			if (e != null) {
				e.received = System.nanoTime();
				b.receive(e, e.channel, e.created);
			}
		}

		b.endOfStream();
		buf.flush();
	}

	@Test
	public void showGraph() {
		for (int i = 0; i < 100; i++) {
			System.out.println("x: " + i + ", y:" + triangle(15).update(i));
		}
	}

	private interface Rate {
		int update(int tick);
	}

	private Rate triangle(int maximum) {

		return tick -> {
			int max = maximum - 1;
			int iteration = Math.floorDiv(tick, max);

			if (iteration % 2 == 0) {
				return (tick - iteration * max) + 1;
			} else {
				return (max - (tick - iteration * max)) + 1;
			}
		};
	}

	private Rate hiccup(int rate) {
		return tick -> {
			if (tick >= 15 && tick < 20) {
				return 0;
			} else {
				return rate;
			}
		};
	}

	private Rate constant(int rate) {
		return tick -> rate;
	}

	private Rate linear(int gradient) {
		return tick -> (tick * gradient);
	}

	private Rate sin(int amplitude) {
		return tick -> (int) (amplitude + Math.ceil((amplitude * Math.sin(tick / Math.PI))));
	}

	// TODO: Use more than one queue and be able to apply "backpressure"...
	private class VariableProducer implements Runnable {

		private final BlockingQueue<Elem> queue;
		private final int channel;
		private final Rate rate;

		AtomicInteger pause;
		AtomicInteger count;

		private VariableProducer(BlockingQueue<Elem> queue, int channel, Rate r) {
			this.queue = queue;
			this.channel = channel;

			this.pause = new AtomicInteger(rateToPause(1));
			this.count = new AtomicInteger(0);
			this.rate = r;
		}

		public void doWork() {
			long now = System.currentTimeMillis();
			Elem e = new Elem(channel, now);
			queue.add(e);
		}

		private int rateToPause(int elementsPerSecond) {

			if (elementsPerSecond == 0) {
				return -1;
			}

			if (elementsPerSecond > 1000) {
				throw new RuntimeException("Cannot handle rate > 1000");
			}

			double elementsPerMilli = (double) elementsPerSecond / 1000.0;
			return (int) Math.ceil(1 / elementsPerMilli);
		}

		public void run() {
			Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new Runnable() {

				int tick = 1;

				@Override
				public void run() {
					int pause = rateToPause(VariableProducer.this.rate.update(tick++));
					VariableProducer.this.pause.set(pause);
				}

			}, 1, 1, TimeUnit.SECONDS);

			while (true) {
				try {
					if (this.pause.get() != -1) {
						this.doWork();
						this.count.getAndIncrement();
						Thread.sleep(this.pause.get());
					} else {
						Thread.sleep(1);
					}
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}

			}
		}
	}

	private static final class Elem {
		private int channel;
		private long created;
		private long received;

		public Elem(int channel, long created) {
			this.channel = channel;
			this.created = created;
		}
	}
}
