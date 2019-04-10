package org.apache.flink.streaming.runtime.io.replication;

import org.apache.flink.streaming.runtime.streamrecord.BoundedDelayMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

public class BetterBiasAlgorithm extends Chainable {

	private static final Logger LOG = LoggerFactory.getLogger(BetterBiasAlgorithm.class);

	private long currentEpoch = 0;

	private Map<Long, PriorityQueue<QueueElem>> messages;

	private long[] epochStart;
	private long[] currentEpochAtChannel;
	private long[] previousTimestamp;
	private final int numProducers;

	private Map<Long, Map<Integer, Long>> latest;
	private Map<Long, Map<Integer, Long>> epochCount;

	public BetterBiasAlgorithm(int numProducers) {
		this.epochStart = new long[numProducers];
		this.currentEpochAtChannel = new long[numProducers];
		this.previousTimestamp = new long[numProducers];
		this.epochCount = new HashMap<>();
		this.latest = new HashMap<>();
		this.numProducers = numProducers;

		HashMap<Integer, Long> value = new HashMap<>();
		this.latest.put(0L, value);
		for (int i = 0; i < numProducers; i++) {
			this.epochStart[i] = 0;
			this.currentEpochAtChannel[i] = 0;
			this.previousTimestamp[i] = 0;
			value.put(i, 0L);
		}

		this.messages = new HashMap<>();
		this.messages.put(0L, createNewQueue());
	}

	private PriorityQueue<QueueElem> createNewQueue() {
		return new PriorityQueue<>(Comparator
			.comparingLong(QueueElem::getPrev)
			.thenComparingInt(QueueElem::getChannel));
	}

	@Override
	public void accept(StreamElement element, int channel) throws Exception {

		long epoch, curr, prev;

		if (element.isBoundedDelayMarker()) {
			epoch = element.asBoundedDelayMarker().getEpoch();
			curr = Long.MAX_VALUE;
			prev = Long.MAX_VALUE;
		} else {
			epoch = currentEpochAtChannel[channel];

			if (element.getSentTimestamp() == -1) {
				throw new RuntimeException("sentTs not properly initialized!");
			}

			curr = element.getSentTimestamp() - epochStart[channel];
			prev = previousTimestamp[channel];
			Preconditions.checkState(curr > prev, "curr needs to be bigger than prev for deterministic ordering");
		}

		if (element.isBoundedDelayMarker()) {
			currentEpochAtChannel[channel] = epoch + 1;
//			epochStart[channel] = element.getSentTimestamp();
		}

		LOG.trace("Received element (epoch={}, prev={}, curr={}, isBounded={}, channel={})", epoch, prev, curr, element.isBoundedDelayMarker(), channel);
		previousTimestamp[channel] = curr;

		curr += bias(curr, channel);
		prev += bias(prev, channel);

		QueueElem e = new QueueElem(element, prev, curr, epoch, channel);
		enqueue(epoch, e);
		updateLatestInEpoch(channel, epoch, curr);

		if (epoch == currentEpoch) {
			boolean done = false;
			while (!done) {
				QueueElem q = messages.get(epoch).peek();

				if (q != null && canBeDelivered(epoch, q)) {
					this.messages.get(epoch).poll();
					deliver(q, epoch);
				} else {
					done = true;
				}
			}
		}

		if (newEpochHasStarted()) { // we have seen epoch messages for both channels

			LOG.info("Starting procedure for epoch {} with biases {}, {}", currentEpoch + 1, bias(0, 0), bias(0, 1));

			epochCount.computeIfAbsent(epoch, k -> new HashMap<>());
			epochCount.get(epoch).forEach((chan, count) -> {
				System.out.println(count + " elements in channel " + chan + " in epoch " + epoch);
			});

			// emit all epoch markers forward round robin over all channels
			for (int i = 0; i < numProducers; i++) {
				QueueElem elem = this.messages.get(currentEpoch).poll();
				if (!(elem.m instanceof BoundedDelayMarker)) {
					LOG.error("elem is not an end-of-epoch marker for current epoch {}", currentEpoch);
					LOG.error("Dumping elements of channel {} (including first dequeued element that was expected to be a BoundedDelayMarker)", i);
					LOG.error("------ Queue {} ------", i);
					LOG.error(elem.toString());
					for (QueueElem q : this.messages.get(currentEpoch)) {
						LOG.error(q.toString());
					}
					LOG.error("----------------------");
					throw new RuntimeException("Should be the end of epoch marker");
				}
				deliver(elem, epoch);
			}

			if (this.messages.get(currentEpoch).size() != 0) {
				throw new RuntimeException("Epoch queue should be empty after markers");
			}

			this.messages.remove(currentEpoch);

			currentEpoch++;

			messages.put(epoch, createNewQueue());

			Map<Integer, Long> m = new HashMap<>();
			for (int i = 0; i < numProducers; i++) {
				m.put(i, 0L);
				previousTimestamp[i] = 0;
			}
			latest.put(currentEpoch, m);
		}
	}

	private boolean newEpochHasStarted() {
		long minValue = -1;

		for (int i = 0; i < numProducers; i++) {
			long epoch = currentEpochAtChannel[i];
			if (minValue == -1 || epoch < minValue) {
				minValue = epoch;
			}
		}

		return minValue > currentEpoch;
	}

	private void updateLatestInEpoch(int channel, long epoch, long curr) {
		Map<Integer, Long> perChannel = latest.get(epoch);
		if (perChannel == null) {
			perChannel = new HashMap<>();
			latest.put(epoch, perChannel);
		}

		perChannel.put(channel, curr);
	}

	private void enqueue(long epoch, QueueElem e) {
		PriorityQueue<QueueElem> queue = messages.get(epoch);
		if (queue == null) {
			queue = createNewQueue();
			messages.put(epoch, queue);
		}

		queue.add(e);
	}

	private boolean canBeDelivered(long epoch, QueueElem q) {
		long minLatestValueOnAllChannels = Collections.min(latest.get(epoch).values());
		return q.prev < minLatestValueOnAllChannels;
	}

	private long bias(long somevalue, int channel) {
		return 0;
	}

	private void deliver(QueueElem q, long epoch) throws Exception {
		Map<Integer, Long> epochCountPerChannel =
			epochCount.computeIfAbsent(epoch, k -> new HashMap<>());

		epochCountPerChannel.compute(q.channel, (k, v) -> (v != null) ? v + 1 : 0);

		LOG.trace("Delivering {}", q);
		if (this.hasNext()) {
			this.getNext().accept(q.m, q.channel);
		}
	}

	public static class QueueElem {
		private final StreamElement m;
		private final long prev;
		private final long curr;
		private final int channel;
		private final long epoch;

		public QueueElem(StreamElement m, long prev, long curr, long epoch, int channel) {
			this.m = m;
			this.prev = prev;
			this.curr = curr;
			this.epoch = epoch;
			this.channel = channel;
		}

		@Override
		public String toString() {
			String className = (m != null) ? m.getClass().getName() : "null";
			return String.format("(epoch: %d, prev: %d, curr: %d, channel: %d, type: %s)", epoch, prev, curr, channel, className);
		}

		public long getPrev() {
			return prev;
		}

		public int getChannel() {
			return channel;
		}

		public long getDedupTs() {
			return m.getDeduplicationTimestamp();
		}
	}
}
