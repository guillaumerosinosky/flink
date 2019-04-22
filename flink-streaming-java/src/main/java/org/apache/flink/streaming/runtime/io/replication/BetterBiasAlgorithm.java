package org.apache.flink.streaming.runtime.io.replication;

import org.apache.flink.streaming.runtime.streamrecord.EndOfEpochMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

public class BetterBiasAlgorithm extends Chainable {

	private static final Logger LOG = LoggerFactory.getLogger(BetterBiasAlgorithm.class);

	private long currentEpoch = 0;

	private Map<Long, PriorityQueue<QueueElem>> messages;

	private long[] currentEpochAtChannel;
	private final int numProducers;

	// contains the latest timestamp per epoch per channel
	private Map<Long, Long[]> latest;

	public BetterBiasAlgorithm(int numProducers) {
		this.currentEpochAtChannel = new long[numProducers];
		this.latest = new HashMap<>();
		this.numProducers = numProducers;

		HashMap<Integer, Long> value = new HashMap<>();
		this.latest.put(0L, new Long[numProducers]);
		for (int i = 0; i < numProducers; i++) {
			this.latest.get(0L)[i] = 0L;
			this.currentEpochAtChannel[i] = 0;
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
		epoch = currentEpochAtChannel[channel];
		curr = element.getCurrentTs(); //  - epochStart[channel];
		prev = element.getPreviousTs(); // - epochStart[channel];

		LOG.trace("Received element curr {} prev {} epoch {} chan {} delayMarker? {}", curr, prev, epoch, channel, element.isBoundedDelayMarker());

		if (element.getCurrentTs() == -1 || element.getPreviousTs() == -1) {
			throw new RuntimeException("sentTs not properly initialized!");
		}

		if (!element.isBoundedDelayMarker()) {
			Preconditions.checkState(curr > prev, "curr ({}) needs to be bigger than prev ({}) for deterministic ordering", curr, prev);
		}

//		curr += bias(curr, channel);
//		prev += bias(prev, channel);

		QueueElem e = new QueueElem(element, prev, curr, epoch, channel);
		enqueue(epoch, e);
		updateLatestInEpoch(channel, epoch, curr);

		LOG.trace("Updated latest for epoch {} to: {} ", epoch, Arrays.toString(this.latest.get(epoch)));

		if (epoch == currentEpoch) {
			boolean done = false;
			while (!done) {
				QueueElem q = messages.get(epoch).peek();

				if (q != null && canBeDelivered(epoch, q)) {
					if (q.m instanceof EndOfEpochMarker) {
						throw new RuntimeException("EndOfEpochMarker should never be delivered here!");
					}
					this.messages.get(epoch).poll();
					deliver(q);
				} else {
					done = true;
				}
			}
		}

		if (element.isBoundedDelayMarker()) {
			if (element.asBoundedDelayMarker().getEpoch() == -1) {
				throw new RuntimeException("Epoch not initialized properly!");
			}
			long newEpoch = element.asBoundedDelayMarker().getEpoch() + 1;

			if (newEpoch != currentEpochAtChannel[channel] + 1) {
				throw new RuntimeException("new epoch " + newEpoch + " != epoch + 1: " + (epoch + 1));
			}

			currentEpochAtChannel[channel] = newEpoch;

			Map<Integer, Long> m = new HashMap<>();
			for (int i = 0; i < numProducers; i++) {
				m.put(i, 0L);
			}

			if (latest.get(newEpoch) == null) {
				Long[] lastest = new Long[numProducers];
				Arrays.fill(lastest, 0L);
				latest.put(newEpoch, lastest);
			}
		}

		if (newEpochHasStarted()) { // we have seen epoch messages for both channels

			// do everything that needs the current epoch to still be the old one here
			for (int i = 0; i < numProducers; i++) {
				QueueElem q = this.messages.get(currentEpoch).poll();
				if (q == null) {
					throw new RuntimeException("There should still be numProducer EndOfEpochMarkers in the queue");
				} else if (!(q.m instanceof EndOfEpochMarker)) {
					throw new RuntimeException("Found element still in queue while flushing epoch " + currentEpoch + " that is not an EndOfEpochMarker: " + q);
				} else {
					// everything is fine
				}
			}

			if (this.messages.get(currentEpoch).size() > 0) {
				throw new RuntimeException("Queue should be empty after EndOfEpochMarkers are delivered");
			}

			EndOfEpochMarker marker = new EndOfEpochMarker();
			marker.setEpoch(currentEpoch);
			marker.setPreviousTimestamp(Long.MAX_VALUE);
			marker.setCurrentTimestamp(Long.MAX_VALUE);
			QueueElem q = new QueueElem(marker, Long.MAX_VALUE, Long.MAX_VALUE, currentEpoch, -1);
			deliver(q);

			// update the current epoch and initialize datastructures for new epoch
			currentEpoch += 1;
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
		long prev = latest.get(epoch)[channel];
		if (prev > curr) {
			throw new RuntimeException("Prev > curr!! " + prev + " > " + curr);
		}
		latest.get(epoch)[channel] = curr;
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
		long min = Arrays.stream(latest.get(epoch)).min(Long::compare).get();
		return q.prev < min;
	}

	private long bias(long somevalue, int channel) {
		return 0;
	}

	private void deliver(QueueElem q) throws Exception {
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
