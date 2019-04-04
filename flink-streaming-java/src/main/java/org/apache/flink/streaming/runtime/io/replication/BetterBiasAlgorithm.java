package org.apache.flink.streaming.runtime.io.replication;

import org.apache.flink.streaming.runtime.streamrecord.StreamElement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class BetterBiasAlgorithm extends Chainable {

	private static final Logger LOG = LoggerFactory.getLogger(BetterBiasAlgorithm.class);

	private Queue<QueueElem>[] queues;

	private long[] epochStart;
	private long[] epochCount;
	private long[] currentEpoch;

	private Map<Long, Long>[] bias;

	public BetterBiasAlgorithm(int numProducers) {
		queues = new Queue[numProducers];
		bias = new Map[numProducers];

		epochStart = new long[numProducers];
		epochCount = new long[numProducers];
		currentEpoch = new long[numProducers];

		for (int i = 0; i < numProducers; i++) {
			queues[i] = new LinkedList<>();
			epochStart[i] = 0;
			epochCount[i] = 0;
			currentEpoch[i] = 0;
			bias[i] = new HashMap<>();
		}
	}

	@Override
	public void accept(StreamElement element, int channel) throws Exception {

		if (element.isBoundedDelayMarker()) {
			long epoch = element.asBoundedDelayMarker().getEpoch();
			long createdAt = element.asBoundedDelayMarker().getCreatedAt();
			newEpochOnChannel(channel, epoch, createdAt);
		}

		queues[channel].add(new QueueElem(element, element.getSentTimestamp(), channel, currentEpoch[channel]));

		while (true) {
			long minTs = -1;
			int minIdx = -1;

			boolean cannotMakeProgress = false;
			for (int i = 0; i < queues.length; i++) {
				QueueElem elem = queues[i].peek();
				if (elem == null) {
					// we cannot make progress
					// unless we have seen at least
					// one element on each channel
					cannotMakeProgress = true;
					break;
				} else if ((minTs == -1 && minIdx == -1) || (elem.timestamp + bias(elem.channel, elem.epoch)) < minTs) {
					minTs = elem.timestamp;
					minIdx = i;
				}
			}

			if (cannotMakeProgress) {
				break;
			} else {
				QueueElem toBeDelivered = queues[minIdx].poll();
				deliver(toBeDelivered);
			}
		}
	}

	private long bias(int channel, long epoch) {
		return 0;
	}

	private void newEpochOnChannel(int channel, long newEpoch, long newEpochStart) {

		currentEpoch[channel] = newEpoch;

		if (epochStart[channel] == 0) {
			epochStart[channel] = newEpochStart;
			return;
		}

		long duration = newEpochStart - epochStart[channel];
		long count = epochCount[channel];

		epochStart[channel] = newEpochStart;
		epochCount[channel] = 0;
	}

	private void deliver(QueueElem q) throws Exception {
		epochCount[q.channel]++;

		if (this.hasNext()) {
			this.getNext().accept(q.value, q.channel);
		}
	}

	private class QueueElem {
		StreamElement value;
		long timestamp;
		int channel;
		long epoch;

		public QueueElem(StreamElement value, long timestamp, int channel, long epoch) {
			this.value = value;
			this.timestamp = timestamp;
			this.channel = channel;
			this.epoch = epoch;
		}
	}
}
