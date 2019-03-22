package org.apache.flink.streaming.runtime.io.replication;

import org.apache.flink.streaming.runtime.streamrecord.StreamElement;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

public class RoundRobin<T> extends Merger<T> {

	private Queue<QueueElem>[] queues;

	private final int numProducers;
	private int current = 0;

	@SuppressWarnings("unchecked")
	public RoundRobin(int numProducers, StreamElementConsumer<T> s) {
		super(s);
		this.numProducers = numProducers;
		this.queues = new Queue[numProducers];
		Arrays.setAll(this.queues, (i) -> new LinkedList<StreamElement>());
	}

	@Override
	public void receive(T e, int logicalChannel, long ingestionTimestamp) throws Exception {
		this.queues[logicalChannel].add(new QueueElem(e, logicalChannel));

		int next = next();
		if (queues[next].peek() != null) {
			QueueElem element = queues[next].poll();
			deliver(element.elem, element.channel);
		} else {
			// do nothing
		}
	}

	private int next() {
		return current++ % numProducers;
	}

	private final class QueueElem {
		T elem;
		int channel;

		public QueueElem(T elem, int channel) {
			this.elem = elem;
			this.channel = channel;
		}
	}
}
