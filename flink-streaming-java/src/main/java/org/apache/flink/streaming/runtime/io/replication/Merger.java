package org.apache.flink.streaming.runtime.io.replication;

import org.apache.flink.streaming.runtime.streamrecord.StreamElement;

public abstract class Merger<T> {

	private final StreamElementConsumer orderingService;

	public Merger(StreamElementConsumer s) {
		orderingService = s;
	}

	public final void deliver(T element, int logicalChannel) throws Exception {
		this.orderingService.accept(element, logicalChannel);
	}

	public abstract void receive(T e, int logicalChannel, long ingestionTimestamp) throws Exception;

	public void endOfStream() throws Exception {
		// empty on purpose
		// implementations can react to end of stream
		// e.g. to flush all elements remaining in buffers
	}
}
