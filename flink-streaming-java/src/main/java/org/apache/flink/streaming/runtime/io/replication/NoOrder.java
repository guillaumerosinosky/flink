package org.apache.flink.streaming.runtime.io.replication;

public class NoOrder<T> extends Merger<T> {
	public NoOrder(StreamElementConsumer s) {
		super(s);
	}

	@Override
	public void receive(T e, int logicalChannel, long ingestionTimestamp) throws Exception {
		deliver(e, logicalChannel);
	}
}
