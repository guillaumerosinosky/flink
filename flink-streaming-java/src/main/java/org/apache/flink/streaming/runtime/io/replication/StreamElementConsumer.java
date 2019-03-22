package org.apache.flink.streaming.runtime.io.replication;

import org.apache.flink.streaming.runtime.streamrecord.StreamElement;

public interface StreamElementConsumer<T> {
	void accept(T element, int channel) throws Exception;
}
