package org.apache.flink.streaming.runtime.io.replication;

import org.apache.flink.streaming.runtime.streamrecord.StreamElement;

import java.util.Arrays;

public class Deduplication {

	private long[] latestDeduplicationTimestamp;

	public Deduplication(int numLogicalChannels) {
		latestDeduplicationTimestamp = new long[numLogicalChannels];
		Arrays.fill(latestDeduplicationTimestamp, -1);
	}

	public boolean isDuplicate(StreamElement elem, int channel) {

		long deduplicationTimestamp = elem.getDeduplicationTimestamp();

		if (deduplicationTimestamp == -1) {
			throw new RuntimeException("Deduplication timestamp was -1. Expected value >= 0");
		}

		boolean isDuplicate = deduplicationTimestamp <= this.latestDeduplicationTimestamp[channel];

		if (isDuplicate) {
			return true;
		} else {
			this.latestDeduplicationTimestamp[channel] = elem.getDeduplicationTimestamp();
			return false;
		}
	}
}
