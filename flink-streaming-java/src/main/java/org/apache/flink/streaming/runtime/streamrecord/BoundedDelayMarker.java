package org.apache.flink.streaming.runtime.streamrecord;

public class BoundedDelayMarker extends StreamElement {

	public long createdAt;
	private long epoch;

	public BoundedDelayMarker(long createdAt, long epoch) {
		this.createdAt = createdAt;
		this.epoch = epoch;
	}

	public BoundedDelayMarker() {
	}

	public long getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(long createdAt) {
		this.createdAt = createdAt;
	}

	public long getEpoch() {
		return epoch;
	}

	public void setEpoch(long epoch) {
		this.epoch = epoch;
	}
}
