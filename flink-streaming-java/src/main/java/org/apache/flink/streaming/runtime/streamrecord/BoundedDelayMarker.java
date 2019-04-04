package org.apache.flink.streaming.runtime.streamrecord;

public class BoundedDelayMarker extends StreamElement {

	public BoundedDelayMarker(long createdAt) {
		this.createdAt = createdAt;
	}

	public long createdAt;

	public long getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(long createdAt) {
		this.createdAt = createdAt;
	}
}
