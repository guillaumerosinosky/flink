package org.apache.flink.streaming.runtime.streamrecord;

public class BoundedDelayMarker extends StreamElement {

	private long epoch;

	public BoundedDelayMarker(long epoch) {
		this.epoch = epoch;
	}

	public BoundedDelayMarker() {
	}

	public long getEpoch() {
		return epoch;
	}

	public void setEpoch(long epoch) {
		this.epoch = epoch;
	}
}
