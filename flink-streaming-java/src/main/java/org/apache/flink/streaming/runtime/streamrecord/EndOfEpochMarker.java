package org.apache.flink.streaming.runtime.streamrecord;

public class EndOfEpochMarker extends StreamElement {

	private long epoch;

	public EndOfEpochMarker(long epoch) {
		this.epoch = epoch;
	}

	public EndOfEpochMarker() {
	}

	public long getEpoch() {
		return epoch;
	}

	public void setEpoch(long epoch) {
		this.epoch = epoch;
	}
}
