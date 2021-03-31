package org.apache.flink.streaming.runtime.io.replication;

import org.apache.flink.streaming.runtime.streamrecord.EndOfEpochMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

public class LiveRobinAlgorithm extends Chainable {
	private static final Logger LOG = LoggerFactory.getLogger(LiveRobinAlgorithm.class);
	
    private long highestHeartbeat = 0;
    private long currentHeartbeat = 0;
    private long[] heartbeatAtChannel;
    private long nextQueueToPoll;
    private int numProducers;

    public LiveRobinAlgorithm(int numProducers) {
        this.heartbeatAtChannel = new long[numProducers];
        for (int i = 0; i < numProducers; i++) {
            this.heartbeatAtChannel[i] = 0;
        }
        this.nextQueueToPoll = 0;
        this.numProducers = numProducers;
	}


	@Override
	public void accept(StreamElement element, int channel) throws Exception {
        if (element.isEndOfEpochMarker()) { // read HB
            if (element.getEpoch() > highestHeartbeat) {  // first?
                if (this.hasNext()) { // propagate
                    this.getNext().accept(element, channel);
                    highestHeartbeat = element.getEpoch();
                }
            }
            heartbeatAtChannel[channel] = element.getEpoch();
            long lastProducers = this.currentHeartbeat;
            for (int i = 0; i < numProducers; i++) {
                if (heartbeatAtChannel[i] == this.currentHeartbeat) {
                    this.nextQueueToPoll = i;
                    lastProducers--;
                }
            }
            if (lastProducers <= 0) {
                this.currentHeartbeat++;
            }

        } else {
            if (channel == this.nextQueueToPoll) {
                this.getNext().accept(element, channel);
            }
        }
    }

}