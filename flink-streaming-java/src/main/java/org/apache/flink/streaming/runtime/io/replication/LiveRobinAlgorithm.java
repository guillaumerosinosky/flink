package org.apache.flink.streaming.runtime.io.replication;

import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class LiveRobinAlgorithm extends Chainable {
	private static final Logger LOG = LoggerFactory.getLogger(LiveRobinAlgorithm.class);
	
    private long highestHeartbeat = 0;
    private long currentHeartbeat = 0;
    private long[] heartbeatAtChannel;
    private int nextQueueToPoll;
    private int numProducers;

    private Map<Integer, LinkedBlockingQueue<StreamElement>> messages;

    public LiveRobinAlgorithm(int numProducers) {
        this.heartbeatAtChannel = new long[numProducers];
        for (int i = 0; i < numProducers; i++) {
            this.heartbeatAtChannel[i] = 0;
        }
        this.nextQueueToPoll = 0;
        this.numProducers = numProducers;
		this.messages = new HashMap<>();
        for (int i = 0; i < numProducers; i++) {
            this.messages.put(i, new LinkedBlockingQueue<StreamElement>());
        }
    }

	@Override
	public void accept(StreamElement element, int channel) throws Exception {
        messages.get(channel).add(element);
        processingLoop();
    }

    public void processingLoop() throws Exception {
        boolean processing = true;
        while (processing) {
            StreamElement nextElement = messages.get(this.nextQueueToPoll).poll();
            if (nextElement == null) {
                LOG.debug("No more elements to poll on channel {}", this.nextQueueToPoll);
                return;
            }
            
            if (nextElement.isEndOfEpochMarker()) { // read HB
                if (nextElement.getEpoch() > highestHeartbeat) {  // first?
                    if (this.hasNext()) { // propagate
                        LOG.debug("Heartbeat propagation (epoch {})", this.currentHeartbeat);
                        this.getNext().accept(nextElement, this.nextQueueToPoll);
                        highestHeartbeat = nextElement.getEpoch();
                    }
                }
                heartbeatAtChannel[this.nextQueueToPoll] = nextElement.getEpoch();
                long lastProducers = this.currentHeartbeat;
                for (int i = 0; i < numProducers; i++) {
                    if (heartbeatAtChannel[i] == this.currentHeartbeat) {
                        this.nextQueueToPoll = i;
                        lastProducers--;
                    }
                }
                if (lastProducers <= 0) {
                    LOG.debug("All heartbeats {} processed, increase heartbeat to {}", this.currentHeartbeat, this.currentHeartbeat + 1);
                    this.currentHeartbeat++;
                }

            } else {
                if (this.hasNext()) {
                    LOG.debug("Processing of element (epoch {})", this.currentHeartbeat);
                    this.getNext().accept(nextElement, this.nextQueueToPoll);
                }
            }
        }            
    }

}