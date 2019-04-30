package org.apache.flink.streaming.runtime.io.replication;

import org.apache.flink.streaming.runtime.streamrecord.StreamElement;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.CancelLeadershipException;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class KafkaReplication extends Chainable implements LeaderSelectorListener {

	private static final Logger LOG = LoggerFactory.getLogger(LeaderBasedReplication.class);
	private final int batchSize;

	private final BlockingQueue<Integer> nextCandidates;
	private final BlockingQueue<Enqueued>[] queues;
	private final BlockingQueue<Integer> nextChannels;

	private final OrderBroadcaster broadcaster;

	private final String owningTaskName;
	private final KafkaConsumer<String, String> consumer;

	private volatile boolean hasFailed;
	private volatile boolean stopProcessor;
	private List<AutoCloseable> closables = new LinkedList<>();

	@SuppressWarnings("unchecked")
	public KafkaReplication(
		int numProducers,
		OrderBroadcaster broadcaster,
		int batchSize,
		String topic,
		CuratorFramework f
	) {
		this.broadcaster = broadcaster;
		this.batchSize = batchSize;

		this.queues = new BlockingQueue[numProducers];

		// TODO: Thesis - This has a capacity of Integer.MAX_VALUE and is not unbounded!
		this.nextChannels = new LinkedBlockingQueue<>();
		this.nextCandidates = new LinkedBlockingQueue<>();

		for (int i = 0; i < numProducers; i++) {
			this.queues[i] = new LinkedBlockingQueue<>();
		}

		this.owningTaskName = Thread.currentThread().getName();

		this.hasFailed = false;
		this.stopProcessor = false;

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", UUID.randomUUID().toString());
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		this.consumer = new KafkaConsumer<>(props);
		this.consumer.subscribe(Lists.newArrayList(topic));

		LOG.info("Setup kafka consumer to read from topic {}", topic);

		Thread processor = new Thread(this::processInput, "active-replication-" + owningTaskName);
		processor.start();

		LeaderSelector leaderSelector = new LeaderSelector(f, "/flink/" + topic, this);
		leaderSelector.autoRequeue();
		leaderSelector.start();
	}


	@Override
	public void accept(StreamElement element, int channel) throws Exception {
		checkFailure();
		LOG.info("At {}: Adding element to queue", owningTaskName);
		Enqueued e = new Enqueued(element.getCurrentTs(), channel, element);
		this.queues[channel].put(e);
		this.nextCandidates.put(channel);
		LOG.info("At {}: done adding element to queue", owningTaskName);
	}

	private void processInput() {
		hasFailed = false;
		while (!hasFailed || !stopProcessor) {
			System.out.println("Polling");
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> record : records) {
				String next = record.value();
				int[] nextChans = Arrays.stream(next.split(","))
					.mapToInt(Integer::valueOf)
					.toArray();

				LOG.info("At {}: Received order from kafka {}", owningTaskName, Arrays.toString(nextChans));

				for (int chan : nextChans) {

					try {
						Enqueued elem = queues[chan].take();
						if (hasNext()) {
							this.getNext().accept(elem.value, elem.channel);
						}
					} catch (Exception e) {
						LOG.error("Input processor failed with exception {}", e);
						hasFailed = true;
					}
				}
			}
		}
	}

	private void checkFailure() {
		if (this.hasFailed) {
			throw new RuntimeException("We have failed on the leader thread");
		}
	}

	public void acceptOrdering(List<Integer> nextBatch) throws Exception {
		checkFailure();
		LOG.info("At {}: Accepting ordering {}", owningTaskName, nextBatch);
		this.nextChannels.addAll(nextBatch);
	}

	@Override
	public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
		LOG.info("At {}: I became the leader", owningTaskName);

		String previousName = Thread.currentThread().getName();
		Thread.currentThread().setName("leader-for-" + owningTaskName);
		try {
			broadcastOrder();
		} catch (Throwable t) {
			LOG.warn("Lost leadership because of {}", owningTaskName, t);
			this.hasFailed = true;
			Thread.currentThread().setName(previousName);
			throw t;
		}
	}

	@SuppressWarnings({"Duplicates"})
	private void broadcastOrder() throws InterruptedException, java.util.concurrent.ExecutionException {
		while (true) {
			List<Integer> next = new ArrayList<>(batchSize);

			boolean timeout = false;

			while (next.size() < batchSize && !timeout) {
				Integer n = this.nextCandidates.poll(1000, TimeUnit.MILLISECONDS);
				if (n != null) {
					next.add(n);
				} else {
					timeout = true;
				}
			}

			if (next.size() > 0) {
				broadcaster.broadcast(next).get();
			} else {
				LOG.info("No next candidates to broadcast");
			}
		}
	}

	@Override
	public void stateChanged(CuratorFramework client, ConnectionState newState) {
		if (newState == ConnectionState.SUSPENDED || newState == ConnectionState.LOST) {
			LOG.error("At {}: State changed to {}", owningTaskName, newState);
			throw new CancelLeadershipException();
		}
	}

	@Override
	public void close() throws Exception {
		this.stopProcessor = true;
		for (AutoCloseable closable : this.closables) {
			closable.close();
		}
	}

	public void addCloseables(AutoCloseable... c) {
		this.closables.addAll(Lists.newArrayList(c));
	}
}
