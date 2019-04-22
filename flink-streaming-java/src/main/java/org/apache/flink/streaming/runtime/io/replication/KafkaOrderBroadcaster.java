package org.apache.flink.streaming.runtime.io.replication;

import org.apache.flink.runtime.messages.Acknowledge;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class KafkaOrderBroadcaster implements OrderBroadcaster {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaOrderBroadcaster.class);

	private final KafkaProducer<String, String> producer;
	private final String topic;

	public KafkaOrderBroadcaster(String topic) {
		this.topic = topic;
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("auto.create.topics.enable", "true");

		this.producer = new KafkaProducer<>(props);

		LOG.info("Setup kafka producer to write to topic {}", topic);
	}

	@Override
	public CompletableFuture<Acknowledge> broadcast(List<Integer> nextOrder) throws ExecutionException, InterruptedException {
		LOG.info("Trying to broadcast order {} via kafka", nextOrder);
		ProducerRecord<String, String> record = new ProducerRecord<>(
			topic,
			"some-key",
			nextOrder.toString()
		);
		this.producer.send(record).get();
		LOG.info("Success - Trying to broadcast order {} via kafka", nextOrder);
		return CompletableFuture.completedFuture(Acknowledge.get());
	}
}
