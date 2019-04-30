package org.apache.flink.streaming.runtime.io.replication;

import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.Test;

import java.util.Random;

public class KafkaReplicationTest {

	private Random r = new Random();

	@Test
	public void test() throws Exception {
		String topic = "topic-for-operator-" + r.nextInt(1000);
		KafkaOrderBroadcaster b = new KafkaOrderBroadcaster(topic);
		KafkaReplication r = new KafkaReplication(2, b, 1, topic, f);
		KafkaReplication r2 = new KafkaReplication(2, b, 1, topic, f);


		r.setNext(new Chainable() {
			@Override
			public void accept(StreamElement element, int channel) throws Exception {
				System.out.println("r1: Delivered " + element + " at channel " + channel);
			}
		});

		r2.setNext(new Chainable() {
			@Override
			public void accept(StreamElement element, int channel) throws Exception {
				System.out.println("r2: Delivered " + element + " at channel " + channel);
			}
		});

		r.accept(new StreamRecord<>("hello"), 0);
		r.accept(new StreamRecord<>("world"), 1);
		r2.accept(new StreamRecord<>("hello"), 0);
		r2.accept(new StreamRecord<>("world"), 1);

		r.takeLeadership(null);
	}

}
