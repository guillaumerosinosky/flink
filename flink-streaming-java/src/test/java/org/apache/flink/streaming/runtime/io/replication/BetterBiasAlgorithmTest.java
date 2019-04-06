package org.apache.flink.streaming.runtime.io.replication;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.runtime.streamrecord.BoundedDelayMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

public class BetterBiasAlgorithmTest {

	@Test
	public void acceptSingleChannelNoEpoch() throws Exception {
		BetterBiasAlgorithm b = new BetterBiasAlgorithm(2);
		CollectionChainable collector = new CollectionChainable();
		b.setNext(collector);

		b.accept(createStreamElement(0), 0);
		b.accept(createStreamElement(1), 0);
		b.accept(createStreamElement(2), 0);
		b.accept(createStreamElement(3), 0);
		b.accept(createStreamElement(4), 0);

		// list should be empty
		Assert.assertEquals(Lists.newArrayList(), collector.getElements());
	}

	@Test
	public void acceptTwoChannelsNoEpoch() throws Exception {
		BetterBiasAlgorithm b = new BetterBiasAlgorithm(2);
		CollectionChainable collector = new CollectionChainable();
		b.setNext(collector);

		StreamElement s0 = createStreamElement(0);
		b.accept(s0, 0);
		StreamElement s1 = createStreamElement(0);
		b.accept(s1, 1);

		StreamElement s2 = createStreamElement(1);
		b.accept(s2, 0);

		StreamElement s3 = createStreamElement(1);
		b.accept(s3, 1);

		// list should be empty
		Assert.assertEquals(Lists.newArrayList(
			Tuple2.of(s0, 0),
			Tuple2.of(s1, 1)
		), collector.getElements());
	}

	@Test
	public void acceptOneChannelEpochOnBothChannels() throws Exception {
		BetterBiasAlgorithm b = new BetterBiasAlgorithm(2);
		CollectionChainable collector = new CollectionChainable();
		b.setNext(collector);

		StreamElement s0 = createStreamElement(0);
		b.accept(s0, 0);
		StreamElement s1 = createStreamElement(0);
		b.accept(s1, 0);

		StreamElement e1 = createEndOfEpoch(0, 0);
		b.accept(e1, 0); // end of epoch with ts 0 and epoch 0 on channel 0
		StreamElement e2 = createEndOfEpoch(0, 0);
		b.accept(e2, 1); // end of epoch with ts 0 and epoch 0 on channel 1

		// list should be empty
		Assert.assertEquals(Lists.newArrayList(
			Tuple2.of(s0, 0),
			Tuple2.of(s1, 0),
			Tuple2.of(e1, 0),
			Tuple2.of(e2, 1)
		), collector.getElements());
	}

	@Test
	public void acceptTwoChannelsEpochOnBothChannels() throws Exception {
		BetterBiasAlgorithm b = new BetterBiasAlgorithm(2);
		CollectionChainable collector = new CollectionChainable();
		b.setNext(collector);

		StreamElement s0 = createStreamElement(0); b.accept(s0, 0);
		StreamElement s5 = createStreamElement(0); b.accept(s5, 1);
		StreamElement s1 = createStreamElement(1); b.accept(s1, 0);
		StreamElement s2 = createStreamElement(2); b.accept(s2, 0);
		StreamElement s6 = createStreamElement(2); b.accept(s6, 1);
		StreamElement s3 = createStreamElement(3); b.accept(s3, 0);
		StreamElement s4 = createStreamElement(4); b.accept(s4, 0);
		StreamElement s7 = createStreamElement(4); b.accept(s7, 1);
		StreamElement s8 = createStreamElement(6); b.accept(s8, 1);


		StreamElement e1 = createEndOfEpoch(0, 0);
		b.accept(e1, 0); // end of epoch with ts 0 and epoch 0 on channel 0
		StreamElement e2 = createEndOfEpoch(0, 0);
		b.accept(e2, 1); // end of epoch with ts 0 and epoch 0 on channel 1

		// list should be empty
		Assert.assertEquals(Lists.newArrayList(
			Tuple2.of(s0, 0),
			Tuple2.of(s5, 1),
			Tuple2.of(s1, 0),
			Tuple2.of(s2, 0),
			Tuple2.of(s6, 1),
			Tuple2.of(s3, 0),
			Tuple2.of(s4, 0),
			Tuple2.of(s7, 1),
			Tuple2.of(s8, 1),
			Tuple2.of(e1, 0),
			Tuple2.of(e2, 1)
		), collector.getElements());
	}

	@Test
	public void acceptTwoChannelsTwoEpochsOnBothChannels() throws Exception {
		BetterBiasAlgorithm b = new BetterBiasAlgorithm(2);
		CollectionChainable collector = new CollectionChainable();
		b.setNext(collector);

		StreamElement s0 = createStreamElement(0); b.accept(s0, 0);
		StreamElement s5 = createStreamElement(0); b.accept(s5, 1);
		StreamElement s1 = createStreamElement(1); b.accept(s1, 0);
		StreamElement s2 = createStreamElement(2); b.accept(s2, 0);
		StreamElement s6 = createStreamElement(2); b.accept(s6, 1);
		StreamElement s3 = createStreamElement(3); b.accept(s3, 0);
		StreamElement s4 = createStreamElement(4); b.accept(s4, 0);
		StreamElement s7 = createStreamElement(4); b.accept(s7, 1);
		StreamElement s8 = createStreamElement(6); b.accept(s8, 1);


		StreamElement e1 = createEndOfEpoch(10, 0);
		b.accept(e1, 0); // end of epoch with ts 0 and epoch 0 on channel 0
		StreamElement e2 = createEndOfEpoch(10, 0);
		b.accept(e2, 1); // end of epoch with ts 0 and epoch 0 on channel 1

		// list should be empty
		Assert.assertEquals(Lists.newArrayList(
			Tuple2.of(s0, 0),
			Tuple2.of(s5, 1),
			Tuple2.of(s1, 0),
			Tuple2.of(s2, 0),
			Tuple2.of(s6, 1),
			Tuple2.of(s3, 0),
			Tuple2.of(s4, 0),
			Tuple2.of(s7, 1),
			Tuple2.of(s8, 1),
			Tuple2.of(e1, 0),
			Tuple2.of(e2, 1)
		), collector.getElements());

		collector.clear();

		StreamElement s9 = createStreamElement(10); b.accept(s9, 0);
		StreamElement s10 = createStreamElement(10); b.accept(s10, 1);
		StreamElement s11 = createStreamElement(11); b.accept(s11,1);
		StreamElement s12 = createStreamElement(12); b.accept(s12,1);
		StreamElement s13 = createStreamElement(12); b.accept(s13,0);
		StreamElement s14 = createStreamElement(13); b.accept(s14,1);

		StreamElement e4 = createEndOfEpoch(20, 1); b.accept(e4, 1); // end of epoch with ts 0 and epoch 0 on channel 1

		StreamElement s15 = createStreamElement(14); b.accept(s15,0);
		StreamElement s16 = createStreamElement(14); b.accept(s16,0);
		StreamElement s17 = createStreamElement(16); b.accept(s17,0);

		StreamElement e3 = createEndOfEpoch(21, 1); b.accept(e3, 0); // end of epoch with ts 0 and epoch 0 on channel 0

		// list should be empty
		Assert.assertEquals(Lists.newArrayList(
			Tuple2.of(s9, 0),
			Tuple2.of(s10, 1),
			Tuple2.of(s11, 1),
			Tuple2.of(s13, 0),
			Tuple2.of(s12, 1),
			Tuple2.of(s14, 1),
			Tuple2.of(s15, 0),
			Tuple2.of(s16, 0),
			Tuple2.of(s17, 0),
			Tuple2.of(e3, 0),
			Tuple2.of(e4, 1)
		), collector.getElements());
	}

	@Test
	public void acceptTestBias() throws Exception {
		BetterBiasAlgorithm b = new BetterBiasAlgorithm(2);
		CollectionChainable collector = new CollectionChainable();
		b.setNext(collector);

		for (int i = 0; i < 1000; i++) {
			b.accept(createStreamElement(i), 0);
		}

		b.accept(createStreamElement(0), 1);
	}

	private StreamElement createStreamElement(long timestamp) {
		StreamElement element = new StreamElement() {
			@Override
			public String toString() {
				return "StreamElement@" + timestamp;
			}
		};
		element.setSentTimestamp(timestamp);
		return element;
	}

	private StreamElement createEndOfEpoch(long timestamp, long epoch) {
		BoundedDelayMarker m = new BoundedDelayMarker();
		m.setEpoch(epoch);
		m.setCreatedAt(timestamp);
		return m;
	}

}
