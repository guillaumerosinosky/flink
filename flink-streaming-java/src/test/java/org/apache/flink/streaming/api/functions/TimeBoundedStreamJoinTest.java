package org.apache.flink.streaming.api.functions;

import com.google.common.collect.Iterables;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class TimeBoundedStreamJoinTest {

//	@Test
	public void continousInput() throws Exception {

		long lowerBound = -2;
		boolean lowerBoundInclusive = true;

		long upperBound = -1;
		boolean upperBoundInclusive = true;

		KeyedTwoInputStreamOperatorTestHarness<
			String,
			TestElem,
			TestElem,
			Tuple2<TestElem, TestElem>> testHarness
			= createTestHarness(lowerBound, lowerBoundInclusive, upperBound, upperBoundInclusive);

		// TODO: Add setup
		// WindowOperatorTest
		testHarness.open();

		ScheduledExecutorService service = Executors.newScheduledThreadPool(1);

		final AtomicInteger count = new AtomicInteger();

		Future f = service.scheduleAtFixedRate(() -> {
			try {
				testHarness.processElement1(createStreamRecord(count.get(), "lhs"));
				testHarness.processWatermark1(new Watermark(count.get()));

				testHarness.processElement2(createStreamRecord(count.get(), "rhs"));
				testHarness.processWatermark2(new Watermark(count.get()));

				count.getAndIncrement();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}, 0, 1, TimeUnit.SECONDS);

		f.get();
	}


	private final boolean lhsFasterThanRhs;

	@Parameters(name = "lhs faster than rhs stream: {0}")
	public static Boolean[] data() {
		return new Boolean[]{true, false};
	}

	public TimeBoundedStreamJoinTest(boolean lhsFasterThanRhs) {
		this.lhsFasterThanRhs = lhsFasterThanRhs;
	}

	@Test // lhs - 2 <= rhs <= rhs + 2
	public void testNegativeInclusiveAndNegativeInclusive() throws Exception {

		long lowerBound = -2;
		boolean lowerBoundInclusive = true;

		long upperBound = -1;
		boolean upperBoundInclusive = true;

		KeyedTwoInputStreamOperatorTestHarness<
			String,
			TestElem,
			TestElem,
			Tuple2<TestElem, TestElem>> testHarness
			= createTestHarness(lowerBound, lowerBoundInclusive, upperBound, upperBoundInclusive);

		testHarness.open();

		prepareTestHarness(testHarness);

		List<StreamRecord<Tuple2<TestElem, TestElem>>> expectedOutput = Lists.newArrayList(
			streamRecordOf(2, 1),

			streamRecordOf(3, 1),
			streamRecordOf(3, 2),

			streamRecordOf(4, 2),
			streamRecordOf(4, 3)
		);

		ConcurrentLinkedQueue<Object> output = testHarness.getOutput();

		validateStreamRecords(expectedOutput, testHarness.getOutput());
		ensureNoLateData(output);

		testHarness.close();
	}

	@Test // lhs - 1 <= rhs <= rhs + 1
	public void testNegativeInclusiveAndPositiveInclusive() throws Exception {

		long lowerBound = -1;
		boolean lowerBoundInclusive = true;

		long upperBound = 1;
		boolean upperBoundInclusive = true;

		KeyedTwoInputStreamOperatorTestHarness<
			String,
			TestElem,
			TestElem,
			Tuple2<TestElem, TestElem>> testHarness
			= createTestHarness(lowerBound, lowerBoundInclusive, upperBound, upperBoundInclusive);

		testHarness.open();

		prepareTestHarness(testHarness);

		List<StreamRecord<Tuple2<TestElem, TestElem>>> expectedOutput = Lists.newArrayList(
			streamRecordOf(1, 1),
			streamRecordOf(1, 2),

			streamRecordOf(2, 1),
			streamRecordOf(2, 2),
			streamRecordOf(2, 3),

			streamRecordOf(3, 2),
			streamRecordOf(3, 3),
			streamRecordOf(3, 4),

			streamRecordOf(4, 3),
			streamRecordOf(4, 4)
		);

		ConcurrentLinkedQueue<Object> output = testHarness.getOutput();

		validateStreamRecords(expectedOutput, testHarness.getOutput());
		ensureNoLateData(output);

		testHarness.close();
	}

	@Test // lhs + 1 <= rhs <= lhs + 2
	public void testPositiveInclusiveAndPositiveInclusive() throws Exception {
		long lowerBound = 1;
		long upperBound = 2;

		boolean lowerBoundInclusive = true;
		boolean upperBoundInclusive = true;

		KeyedTwoInputStreamOperatorTestHarness<
			String,
			TestElem,
			TestElem,
			Tuple2<TestElem, TestElem>> testHarness
			= createTestHarness(lowerBound, lowerBoundInclusive, upperBound, upperBoundInclusive);

		testHarness.open();

		prepareTestHarness(testHarness);

		List<StreamRecord<Tuple2<TestElem, TestElem>>> expected = Lists.newArrayList(
			streamRecordOf(1, 2),
			streamRecordOf(1, 3),
			streamRecordOf(2, 3),
			streamRecordOf(2, 4),
			streamRecordOf(3, 4)
		);

		validateStreamRecords(expected, testHarness.getOutput());
		ensureNoLateData(testHarness.getOutput());

		testHarness.close();
	}

	@Test
	public void testNegativeExclusiveAndNegativeExlusive() throws Exception {
		long lowerBound = -3;
		boolean lowerBoundInclusive = false;

		long upperBound = -1;
		boolean upperBoundInclusive = false;

		KeyedTwoInputStreamOperatorTestHarness<
			String,
			TestElem,
			TestElem,
			Tuple2<TestElem, TestElem>> testHarness
			= createTestHarness(lowerBound, lowerBoundInclusive, upperBound, upperBoundInclusive);

		testHarness.open();
		prepareTestHarness(testHarness);

		List<StreamRecord<Tuple2<TestElem, TestElem>>> expectedOutput = Lists.newArrayList(
			streamRecordOf(3, 1),
			streamRecordOf(4, 2)
		);

		ConcurrentLinkedQueue<Object> output = testHarness.getOutput();

		validateStreamRecords(expectedOutput, testHarness.getOutput());
		ensureNoLateData(output);

		testHarness.close();
	}

	@Test
	public void testNegativeExclusiveAndPositiveExlusive() throws Exception {
		long lowerBound = -1;
		boolean lowerBoundInclusive = false;

		long upperBound = 1;
		boolean upperBoundInclusive = false;

		KeyedTwoInputStreamOperatorTestHarness<
			String,
			TestElem,
			TestElem,
			Tuple2<TestElem, TestElem>> testHarness
			= createTestHarness(lowerBound, lowerBoundInclusive, upperBound, upperBoundInclusive);

		testHarness.open();
		prepareTestHarness(testHarness);

		List<StreamRecord<Tuple2<TestElem, TestElem>>> expectedOutput = Lists.newArrayList(
			streamRecordOf(1, 1),
			streamRecordOf(2, 2),
			streamRecordOf(3, 3),
			streamRecordOf(4, 4)
		);

		ConcurrentLinkedQueue<Object> output = testHarness.getOutput();

		validateStreamRecords(expectedOutput, testHarness.getOutput());
		ensureNoLateData(output);

		testHarness.close();
	}

	@Test
	public void testPositiveExclusiveAndPositiveExlusive() throws Exception {
		long lowerBound = 1;
		boolean lowerBoundInclusive = false;

		long upperBound = 3;
		boolean upperBoundInclusive = false;

		KeyedTwoInputStreamOperatorTestHarness<
			String,
			TestElem,
			TestElem,
			Tuple2<TestElem, TestElem>> testHarness
			= createTestHarness(lowerBound, lowerBoundInclusive, upperBound, upperBoundInclusive);

		testHarness.open();
		prepareTestHarness(testHarness);

		List<StreamRecord<Tuple2<TestElem, TestElem>>> expectedOutput = Lists.newArrayList(
			streamRecordOf(1, 3),
			streamRecordOf(2, 4)
		);

		ConcurrentLinkedQueue<Object> output = testHarness.getOutput();

		validateStreamRecords(expectedOutput, testHarness.getOutput());
		ensureNoLateData(output);

		testHarness.close();
	}

	private void validateStreamRecords(
		Iterable<StreamRecord<Tuple2<TestElem, TestElem>>> expectedOutput,
		Queue<Object> actualOutput) {

		int actualSize = actualOutput.stream()
			.filter(elem -> elem instanceof StreamRecord)
			.collect(Collectors.toList())
			.size();

		int expectedSize = Iterables.size(expectedOutput);

		Assert.assertEquals(
			"Expected and actual size of stream records different",
			expectedSize,
			actualSize
		);

		for (StreamRecord<Tuple2<TestElem, TestElem>> record : expectedOutput) {
			Assert.assertTrue(actualOutput.contains(record));
		}
	}

	private void ensureNoLateData(Iterable<Object> output) throws Exception {
		// check that no watermark is violated
		long highestWatermark = Long.MIN_VALUE;

		for (Object elem : output) {
			if (elem instanceof Watermark) {
				highestWatermark = ((Watermark) elem).asWatermark().getTimestamp();
			} else if (elem instanceof StreamRecord) {
				boolean dataIsOnTime = highestWatermark < ((StreamRecord) elem).getTimestamp();
				// TODO: Wording
				Assert.assertTrue("Late data was emitted after join", dataIsOnTime);
			} else {
				// TODO: What to do here?
				throw new Exception("Unexpected conditions");
			}
		}
	}

	private KeyedTwoInputStreamOperatorTestHarness<
		String,
		TestElem,
		TestElem,
		Tuple2<TestElem, TestElem>> createTestHarness(long lowerBound,
													  boolean lowerBoundInclusive,
													  long upperBound,
													  boolean upperBoundInclusive) throws Exception {

		TimeBoundedStreamJoin<TestElem, TestElem> joinFunc = new TimeBoundedStreamJoin<>(
			lowerBound,
			upperBound,
			lowerBoundInclusive,
			upperBoundInclusive
		);

		long delay = joinFunc.getWatermarkDelay();

		KeyedCoProcessOperatorWithWatermarkDelay<
			String,
			TestElem,
			TestElem,
			Tuple2<TestElem, TestElem>> operator
			= new KeyedCoProcessOperatorWithWatermarkDelay<>(joinFunc, delay);

		return new KeyedTwoInputStreamOperatorTestHarness<>(
			operator,
			(elem) -> elem.key, // key
			(elem) -> elem.key, // key
			TypeInformation.of(String.class)
		);
	}

	private StreamRecord<Tuple2<TestElem, TestElem>> streamRecordOf(long lhsTs,
																	long rhsTs) {
		TestElem lhs = new TestElem(lhsTs, "lhs");
		TestElem rhs = new TestElem(rhsTs, "rhs");

		// TODO: this limits the test to selection of left timestamp
		return new StreamRecord<>(Tuple2.of(lhs, rhs), lhsTs);
	}

	private static class TestElem {
		String key;
		long ts;
		String source;

		public TestElem(long ts, String source) {
			this.key = "key";
			this.ts = ts;
			this.source = source;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			TestElem testElem = (TestElem) o;

			if (ts != testElem.ts) return false;
			if (key != null ? !key.equals(testElem.key) : testElem.key != null) return false;
			return source != null ? source.equals(testElem.source) : testElem.source == null;
		}

		@Override
		public int hashCode() {
			int result = key != null ? key.hashCode() : 0;
			result = 31 * result + (int) (ts ^ (ts >>> 32));
			result = 31 * result + (source != null ? source.hashCode() : 0);
			return result;
		}

		@Override
		public String toString() {
			return this.source + ":" + this.ts;
		}
	}

	private static StreamRecord<TestElem> createStreamRecord(long ts, String source) {
		TestElem testElem = new TestElem(ts, source);
		return new StreamRecord<>(testElem, ts);
	}

	private void prepareTestHarness(KeyedTwoInputStreamOperatorTestHarness<String, TestElem, TestElem, Tuple2<TestElem, TestElem>> testHarness) throws Exception {
		if (lhsFasterThanRhs) {
			// add to lhs
			testHarness.processElement1(createStreamRecord(1, "lhs"));
			testHarness.processWatermark1(new Watermark(1));

			testHarness.processElement1(createStreamRecord(2, "lhs"));
			testHarness.processWatermark1(new Watermark(2));

			testHarness.processElement1(createStreamRecord(3, "lhs"));
			testHarness.processWatermark1(new Watermark(3));

			testHarness.processElement1(createStreamRecord(4, "lhs"));
			testHarness.processWatermark1(new Watermark(4));

			// add to rhs
			testHarness.processElement2(createStreamRecord(1, "rhs"));
			testHarness.processWatermark2(new Watermark(1));

			testHarness.processElement2(createStreamRecord(2, "rhs"));
			testHarness.processWatermark2(new Watermark(2));

			testHarness.processElement2(createStreamRecord(3, "rhs"));
			testHarness.processWatermark2(new Watermark(3));

			testHarness.processElement2(createStreamRecord(4, "rhs"));
			testHarness.processWatermark2(new Watermark(4));
		} else {
			// add to rhs
			testHarness.processElement2(createStreamRecord(1, "rhs"));
			testHarness.processWatermark2(new Watermark(1));

			testHarness.processElement2(createStreamRecord(2, "rhs"));
			testHarness.processWatermark2(new Watermark(2));

			testHarness.processElement2(createStreamRecord(3, "rhs"));
			testHarness.processWatermark2(new Watermark(3));

			testHarness.processElement2(createStreamRecord(4, "rhs"));
			testHarness.processWatermark2(new Watermark(4));

			// add to lhs
			testHarness.processElement1(createStreamRecord(1, "lhs"));
			testHarness.processWatermark1(new Watermark(1));

			testHarness.processElement1(createStreamRecord(2, "lhs"));
			testHarness.processWatermark1(new Watermark(2));

			testHarness.processElement1(createStreamRecord(3, "lhs"));
			testHarness.processWatermark1(new Watermark(3));

			testHarness.processElement1(createStreamRecord(4, "lhs"));
			testHarness.processWatermark1(new Watermark(4));
		}
	}
}
