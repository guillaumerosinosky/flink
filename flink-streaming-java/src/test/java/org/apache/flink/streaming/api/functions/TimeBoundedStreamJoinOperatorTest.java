/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorStateHandles;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

// TODO: Parameterize to use different state backends --> This would require circular dependency on flink rocksdb

/**
 * Tests for {@link TimeBoundedStreamJoinOperator}.
 * Those tests cover correctness and cleaning of state
 */
@RunWith(Parameterized.class)
public class TimeBoundedStreamJoinOperatorTest {

	private final boolean lhsFasterThanRhs;
	private final int bucketSize;

	@Parameters(name = "lhs faster than rhs: {0}, bucketSize: {1}")
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][]{
			{true, 1}, {true, 5}, {false, 1}, {false, 5}
		});
	}

	public TimeBoundedStreamJoinOperatorTest(boolean lhsFasterThanRhs, int bucketSize) {
		this.lhsFasterThanRhs = lhsFasterThanRhs;
		this.bucketSize = bucketSize;
	}

	@Test // lhs - 2 <= rhs <= rhs + 2
	public void testNegativeInclusiveAndNegativeInclusive() throws Exception {

		long lowerBound = -2;
		boolean lowerBoundInclusive = true;

		long upperBound = -1;
		boolean upperBoundInclusive = true;

		try (TestHarness testHarness =
			createTestHarness(lowerBound, lowerBoundInclusive, upperBound, upperBoundInclusive)) {

			testHarness.setup();
			testHarness.open();

			processElementsAndWatermarks(testHarness);

			List<StreamRecord<Tuple2<TestElem, TestElem>>> expectedOutput = Lists.newArrayList(
				streamRecordOf(2, 1),
				streamRecordOf(3, 1),
				streamRecordOf(3, 2),
				streamRecordOf(4, 2),
				streamRecordOf(4, 3)
			);

			assertOutput(expectedOutput, testHarness.getOutput());
			TestHarnessUtil.assertNoLateRecords(testHarness.getOutput());
		}
	}

	@Test // lhs - 1 <= rhs <= rhs + 1
	public void testNegativeInclusiveAndPositiveInclusive() throws Exception {

		long lowerBound = -1;
		boolean lowerBoundInclusive = true;

		long upperBound = 1;
		boolean upperBoundInclusive = true;

		try (TestHarness testHarness =
			createTestHarness(lowerBound, lowerBoundInclusive, upperBound, upperBoundInclusive)) {

			testHarness.setup();
			testHarness.open();

			processElementsAndWatermarks(testHarness);

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

			assertOutput(expectedOutput, testHarness.getOutput());
			TestHarnessUtil.assertNoLateRecords(output);

		}
	}

	@Test // lhs + 1 <= rhs <= lhs + 2
	public void testPositiveInclusiveAndPositiveInclusive() throws Exception {
		long lowerBound = 1;
		long upperBound = 2;

		boolean lowerBoundInclusive = true;
		boolean upperBoundInclusive = true;

		try (TestHarness testHarness =
			createTestHarness(lowerBound, lowerBoundInclusive, upperBound, upperBoundInclusive)) {

			testHarness.setup();
			testHarness.open();

			processElementsAndWatermarks(testHarness);

			List<StreamRecord<Tuple2<TestElem, TestElem>>> expected = Lists.newArrayList(
				streamRecordOf(1, 2),
				streamRecordOf(1, 3),
				streamRecordOf(2, 3),
				streamRecordOf(2, 4),
				streamRecordOf(3, 4)
			);

			assertOutput(expected, testHarness.getOutput());
			TestHarnessUtil.assertNoLateRecords(testHarness.getOutput());
		}
	}

	@Test
	public void testNegativeExclusiveAndNegativeExlusive() throws Exception {
		long lowerBound = -3;
		boolean lowerBoundInclusive = false;

		long upperBound = -1;
		boolean upperBoundInclusive = false;

		try (TestHarness testHarness =
			createTestHarness(lowerBound, lowerBoundInclusive, upperBound, upperBoundInclusive)) {

			testHarness.setup();
			testHarness.open();
			processElementsAndWatermarks(testHarness);

			List<StreamRecord<Tuple2<TestElem, TestElem>>> expectedOutput = Lists.newArrayList(
				streamRecordOf(3, 1),
				streamRecordOf(4, 2)
			);

			ConcurrentLinkedQueue<Object> output = testHarness.getOutput();

			assertOutput(expectedOutput, testHarness.getOutput());
			TestHarnessUtil.assertNoLateRecords(output);
		}
	}

	@Test
	public void testNegativeExclusiveAndPositiveExlusive() throws Exception {
		long lowerBound = -1;
		boolean lowerBoundInclusive = false;

		long upperBound = 1;
		boolean upperBoundInclusive = false;

		try (TestHarness testHarness =
			createTestHarness(lowerBound, lowerBoundInclusive, upperBound, upperBoundInclusive)) {

			testHarness.setup();
			testHarness.open();
			processElementsAndWatermarks(testHarness);

			List<StreamRecord<Tuple2<TestElem, TestElem>>> expectedOutput = Lists.newArrayList(
				streamRecordOf(1, 1),
				streamRecordOf(2, 2),
				streamRecordOf(3, 3),
				streamRecordOf(4, 4)
			);

			ConcurrentLinkedQueue<Object> output = testHarness.getOutput();

			assertOutput(expectedOutput, testHarness.getOutput());
			TestHarnessUtil.assertNoLateRecords(output);
		}
	}

	@Test
	public void testPositiveExclusiveAndPositiveExlusive() throws Exception {
		long lowerBound = 1;
		boolean lowerBoundInclusive = false;

		long upperBound = 3;
		boolean upperBoundInclusive = false;

		try (TestHarness testHarness =
			createTestHarness(lowerBound, lowerBoundInclusive, upperBound, upperBoundInclusive)) {

			testHarness.setup();
			testHarness.open();
			processElementsAndWatermarks(testHarness);

			List<StreamRecord<Tuple2<TestElem, TestElem>>> expectedOutput = Lists.newArrayList(
				streamRecordOf(1, 3),
				streamRecordOf(2, 4)
			);

			ConcurrentLinkedQueue<Object> output = testHarness.getOutput();

			assertOutput(expectedOutput, testHarness.getOutput());
			TestHarnessUtil.assertNoLateRecords(output);

		}
	}

	@Test
	public void testStateGetsCleanedWhenNotNeededBucketSize1() throws Exception {

		long lowerBound = 1;
		boolean lowerBoundInclusive = true;

		long upperBound = 2;
		boolean upperBoundInclusive = true;

		int bucketGranularity = 1;

		TimeBoundedStreamJoinOperator<String, TestElem, TestElem, Tuple2<TestElem, TestElem>> operator = new TimeBoundedStreamJoinOperator<>(
			lowerBound,
			upperBound,
			lowerBoundInclusive,
			upperBoundInclusive,
			bucketGranularity,
			TestElem.serializer(),
			TestElem.serializer(),
			new PassthroughFunction()
		);

		TestHarness testHarness = new TestHarness(
				operator,
				(elem) -> elem.key, // key
				(elem) -> elem.key, // key
				TypeInformation.of(String.class)
		);

		testHarness.setup();
		testHarness.open();

		testHarness.processElement1(createStreamRecord(1, "lhs"));
		testHarness.processWatermark1(new Watermark(1));

		assertContainsOnly(operator.getLeftBuffer(), 1);
		assertEmpty(operator.getRightBuffer());

		testHarness.processElement2(createStreamRecord(1, "rhs"));
		testHarness.processWatermark2(new Watermark(1));

		assertContainsOnly(operator.getLeftBuffer(), 1);
		assertContainsOnly(operator.getRightBuffer(), 1);

		testHarness.processElement1(createStreamRecord(2, "lhs"));
		testHarness.processWatermark1(new Watermark(2));

		assertContainsOnly(operator.getLeftBuffer(), 1, 2);
		assertContainsOnly(operator.getRightBuffer(), 1);

		testHarness.processElement2(createStreamRecord(2, "rhs"));
		testHarness.processWatermark2(new Watermark(2));

		assertContainsOnly(operator.getLeftBuffer(), 1, 2);
		assertEmpty(operator.getRightBuffer());

		testHarness.processElement1(createStreamRecord(3, "lhs"));
		testHarness.processWatermark1(new Watermark(3));

		assertContainsOnly(operator.getLeftBuffer(), 1, 2, 3);
		assertEmpty(operator.getRightBuffer());

		testHarness.processElement2(createStreamRecord(3, "rhs"));
		testHarness.processWatermark2(new Watermark(3));

		assertContainsOnly(operator.getLeftBuffer(), 1, 2, 3);
		assertEmpty(operator.getRightBuffer());

		testHarness.processElement1(createStreamRecord(4, "lhs"));
		testHarness.processWatermark1(new Watermark(4));

		assertContainsOnly(operator.getLeftBuffer(), 1, 2, 3, 4);
		assertEmpty(operator.getRightBuffer());

		testHarness.processElement2(createStreamRecord(4, "rhs"));
		testHarness.processWatermark2(new Watermark(4));

		assertContainsOnly(operator.getLeftBuffer(), 2, 3, 4);
		assertEmpty(operator.getRightBuffer());
	}

	@Test
	public void testStateGetsCleanedWhenNotNeededBucketSize5() throws Exception {

		long lowerBound = 0;
		boolean lowerBoundInclusive = true;

		long upperBound = 0;
		boolean upperBoundInclusive = true;

		int bucketGranularity = 5;

		TimeBoundedStreamJoinOperator<String, TestElem, TestElem, Tuple2<TestElem, TestElem>> operator = new TimeBoundedStreamJoinOperator<>(
			lowerBound,
			upperBound,
			lowerBoundInclusive,
			upperBoundInclusive,
			bucketGranularity,
			TestElem.serializer(),
			TestElem.serializer(),
			new PassthroughFunction()
		);

		TestHarness testHarness = new TestHarness(
				operator,
				(elem) -> elem.key, // key
				(elem) -> elem.key, // key
				TypeInformation.of(String.class)
			);

		testHarness.setup();
		testHarness.open();

		for (int i = 0; i < 10; i++) {
			testHarness.processElement1(createStreamRecord(i, "lhs"));
			testHarness.processWatermark1(new Watermark(i));
			testHarness.processElement2(createStreamRecord(i, "rhs"));
			testHarness.processWatermark2(new Watermark(i));
		}

		assertContainsOnly(operator.getLeftBuffer(), 5);
		assertContainsOnly(operator.getRightBuffer(), 5);
	}

	@Test
	public void testRestoreFromSnapshot() throws Exception {

		// config
		int lowerBound = -1;
		boolean lowerBoundInclusive = true;
		int upperBound = 1;
		boolean upperBoundInclusive = true;

		// create first test harness
		TestHarness testHarness = createTestHarness(
			lowerBound,
			lowerBoundInclusive,
			upperBound,
			upperBoundInclusive
		);

		testHarness.setup();
		testHarness.open();

		// process elements with first test harness
		testHarness.processElement1(createStreamRecord(1, "lhs"));
		testHarness.processWatermark1(new Watermark(1));

		testHarness.processElement2(createStreamRecord(1, "rhs"));
		testHarness.processWatermark2(new Watermark(1));

		testHarness.processElement1(createStreamRecord(2, "lhs"));
		testHarness.processWatermark1(new Watermark(2));

		testHarness.processElement2(createStreamRecord(2, "rhs"));
		testHarness.processWatermark2(new Watermark(2));

		testHarness.processElement1(createStreamRecord(3, "lhs"));
		testHarness.processWatermark1(new Watermark(3));

		testHarness.processElement2(createStreamRecord(3, "rhs"));
		testHarness.processWatermark2(new Watermark(3));

		// snapshot and validate output
		OperatorStateHandles handles = testHarness.snapshot(0, 0);
		testHarness.close();

		List<StreamRecord<Tuple2<TestElem, TestElem>>> expectedOutput = Lists.newArrayList(
			streamRecordOf(1, 1),
			streamRecordOf(1, 2),
			streamRecordOf(2, 1),
			streamRecordOf(2, 2),
			streamRecordOf(2, 3),
			streamRecordOf(3, 2),
			streamRecordOf(3, 3)
		);

		TestHarnessUtil.assertNoLateRecords(testHarness.getOutput());
		assertOutput(expectedOutput, testHarness.getOutput());

		// create new test harness from snapshpt
		TestHarness newTestHarness = createTestHarness(lowerBound, lowerBoundInclusive, upperBound, upperBoundInclusive);

		newTestHarness.setup();
		newTestHarness.initializeState(handles);
		newTestHarness.open();

		// process elements
		newTestHarness.processElement1(createStreamRecord(4, "lhs"));
		newTestHarness.processWatermark1(new Watermark(4));

		newTestHarness.processElement2(createStreamRecord(4, "rhs"));
		newTestHarness.processWatermark2(new Watermark(4));

		// assert expected output
		expectedOutput = Lists.newArrayList(
			streamRecordOf(3, 4),
			streamRecordOf(4, 3),
			streamRecordOf(4, 4)
		);

		TestHarnessUtil.assertNoLateRecords(newTestHarness.getOutput());
		assertOutput(expectedOutput, newTestHarness.getOutput());
	}

	@Test
	public void testContextCorrectLeftTimestamp() throws Exception {

		TimeBoundedStreamJoinOperator<String, TestElem, TestElem, Tuple2<TestElem, TestElem>> op =
			new TimeBoundedStreamJoinOperator<>(
				-1,
				1,
				true,
				true,
				bucketSize,
				TestElem.serializer(),
				TestElem.serializer(),
				new JoinedProcessFunction<TestElem, TestElem, Tuple2<TestElem, TestElem>>() {
					@Override
					public void processElement(
						TestElem left,
						TestElem right,
						Context ctx,
						Collector<Tuple2<TestElem, TestElem>> out) throws Exception {
						Assert.assertEquals(left.ts, ctx.getLeftTimestamp());
					}
				}
			);

		TestHarness testHarness = new TestHarness(
			op,
			(elem) -> elem.key,
			(elem) -> elem.key,
			TypeInformation.of(String.class)
		);

		testHarness.setup();
		testHarness.open();

		processElementsAndWatermarks(testHarness);

		testHarness.close();
	}

	@Test
	public void testContextCorrectRightTimestamp() throws Exception {

		TimeBoundedStreamJoinOperator<String, TestElem, TestElem, Tuple2<TestElem, TestElem>> op =
			new TimeBoundedStreamJoinOperator<>(
				-1,
				1,
				true,
				true,
				bucketSize,
				TestElem.serializer(),
				TestElem.serializer(),
				new JoinedProcessFunction<TestElem, TestElem, Tuple2<TestElem, TestElem>>() {
					@Override
					public void processElement(
						TestElem left,
						TestElem right,
						Context ctx,
						Collector<Tuple2<TestElem, TestElem>> out) throws Exception {
						Assert.assertEquals(right.ts, ctx.getRightTimestamp());
					}
				}
			);

		TestHarness testHarness = new TestHarness(
			op,
			(elem) -> elem.key,
			(elem) -> elem.key,
			TypeInformation.of(String.class)
		);

		testHarness.setup();
		testHarness.open();

		processElementsAndWatermarks(testHarness);

		testHarness.close();
	}

	@Test(expected = RuntimeException.class)
	public void testFailsWithNoTimestampsLeft() throws Exception {
		TestHarness newTestHarness = createTestHarness(0L, true, 0L, true);

		newTestHarness.setup();
		newTestHarness.open();

		// note that the StreamRecord has no timestamp in constructor
		newTestHarness.processElement1(new StreamRecord<>(new TestElem(0, "lhs")));
	}

	@Test(expected = RuntimeException.class)
	public void testFailsWithNoTimestampsRight() throws Exception {
		TestHarness newTestHarness = createTestHarness(0L, true, 0L, true);

		newTestHarness.setup();
		newTestHarness.open();

		// note that the StreamRecord has no timestamp in constructor
		newTestHarness.processElement2(new StreamRecord<>(new TestElem(0, "rhs")));
	}

	@Test
	public void udfCanRegisterTimer() throws Exception {

		AtomicInteger numCalled = new AtomicInteger(0);
		TimeBoundedStreamJoinOperator op = new TimeBoundedStreamJoinOperator<String, TestElem, TestElem, Object>(
			0,
			0,
			true,
			true,
			1,
			TestElem.serializer(),
			TestElem.serializer(),
			new JoinedProcessFunction<TestElem, TestElem, Object>() {
				@Override
				public void processElement(TestElem left,
										   TestElem right, Context ctx, Collector<Object> out) throws Exception {
					ctx.registerEventTimeTimer(1);
				}

				@Override
				public void onTimer(long timestamp, Context ctx, Collector<Object> out) {
					Assert.assertEquals(1, timestamp);
					numCalled.getAndIncrement();
				}
			}
		);

		TestHarness harness = new TestHarness(op, elem -> "", elem -> "", TypeInformation.of(String.class));
		harness.open();
		harness.setup();

		harness.processElement1(new StreamRecord<>(new TestElem(1, "lhs"), 1L));
		harness.processElement2(new StreamRecord<>(new TestElem(1, "rhs"), 1L));

		harness.processWatermark1(new Watermark(1L));
		harness.processWatermark2(new Watermark(1L));

		harness.close();

		Assert.assertEquals(1, numCalled.get());
	}

	@Test
	public void udfTimerGetsOnlyFiredForUserTimers() throws Exception {
		AtomicInteger numCalled = new AtomicInteger(0);
		TimeBoundedStreamJoinOperator op = new TimeBoundedStreamJoinOperator<String, TestElem, TestElem, Object>(
			0,
			0,
			true,
			true,
			1,
			TestElem.serializer(),
			TestElem.serializer(),
			new JoinedProcessFunction<TestElem, TestElem, Object>() {
				@Override
				public void processElement(TestElem left,
										   TestElem right, Context ctx, Collector<Object> out) throws Exception {
					if (numCalled.get() == 0) {
						ctx.registerEventTimeTimer(1);
					}
				}

				@Override
				public void onTimer(long timestamp, Context ctx, Collector<Object> out) {
					Assert.assertEquals(1, timestamp);
					numCalled.incrementAndGet();
				}
			}
		);

		TestHarness harness = new TestHarness(op, elem -> "", elem -> "", TypeInformation.of(String.class));
		harness.open();
		harness.setup();

		harness.processElement1(new StreamRecord<>(new TestElem(1, "lhs"), 1L));
		harness.processElement2(new StreamRecord<>(new TestElem(1, "rhs"), 1L));

		harness.processWatermark1(new Watermark(1L));
		harness.processWatermark2(new Watermark(1L));

		harness.processElement1(new StreamRecord<>(new TestElem(2, "lhs"), 1L));
		harness.processElement2(new StreamRecord<>(new TestElem(2, "rhs"), 1L));

		harness.processWatermark1(new Watermark(2L));
		harness.processWatermark2(new Watermark(2L));

		harness.processElement1(new StreamRecord<>(new TestElem(3, "lhs"), 1L));
		harness.processElement2(new StreamRecord<>(new TestElem(3, "rhs"), 1L));

		harness.processWatermark1(new Watermark(3L));
		harness.processWatermark2(new Watermark(3L));

		harness.close();

		Assert.assertEquals(1, numCalled.get());
	}

	private void assertEmpty(MapState<Long, ?> state) throws Exception {
		boolean stateIsEmpty = Iterables.size(state.keys()) == 0;
		Assert.assertTrue("state not empty", stateIsEmpty);
	}

	private void assertContainsOnly(MapState<Long, ?> state, long... ts) throws Exception {
		for (long t : ts) {
			Assert.assertTrue("key not found in state", state.contains(t));
		}

		Assert.assertEquals("too many objects in state", ts.length, Iterables.size(state.keys()));
	}

	private void assertOutput(
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

	private TestHarness createTestHarness(long lowerBound,
		boolean lowerBoundInclusive,
		long upperBound,
		boolean upperBoundInclusive) throws Exception {

		TimeBoundedStreamJoinOperator<String, TestElem, TestElem, Tuple2<TestElem, TestElem>> operator =
			new TimeBoundedStreamJoinOperator<>(
				lowerBound,
				upperBound,
				lowerBoundInclusive,
				upperBoundInclusive,
				bucketSize,
				TestElem.serializer(),
				TestElem.serializer(),
				new PassthroughFunction()
			);

		return new TestHarness(
			operator,
			(elem) -> elem.key, // key
			(elem) -> elem.key, // key
			TypeInformation.of(String.class)
		);
	}

	private static class PassthroughFunction extends JoinedProcessFunction<TestElem, TestElem, Tuple2<TestElem, TestElem>> {

		@Override
		public void processElement(
			TestElem left,
			TestElem right,
			Context ctx,
			Collector<Tuple2<TestElem, TestElem>> out) throws Exception {
			out.collect(Tuple2.of(left, right));
		}
	}

	private StreamRecord<Tuple2<TestElem, TestElem>> streamRecordOf(long lhsTs,
		long rhsTs) {
		TestElem lhs = new TestElem(lhsTs, "lhs");
		TestElem rhs = new TestElem(rhsTs, "rhs");

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
			if (this == o) {
				return true;
			}

			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			TestElem testElem = (TestElem) o;

			if (ts != testElem.ts) {
				return false;
			}

			if (key != null ? !key.equals(testElem.key) : testElem.key != null) {
				return false;
			}

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

		public static TypeSerializer<TestElem> serializer() {
			return TypeInformation.of(new TypeHint<TestElem>() {
			}).createSerializer(new ExecutionConfig());
		}
	}

	private static StreamRecord<TestElem> createStreamRecord(long ts, String source) {
		TestElem testElem = new TestElem(ts, source);
		return new StreamRecord<>(testElem, ts);
	}

	private void processElementsAndWatermarks(TestHarness testHarness) throws Exception {
		if (lhsFasterThanRhs) {
			// add to lhs
			for (int i = 1; i <= 4; i++) {
				testHarness.processElement1(createStreamRecord(i, "lhs"));
				testHarness.processWatermark1(new Watermark(i));
			}

			// add to rhs
			for (int i = 1; i <= 4; i++) {
				testHarness.processElement2(createStreamRecord(i, "rhs"));
				testHarness.processWatermark2(new Watermark(i));
			}
		} else {
			// add to rhs
			for (int i = 1; i <= 4; i++) {
				testHarness.processElement2(createStreamRecord(i, "rhs"));
				testHarness.processWatermark2(new Watermark(i));
			}

			// add to lhs
			for (int i = 1; i <= 4; i++) {
				testHarness.processElement1(createStreamRecord(i, "lhs"));
				testHarness.processWatermark1(new Watermark(i));
			}
		}
	}

	/**
	 * Custom test harness to avoid endless generics in all of the test code.
	 */
	private static class TestHarness extends KeyedTwoInputStreamOperatorTestHarness<String, TestElem, TestElem, Tuple2<TestElem, TestElem>> {

		TestHarness(
			TwoInputStreamOperator<TestElem, TestElem, Tuple2<TestElem, TestElem>> operator,
			KeySelector<TestElem, String> keySelector1,
			KeySelector<TestElem, String> keySelector2,
			TypeInformation<String> keyType) throws Exception {
			super(operator, keySelector1, keySelector2, keyType);
		}
	}
}
