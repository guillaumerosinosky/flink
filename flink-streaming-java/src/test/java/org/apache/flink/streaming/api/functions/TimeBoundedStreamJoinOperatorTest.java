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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorStateHandles;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

// TODO: Parameterize to use different state backends --> This would require circular dependency on flink rocksdb
@RunWith(Parameterized.class)
public class TimeBoundedStreamJoinOperatorTest {

	private final boolean lhsFasterThanRhs;

	@Parameters(name = "lhs faster than rhs stream: {0}")
	public static Boolean[] data() {
		return new Boolean[]{true, false};
	}

	public TimeBoundedStreamJoinOperatorTest(boolean lhsFasterThanRhs) {
		this.lhsFasterThanRhs = lhsFasterThanRhs;
	}

	@Test // lhs - 2 <= rhs <= rhs + 2
	public void testNegativeInclusiveAndNegativeInclusive() throws Exception {

		long lowerBound = -2;
		boolean lowerBoundInclusive = true;

		long upperBound = -1;
		boolean upperBoundInclusive = true;

		try (KeyedTwoInputStreamOperatorTestHarness<String, TestElem, TestElem, Tuple2<TestElem, TestElem>> testHarness
				 = createTestHarness(lowerBound, lowerBoundInclusive, upperBound, upperBoundInclusive)) {


			testHarness.setup();
			testHarness.open();

			prepareTestHarness(testHarness);

			List<StreamRecord<Tuple2<TestElem, TestElem>>> expectedOutput = Lists.newArrayList(
				streamRecordOf(2, 1),
				streamRecordOf(3, 1),
				streamRecordOf(3, 2),
				streamRecordOf(4, 2),
				streamRecordOf(4, 3)
			);

			assertOutput(expectedOutput, testHarness.getOutput());
			ensureNoLateData(testHarness.getOutput());
		}
	}

	@Test // lhs - 1 <= rhs <= rhs + 1
	public void testNegativeInclusiveAndPositiveInclusive() throws Exception {

		long lowerBound = -1;
		boolean lowerBoundInclusive = true;

		long upperBound = 1;
		boolean upperBoundInclusive = true;

		try (KeyedTwoInputStreamOperatorTestHarness<String, TestElem, TestElem, Tuple2<TestElem, TestElem>> testHarness
				 = createTestHarness(lowerBound, lowerBoundInclusive, upperBound, upperBoundInclusive)) {

			testHarness.setup();
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

			assertOutput(expectedOutput, testHarness.getOutput());
			ensureNoLateData(output);

		}
	}

	@Test // lhs + 1 <= rhs <= lhs + 2
	public void testPositiveInclusiveAndPositiveInclusive() throws Exception {
		long lowerBound = 1;
		long upperBound = 2;

		boolean lowerBoundInclusive = true;
		boolean upperBoundInclusive = true;

		try (KeyedTwoInputStreamOperatorTestHarness<String, TestElem, TestElem, Tuple2<TestElem, TestElem>> testHarness
				 = createTestHarness(lowerBound, lowerBoundInclusive, upperBound, upperBoundInclusive)) {

			testHarness.setup();
			testHarness.open();

			prepareTestHarness(testHarness);

			List<StreamRecord<Tuple2<TestElem, TestElem>>> expected = Lists.newArrayList(
				streamRecordOf(1, 2),
				streamRecordOf(1, 3),
				streamRecordOf(2, 3),
				streamRecordOf(2, 4),
				streamRecordOf(3, 4)
			);

			assertOutput(expected, testHarness.getOutput());
			ensureNoLateData(testHarness.getOutput());
		}
	}

	@Test
	public void testNegativeExclusiveAndNegativeExlusive() throws Exception {
		long lowerBound = -3;
		boolean lowerBoundInclusive = false;

		long upperBound = -1;
		boolean upperBoundInclusive = false;

		try (KeyedTwoInputStreamOperatorTestHarness<String, TestElem, TestElem, Tuple2<TestElem, TestElem>> testHarness
				 = createTestHarness(lowerBound, lowerBoundInclusive, upperBound, upperBoundInclusive)) {

			testHarness.setup();
			testHarness.open();
			prepareTestHarness(testHarness);

			List<StreamRecord<Tuple2<TestElem, TestElem>>> expectedOutput = Lists.newArrayList(
				streamRecordOf(3, 1),
				streamRecordOf(4, 2)
			);

			ConcurrentLinkedQueue<Object> output = testHarness.getOutput();

			assertOutput(expectedOutput, testHarness.getOutput());
			ensureNoLateData(output);
		}
	}

	@Test
	public void testNegativeExclusiveAndPositiveExlusive() throws Exception {
		long lowerBound = -1;
		boolean lowerBoundInclusive = false;

		long upperBound = 1;
		boolean upperBoundInclusive = false;

		try (KeyedTwoInputStreamOperatorTestHarness<String, TestElem, TestElem, Tuple2<TestElem, TestElem>> testHarness
				 = createTestHarness(lowerBound, lowerBoundInclusive, upperBound, upperBoundInclusive)) {

			testHarness.setup();
			testHarness.open();
			prepareTestHarness(testHarness);

			List<StreamRecord<Tuple2<TestElem, TestElem>>> expectedOutput = Lists.newArrayList(
				streamRecordOf(1, 1),
				streamRecordOf(2, 2),
				streamRecordOf(3, 3),
				streamRecordOf(4, 4)
			);

			ConcurrentLinkedQueue<Object> output = testHarness.getOutput();

			assertOutput(expectedOutput, testHarness.getOutput());
			ensureNoLateData(output);
		}
	}

	@Test
	public void testPositiveExclusiveAndPositiveExlusive() throws Exception {
		long lowerBound = 1;
		boolean lowerBoundInclusive = false;

		long upperBound = 3;
		boolean upperBoundInclusive = false;

		try (KeyedTwoInputStreamOperatorTestHarness<String, TestElem, TestElem, Tuple2<TestElem, TestElem>> testHarness
				 = createTestHarness(lowerBound, lowerBoundInclusive, upperBound, upperBoundInclusive)) {

			testHarness.setup();
			testHarness.open();
			prepareTestHarness(testHarness);

			List<StreamRecord<Tuple2<TestElem, TestElem>>> expectedOutput = Lists.newArrayList(
				streamRecordOf(1, 3),
				streamRecordOf(2, 4)
			);

			ConcurrentLinkedQueue<Object> output = testHarness.getOutput();

			assertOutput(expectedOutput, testHarness.getOutput());
			ensureNoLateData(output);

		}
	}

	@Test
	public void stateGetsCleanedWhenNotNeeded() throws Exception {

		long lowerBound = 1;
		boolean lowerBoundInclusive = true;

		long upperBound = 2;
		boolean upperBoundInclusive = true;

		TimeBoundedStreamJoinOperator<TestElem, TestElem> operator = new TimeBoundedStreamJoinOperator<>(
			lowerBound,
			upperBound,
			lowerBoundInclusive,
			upperBoundInclusive
		);

		KeyedTwoInputStreamOperatorTestHarness<
			String,
			TestElem,
			TestElem,
			Tuple2<TestElem, TestElem>> testHarness
			= new KeyedTwoInputStreamOperatorTestHarness<>(
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
		assertEmpty(operator.getRightBuffer());

		testHarness.processElement1(createStreamRecord(2, "lhs"));
		testHarness.processWatermark1(new Watermark(2));

		assertContainsOnly(operator.getLeftBuffer(), 1, 2);
		assertEmpty(operator.getRightBuffer());

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

		assertContainsOnly(operator.getLeftBuffer(), 2, 3);
		assertEmpty(operator.getRightBuffer());

		testHarness.processElement1(createStreamRecord(4, "lhs"));
		testHarness.processWatermark1(new Watermark(4));

		assertContainsOnly(operator.getLeftBuffer(), 2, 3, 4);
		assertEmpty(operator.getRightBuffer());

		testHarness.processElement2(createStreamRecord(4, "rhs"));
		testHarness.processWatermark2(new Watermark(4));

		assertContainsOnly(operator.getLeftBuffer(), 3, 4);
		assertEmpty(operator.getRightBuffer());
	}

	@Test
	public void testRestoreFromSnapshot() throws Exception {

		// config
		int lowerBound = -1;
		boolean lowerBoundInclusive = true;
		int upperBound = 1;
		boolean upperBoundInclusive = true;

		// create first test harness
		KeyedTwoInputStreamOperatorTestHarness<
			String,
			TestElem,
			TestElem,
			Tuple2<TestElem, TestElem>> testHarness
			= createTestHarness(lowerBound, lowerBoundInclusive, upperBound, upperBoundInclusive);

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

		ensureNoLateData(testHarness.getOutput());
		assertOutput(expectedOutput, testHarness.getOutput());

		// create new test harness from snapshpt
		KeyedTwoInputStreamOperatorTestHarness<
			String,
			TestElem,
			TestElem,
			Tuple2<TestElem, TestElem>
			> newTestHarness = createTestHarness(lowerBound, lowerBoundInclusive, upperBound, upperBoundInclusive);


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

		ensureNoLateData(newTestHarness.getOutput());
		assertOutput(expectedOutput, newTestHarness.getOutput());
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

	// TODO: Move this to test harness utils
	private void ensureNoLateData(Iterable<Object> output) {
		// check that no watermark is violated
		long highestWatermark = Long.MIN_VALUE;

		for (Object elem : output) {
			if (elem instanceof Watermark) {
				highestWatermark = ((Watermark) elem).asWatermark().getTimestamp();
			} else if (elem instanceof StreamRecord) {
				boolean dataIsOnTime = highestWatermark < ((StreamRecord) elem).getTimestamp();
				Assert.assertTrue("Late data was emitted after join", dataIsOnTime);
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

		TimeBoundedStreamJoinOperator<
			TestElem,
			TestElem> operator
			= new TimeBoundedStreamJoinOperator<>(
			lowerBound,
			upperBound,
			lowerBoundInclusive,
			upperBoundInclusive);

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
