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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO;

// TODO: Make bucket granularity adaptable
/**
 * A TwoInputStreamOperator to execute time-bounded stream inner joins.
 *
 * <p>By using a configurable lower and upper bound this operator will emit exactly those pairs
 * (T1, T2) where t2.ts ∈ [T1.ts + lowerBound, T1.ts + upperBound]. Both the lower and the
 * upper bound can be configured to be either inclusive or exclusive.
 *
 * <p>As soon as elements are joined they are passed to a user-defined {@link JoinedProcessFunction},
 * as a {@link Tuple2}, with f0 being the left element and f1 being the right element
 *
 * @param <T1> The type of the elements in the left stream
 * @param <T2> The type of the elements in the right stream
 * @param <OUT> The output type created by the user-defined function
 */
public class TimeBoundedStreamJoinOperator<T1, T2, OUT>
	extends AbstractUdfStreamOperator<OUT, JoinedProcessFunction<T1, T2, OUT>>
	implements TwoInputStreamOperator<T1, T2, OUT> {

	private final long lowerBound;
	private final long upperBound;

	private final long inverseLowerBound;
	private final long inverseUpperBound;

	private final boolean lowerBoundInclusive;
	private final boolean upperBoundInclusive;

	private final long bucketGranularity = 1;

	private static final String LEFT_BUFFER = "LEFT_BUFFER";
	private static final String RIGHT_BUFFER = "RIGHT_BUFFER";
	private static final String LAST_CLEANUP_LEFT = "LAST_CLEANUP_LEFT";
	private static final String LAST_CLEANUP_RIGHT = "LAST_CLEANUP_RIGHT";

	private transient ValueState<Long> lastCleanupRightBuffer;
	private transient ValueState<Long> lastCleanupLeftBuffer;

	private transient MapState<Long, List<Tuple3<T1, Long, Boolean>>> leftBuffer;
	private transient MapState<Long, List<Tuple3<T2, Long, Boolean>>> rightBuffer;

	private final TypeSerializer<T1> leftTypeSerializer;
	private final TypeSerializer<T2> rightTypeSerializer;

	private transient TimestampedCollector<OUT> collector;

	private ContextImpl context;

	/**
	 * Creates a new TimeBoundedStreamJoinOperator.
	 *
	 * @param lowerBound          The lower bound for evaluating if elements should be joined
	 * @param upperBound          The upper bound for evaluating if elements should be joined
	 * @param lowerBoundInclusive Whether or not to include elements where the timestamp matches
	 *                            the lower bound
	 * @param upperBoundInclusive Whether or not to include elements where the timestamp matches
	 *                            the upper bound
	 * @param udf                 A user-defined {@link JoinedProcessFunction} that gets called
	 *                            whenever two elements of T1 and T2 are joined
	 */
	public TimeBoundedStreamJoinOperator(
		long lowerBound,
		long upperBound,
		boolean lowerBoundInclusive,
		boolean upperBoundInclusive,
		TypeSerializer<T1> leftTypeSerializer,
		TypeSerializer<T2> rightTypeSerializer,
		JoinedProcessFunction<T1, T2, OUT> udf
	) {

		super(udf);

		this.lowerBound = lowerBound;
		this.upperBound = upperBound;

		this.inverseLowerBound = -1 * upperBound;
		this.inverseUpperBound = -1 * lowerBound;

		this.lowerBoundInclusive = lowerBoundInclusive;
		this.upperBoundInclusive = upperBoundInclusive;
		this.leftTypeSerializer = leftTypeSerializer;
		this.rightTypeSerializer = rightTypeSerializer;
	}

	@Override
	public void open() throws Exception {
		super.open();
		collector = new TimestampedCollector<>(output);
		context = new ContextImpl(userFunction);

		Class<Tuple3<T1, Long, Boolean>> leftTypedTuple =
			(Class<Tuple3<T1, Long, Boolean>>) (Class<?>) Tuple3.class;

		TupleSerializer<Tuple3<T1, Long, Boolean>> leftTupleSerializer = new TupleSerializer<>(
			leftTypedTuple,
			new TypeSerializer[]{
				leftTypeSerializer,
				LongSerializer.INSTANCE,
				BooleanSerializer.INSTANCE
			}
		);

		Class<Tuple3<T2, Long, Boolean>> rightTypedTuple =
			(Class<Tuple3<T2, Long, Boolean>>) (Class<?>) Tuple3.class;

		TupleSerializer<Tuple3<T2, Long, Boolean>> rightTupleSerializer = new TupleSerializer<>(
			rightTypedTuple,
			new TypeSerializer[]{
				rightTypeSerializer,
				LongSerializer.INSTANCE,
				BooleanSerializer.INSTANCE
			}
		);

		this.leftBuffer = getRuntimeContext().getMapState(new MapStateDescriptor<>(
			LEFT_BUFFER,
			LongSerializer.INSTANCE,
			new ListSerializer<>(leftTupleSerializer)
		));

		this.rightBuffer = getRuntimeContext().getMapState(new MapStateDescriptor<>(
			RIGHT_BUFFER,
			LongSerializer.INSTANCE,
			new ListSerializer<>(rightTupleSerializer)
		));

		this.lastCleanupRightBuffer = getRuntimeContext().getState(new ValueStateDescriptor<>(
			LAST_CLEANUP_RIGHT,
			LONG_TYPE_INFO
		));

		this.lastCleanupLeftBuffer = getRuntimeContext().getState(new ValueStateDescriptor<>(
			LAST_CLEANUP_LEFT,
			LONG_TYPE_INFO
		));
	}

	/**
	 * Process a {@link StreamRecord} from the left stream. Whenever an {@link StreamRecord}
	 * arrives at the left stream, it will get added to the left buffer. Possible join candidates
	 * for that element will be looked up from the right buffer and if the pair lies within the
	 * user defined boundaries, it gets collected.
	 *
	 * @param record An incoming record to be joined
	 * @throws Exception Can throw an Exception during state access
	 */
	@Override
	public void processElement1(StreamRecord<T1> record) throws Exception {

		long leftTs = record.getTimestamp();
		T1 leftValue = record.getValue();

		addToLeftBuffer(leftValue, leftTs);

		long min = leftTs + lowerBound;
		long max = leftTs + upperBound;

		// TODO: Adapt to different bucket sizes here
		// Go over all buckets that are within the time bounds
		for (long i = min; i <= max; i++) {
			List<Tuple3<T2, Long, Boolean>> fromBucket = rightBuffer.get(i);
			if (fromBucket != null) {

				// check for each element in current bucket if it should be joined
				for (Tuple3<T2, Long, Boolean> tuple : fromBucket) {
					if (shouldBeJoined(leftTs, tuple.f1)) {

						// collect joined tuple with left timestamp
						collect(leftValue, tuple.f0, leftTs, tuple.f1);
					}
				}
			}
		}
	}

	/**
	 * Process a {@link StreamRecord} from the right stream. Whenever a {@link StreamRecord}
	 * arrives at the right stream, it will get added to the right buffer. Possible join candidates
	 * for that element will be looked up from the left buffer and if the pair lies within the user
	 * defined boundaries, it gets collected.
	 *
	 * @param record An incoming record to be joined
	 * @throws Exception Can throw an exception during state access
	 */
	@Override
	public void processElement2(StreamRecord<T2> record) throws Exception {

		long rightTs = record.getTimestamp();
		T2 rightElem = record.getValue();

		addToRightBuffer(rightElem, rightTs);

		long min = rightTs + inverseLowerBound;
		long max = rightTs + inverseUpperBound;

		// TODO: Adapt to different bucket sizes here
		// Go over all buckets that are within the time bounds
		for (long i = min; i <= max; i++) {
			List<Tuple3<T1, Long, Boolean>> fromBucket = leftBuffer.get(i);
			if (fromBucket != null) {

				// check for each element in current bucket if it should be joined
				for (Tuple3<T1, Long, Boolean> tuple : fromBucket) {
					if (shouldBeJoined(tuple.f1, rightTs)) {

						// collect joined tuple with left timestamp
						collect(tuple.f0, rightElem, tuple.f1, rightTs);
					}
				}
			}
		}
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {

		// remove from both sides all those elements where the timestamp is less than the lower
		// bound, because they are not considered for joining anymore
		removeFromLhsUntil(mark.getTimestamp() + inverseLowerBound);
		removeFromRhsUntil(mark.getTimestamp() + lowerBound);

		if (timeServiceManager != null) {
			timeServiceManager.advanceWatermark(mark);
		}

		output.emitWatermark(new Watermark(mark.getTimestamp() - getWatermarkDelay()));
	}

	private void collect(T1 left, T2 right, long leftTs, long rightTs) throws Exception {
		collector.setAbsoluteTimestamp(leftTs);
		context.leftTs = leftTs;
		context.rightTs = rightTs;
		userFunction.processElement(left, right, context, this.collector);
	}

	private void removeFromLhsUntil(long maxCleanup) throws Exception {

		Long lastCleanUpRight = lastCleanupRightBuffer.value();
		if (lastCleanUpRight == null) {
			lastCleanUpRight = 0L;
		}

		// remove elements from leftBuffer in range [lastValue, maxCleanup]
		for (long i = lastCleanUpRight; i <= maxCleanup; i++) {
			leftBuffer.remove(i);
		}

		lastCleanupRightBuffer.update(maxCleanup);
	}

	private void removeFromRhsUntil(long maxCleanup) throws Exception {

		Long lastCleanupLeft = lastCleanupLeftBuffer.value();
		if (lastCleanupLeft == null) {
			lastCleanupLeft = 0L;
		}

		// remove elements from rightBuffer in range [lastValue, maxCleanup]
		for (long i = lastCleanupLeft; i <= maxCleanup; i++) {
			rightBuffer.remove(i);
		}

		lastCleanupLeftBuffer.update(maxCleanup);
	}

	private boolean shouldBeJoined(long leftTs, long rightTs) {
		long elemLowerBound = leftTs + lowerBound;
		long elemUpperBound = leftTs + upperBound;

		boolean lowerBoundOk = (lowerBoundInclusive)
			? (elemLowerBound <= rightTs)
			: (elemLowerBound < rightTs);

		boolean upperBoundOk = (upperBoundInclusive)
			? (rightTs <= elemUpperBound)
			: (rightTs < elemUpperBound);

		return lowerBoundOk && upperBoundOk;
	}

	private void addToLeftBuffer(T1 value, long ts) throws Exception {
		long bucket = calculateBucket(ts);
		Tuple3<T1, Long, Boolean> elem = Tuple3.of(
			value, // actual value
			ts,    // actual timestamp
			false  // has been joined
		);

		List<Tuple3<T1, Long, Boolean>> elemsInBucket = leftBuffer.get(bucket);
		if (elemsInBucket == null) {
			elemsInBucket = new ArrayList<>();
		}
		elemsInBucket.add(elem);
		leftBuffer.put(bucket, elemsInBucket);
	}

	private void addToRightBuffer(T2 value, long ts) throws Exception {
		long bucket = calculateBucket(ts);
		Tuple3<T2, Long, Boolean> elem = Tuple3.of(
			value, // actual value
			ts,    // actual timestamp
			false  // has been joined
		);

		List<Tuple3<T2, Long, Boolean>> elemsInBucket = rightBuffer.get(bucket);
		if (elemsInBucket == null) {
			elemsInBucket = new ArrayList<>();
		}
		elemsInBucket.add(elem);
		rightBuffer.put(bucket, elemsInBucket);
	}

	private long calculateBucket(long ts) {
		return Math.floorDiv(ts, bucketGranularity);
	}

	public long getWatermarkDelay() {
		// TODO: Adapt this to when we use the rightBuffer or min timestamp
		return (upperBound < 0) ? 0 : upperBound;
	}

	private class ContextImpl extends JoinedProcessFunction<T1, T2, OUT>.Context {

		private long leftTs;
		private long rightTs;

		public ContextImpl(JoinedProcessFunction<T1, T2, OUT> func) {
			func.super();
		}

		public long getLeftTimestamp() {
			return leftTs;
		}

		public long getRightTimestamp() {
			return rightTs;
		}

		public long getTimestamp() {
			return leftTs;
		}
	}

	@VisibleForTesting
	public MapState<Long, List<Tuple3<T1, Long, Boolean>>> getLeftBuffer() {
		return leftBuffer;
	}

	@VisibleForTesting
	public MapState<Long, List<Tuple3<T2, Long, Boolean>>> getRightBuffer() {
		return rightBuffer;
	}
}
