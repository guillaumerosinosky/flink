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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO;

/**
 * A TwoInputStreamOperator to execute time-bounded stream inner joins.
 *
 * <p>By using a configurable lower and upper bound this operator will emit exactly those pairs
 * (T1, T2) where t2.ts ∈ [T1.ts + lowerBound, T1.ts + upperBound]. Both the lower and the
 * upper bound can be configured to be either inclusive or exclusive.
 *
 * <p>As soon as elements are joined they are passed to a user-defined {@link TimeBoundedJoinFunction},
 * as a {@link Tuple2}, with f0 being the left element and f1 being the right element
 *
 * <p>The basic idea of this implementation is as follows: Whenever we receive an element at
 * {@link #processElement1(StreamRecord)} (a.k.a. the left side), we add it to the left buffer.
 * We then check the right buffer to see whether there are any elements that can be joined. If
 * there are, they are joined and passed to a user-defined {@link TimeBoundedJoinFunction}.
 * The same happens the other way around when receiving an element on the right side.
 *
 * <p>In some cases the watermark needs to be delayed. This for example can happen if
 * if t2.ts ∈ [t1.ts + 1, t1.ts + 2] and elements from t1 arrive earlier than elements from t2 and
 * therefore get added to the left buffer. When an element now arrives on the right side, the
 * watermark might have already progressed. The right element now gets joined with an
 * older element from the left side, where the timestamp of the left element is lower than the
 * current watermark, which would make this element late. This can be avoided by holding back the
 * watermarks.
 *
 * <p>The left and right buffers are cleared from unused values periodically
 * (triggered by watermarks) in order not to grow infinitely.
 *
 *
 * @param <T1> The type of the elements in the left stream
 * @param <T2> The type of the elements in the right stream
 * @param <OUT> The output type created by the user-defined function
 */
@Internal
public class TimeBoundedStreamJoinOperator<K, T1, T2, OUT>
	extends AbstractUdfStreamOperator<OUT, TimeBoundedJoinFunction<T1, T2, OUT>>
	implements TwoInputStreamOperator<T1, T2, OUT>, Triggerable<K, String> {

	private static final String LEFT_BUFFER = "LEFT_BUFFER";
	private static final String RIGHT_BUFFER = "RIGHT_BUFFER";
	private static final String LAST_CLEANUP_LEFT = "LAST_CLEANUP_LEFT";
	private static final String LAST_CLEANUP_RIGHT = "LAST_CLEANUP_RIGHT";
	private static final String CLEANUP_TIMER_NAMESPACE = "CLEANUP_TIMER";

	private final long lowerBound;
	private final long upperBound;

	private final long inverseLowerBound;
	private final long inverseUpperBound;

	private final boolean lowerBoundInclusive;
	private final boolean upperBoundInclusive;

	private final TypeSerializer<T1> leftTypeSerializer;
	private final TypeSerializer<T2> rightTypeSerializer;

	private final long bucketGranularity;

	private transient ValueState<Long> lastCleanupRightBuffer;
	private transient ValueState<Long> lastCleanupLeftBuffer;

	private transient MapState<Long, List<Tuple3<T1, Long, Boolean>>> leftBuffer;
	private transient MapState<Long, List<Tuple3<T2, Long, Boolean>>> rightBuffer;

	private transient TimestampedCollector<OUT> collector;
	private transient ContextImpl context;

	private Watermark lastWatermark;

	/**
	 * Creates a new TimeBoundedStreamJoinOperator.
	 *
	 * @param lowerBound          The lower bound for evaluating if elements should be joined
	 * @param upperBound          The upper bound for evaluating if elements should be joined
	 * @param lowerBoundInclusive Whether or not to include elements where the timestamp matches
	 *                            the lower bound
	 * @param upperBoundInclusive Whether or not to include elements where the timestamp matches
	 *                            the upper bound
	 * @param udf                 A user-defined {@link TimeBoundedJoinFunction} that gets called
	 *                            whenever two elements of T1 and T2 are joined
	 */
	public TimeBoundedStreamJoinOperator(
			long lowerBound,
			long upperBound,
			boolean lowerBoundInclusive,
			boolean upperBoundInclusive,
			long bucketGranularity,
			TypeSerializer<T1> leftTypeSerializer,
			TypeSerializer<T2> rightTypeSerializer,
			TimeBoundedJoinFunction<T1, T2, OUT> udf) {

		super(udf);
		Preconditions.checkNotNull(udf);
		Preconditions.checkArgument(lowerBound <= upperBound, "lowerBound <= upperBound must be fulfilled");

		this.lowerBound = lowerBound;
		this.upperBound = upperBound;

		this.inverseLowerBound = -1 * upperBound;
		this.inverseUpperBound = -1 * lowerBound;

		this.lowerBoundInclusive = lowerBoundInclusive;
		this.upperBoundInclusive = upperBoundInclusive;
		this.leftTypeSerializer = Preconditions.checkNotNull(leftTypeSerializer);
		this.rightTypeSerializer = Preconditions.checkNotNull(rightTypeSerializer);

		Preconditions.checkArgument(bucketGranularity > 0, "bucket size must be greater than zero");
		this.bucketGranularity = bucketGranularity;
	}

	@Override
	public void open() throws Exception {
		super.open();
		collector = new TimestampedCollector<>(output);

		context = new ContextImpl(userFunction);

		@SuppressWarnings("unchecked")
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

		@SuppressWarnings("unchecked")
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

		T1 leftValue = record.getValue();
		long leftTs = record.getTimestamp();

		long min = calculateBucket(leftTs + lowerBound);
		long max = calculateBucket(leftTs + upperBound);

		if (leftTs == Long.MIN_VALUE) {
			throw new RuntimeException("Time-bounded stream joins need to have timestamps " +
				"assigned to elements, but current element has timestamp Long.MIN_VALUE");
		}

		if (dataIsLate(leftTs)) {
			return;
		}

		addToLeftBuffer(leftValue, leftTs);

		for (long i = min; i <= max; i += bucketGranularity) {
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

		registerCleanupTimer();

	}

	private void registerCleanupTimer() {
		if (this.lastWatermark == null) {
			return;
		}

		long triggerTime = this.lastWatermark.getTimestamp() + 1;

		getInternalTimerService(CLEANUP_TIMER_NAMESPACE, StringSerializer.INSTANCE, this)
			.registerEventTimeTimer(CLEANUP_TIMER_NAMESPACE, triggerTime);
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

		long min = calculateBucket(rightTs + inverseLowerBound);
		long max = calculateBucket(rightTs + inverseUpperBound);

		addToRightBuffer(rightElem, rightTs);

		if (rightTs == Long.MIN_VALUE) {
			throw new RuntimeException("Time-bounded stream joins need to have timestamps " +
				"assigned to elements, but current element has timestamp Long.MIN_VALUE");
		}

		if (dataIsLate(rightTs)) {
			return;
		}

		for (long i = min; i <= max; i += bucketGranularity) {
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

		registerCleanupTimer();
	}

	private boolean dataIsLate(long rightTs) {
		return lastWatermark != null && rightTs < lastWatermark.getTimestamp() - getWatermarkDelay();
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {

		// We can not clean our state here directly because we are not in a keyed context. Instead
		// we set a field containing the last watermark that we have seen, and for every element in
		// processElement1(...) / processElement2(...) we register a timer with time: watermark + 1
		// This watermark + 1 will then trigger the onEventTime(...) method for the next watermark,
		// where we are in a keyed context again, which we can use to clean up our state.
		this.lastWatermark = mark;

		if (timeServiceManager != null) {
			timeServiceManager.advanceWatermark(mark);
		}

		// emit the watermark with the calculated delay, so we don't produce late data
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

		lastCleanUpRight = calculateBucket(lastCleanUpRight);
		maxCleanup = calculateBucket(maxCleanup);

		// Notice that we are not cleaning up the bucket with value maxCleanup, because it might
		// contain entries that are still needed for a join operation. Instead we keep some perhaps
		// un-needed values and do the cleanup of what is now maxCleanup the next time

		// remove elements from leftBuffer in range [lastCleanUpRight, maxCleanup)
		for (long i = lastCleanUpRight; i < maxCleanup; i += bucketGranularity) {
			leftBuffer.remove(i);
		}

		lastCleanupRightBuffer.update(maxCleanup);
	}

	private void removeFromRhsUntil(long maxCleanup) throws Exception {

		Long lastCleanupLeft = lastCleanupLeftBuffer.value();
		if (lastCleanupLeft == null) {
			lastCleanupLeft = 0L;
		}

		lastCleanupLeft = calculateBucket(lastCleanupLeft);
		maxCleanup = calculateBucket(maxCleanup);

		// Notice that we are not cleaning up the bucket with value maxCleanup, because it might
		// contain entries that are still needed for a join operation. Instead we keep some perhaps
		// un-needed values and do the cleanup of what is now maxCleanup the next time

		// remove elements from leftBuffer in range [lastCleanUpLeft, maxCleanup)
		for (long i = lastCleanupLeft; i < maxCleanup; i += bucketGranularity) {
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
		return Math.floorDiv(ts, bucketGranularity) * bucketGranularity;
	}

	private long getWatermarkDelay() {
		return (upperBound < 0) ? 0 : upperBound;
	}

	@Override
	public void onEventTime(InternalTimer<K, String> timer) throws Exception {

		if (timer.getNamespace().equals(CLEANUP_TIMER_NAMESPACE)) {

			// remove from both sides all those elements where the timestamp is less than the lower
			// bound, because they are not considered for joining anymore
			removeFromLhsUntil(timer.getTimestamp() + inverseLowerBound);
			removeFromRhsUntil(timer.getTimestamp() + lowerBound);
		}
	}

	@Override
	public void onProcessingTime(InternalTimer<K, String> timer) throws Exception {
		// do nothing
	}

	private class ContextImpl extends TimeBoundedJoinFunction<T1, T2, OUT>.Context {

		private long leftTs;
		private long rightTs;

		private ContextImpl(TimeBoundedJoinFunction<T1, T2, OUT> func) {
			func.super();
		}

		@Override
		public long getLeftTimestamp() {
			return leftTs;
		}

		@Override
		public long getRightTimestamp() {
			return rightTs;
		}

		@Override
		public long getTimestamp() {
			return leftTs;
		}

		@Override
		public <X> void output(OutputTag<X> outputTag, X value) {
			Preconditions.checkArgument(outputTag != null, "OutputTag must not be null");
			output.collect(outputTag, new StreamRecord<>(value, getTimestamp()));
		}
	}

	@VisibleForTesting
	protected MapState<Long, List<Tuple3<T1, Long, Boolean>>> getLeftBuffer() {
		return leftBuffer;
	}

	@VisibleForTesting
	protected MapState<Long, List<Tuple3<T2, Long, Boolean>>> getRightBuffer() {
		return rightBuffer;
	}
}
