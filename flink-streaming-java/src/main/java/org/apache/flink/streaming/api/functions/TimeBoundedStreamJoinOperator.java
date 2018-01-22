package org.apache.flink.streaming.api.functions;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO;

// TODO: Make bucket granularity adaptable
// TODO: Allow user to specify which ts to pick
public class TimeBoundedStreamJoinOperator<T1, T2>
	extends AbstractStreamOperator<Tuple2<T1, T2>>
	implements TwoInputStreamOperator<T1, T2, Tuple2<T1, T2>> {

	private final long lowerBound;
	private final long upperBound;

	private final long inverseLowerBound;
	private final long inverseUpperBound;

	private final boolean lowerBoundInclusive;
	private final boolean upperBoundInclusive;

	private final long bucketGranularity = 1;

	private ValueState<Long> lastCleanupRightBuffer;
	private ValueState<Long> lastCleanupLeftBuffer;

	private MapState<Long, List<Tuple3<T1, Long, Boolean>>> leftBuffer;
	private MapState<Long, List<Tuple3<T2, Long, Boolean>>> rightBuffer;

	private transient TimestampedCollector<Tuple2<T1, T2>> collector;

	public TimeBoundedStreamJoinOperator(long lowerBound,
										 long upperBound,
										 boolean lowerBoundInclusive,
										 boolean upperBoundInclusive) {

		this.lowerBound = lowerBound;
		this.upperBound = upperBound;

		this.inverseLowerBound = -1 * upperBound;
		this.inverseUpperBound = -1 * lowerBound;

		this.lowerBoundInclusive = lowerBoundInclusive;
		this.upperBoundInclusive = upperBoundInclusive;
	}

	@Override
	public void open() throws Exception {
		super.open();
		collector = new TimestampedCollector<>(output);

		this.leftBuffer = getRuntimeContext().getMapState(new MapStateDescriptor<>(
			"leftBuffer",
			LONG_TYPE_INFO,
			TypeInformation.of(new TypeHint<List<Tuple3<T1, Long, Boolean>>>() {
			})
		));

		this.rightBuffer = getRuntimeContext().getMapState(new MapStateDescriptor<>(
			"rightBuffer",
			LONG_TYPE_INFO,
			TypeInformation.of(new TypeHint<List<Tuple3<T2, Long, Boolean>>>() {
			})
		));

		this.lastCleanupRightBuffer = getRuntimeContext().getState(new ValueStateDescriptor<>(
			"lastCleanupRightBuffer",
			LONG_TYPE_INFO
		));

		this.lastCleanupLeftBuffer = getRuntimeContext().getState(new ValueStateDescriptor<>(
			"lastCleanupLeftBuffer",
			LONG_TYPE_INFO
		));
	}

	@Override
	public void processElement1(StreamRecord<T1> record) throws Exception {

		long leftTs = record.getTimestamp();
		T1 leftValue = record.getValue();

		addToLeftBuffer(leftValue, leftTs);
		List<Tuple3<T2, Long, Boolean>> candidates = getJoinCandidatesForLeftElement(leftTs);

		for (Tuple3<T2, Long, Boolean> rightCandidate : candidates) {

			long rightTs = rightCandidate.f1;

			long lowerBound = leftTs + this.lowerBound;
			long upperBound = leftTs + this.upperBound;

			if (isRightElemInBounds(rightTs, lowerBound, upperBound)) {
				this.collect(leftValue, rightCandidate.f0, leftTs);
			}
		}
	}

	@Override
	public void processElement2(StreamRecord<T2> record) throws Exception {

		long rightTs = record.getTimestamp();
		T2 rightElem = record.getValue();

		addToRightBuffer(rightElem, rightTs);

		List<Tuple3<T1, Long, Boolean>> candidates = getJoinCandidatesForRightElement(rightTs);

		for (Tuple3<T1, Long, Boolean> leftElem : candidates) {

			long leftTs = leftElem.f1;
			long lowerBound = rightTs + inverseLowerBound;
			long upperBound = rightTs + inverseUpperBound;

			if (isLeftElemInBounds(leftTs, lowerBound, upperBound)) {
				this.collect(leftElem.f0, rightElem, leftTs);
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

	private void collect(T1 left, T2 right, long ts) {
		Tuple2<T1, T2> out = Tuple2.of(left, right);
		this.collector.setAbsoluteTimestamp(ts);
		this.collector.collect(out);
	}

	private void removeFromLhsUntil(long maxCleanup) throws Exception {

		// setup state
		if (this.lastCleanupRightBuffer.value() == null) {
			this.lastCleanupRightBuffer.update(0L);
		}

		// remove elements from leftBuffer in range [lastValue, maxCleanup]
		for (long i = lastCleanupRightBuffer.value(); i <= maxCleanup; i++) {
			leftBuffer.remove(i);
		}

		lastCleanupRightBuffer.update(maxCleanup);
	}

	private void removeFromRhsUntil(long maxCleanup) throws Exception {

		// setup state
		if (this.lastCleanupLeftBuffer.value() == null) {
			this.lastCleanupLeftBuffer.update(0L);
		}

		// remove elements from rightBuffer in range [lastValue, maxCleanup]
		for (long i = lastCleanupLeftBuffer.value(); i <= maxCleanup; i++) {
			rightBuffer.remove(i);
		}

		lastCleanupLeftBuffer.update(maxCleanup);
	}

	private boolean isRightElemInBounds(long rhsTs,
										long lowerBound,
										long upperBound) {


		if (lowerBoundInclusive && upperBoundInclusive) {
			return lowerBound <= rhsTs && rhsTs <= upperBound;
		} else if (lowerBoundInclusive && !upperBoundInclusive) {
			return lowerBound <= rhsTs && rhsTs < upperBound;
		} else if (!lowerBoundInclusive && upperBoundInclusive) {
			return lowerBound < rhsTs && rhsTs <= upperBound;
		} else if (!lowerBoundInclusive && !upperBoundInclusive) {
			return lowerBound < rhsTs && rhsTs < upperBound;
		} else {
			// TODO: What to do here?
			throw new RuntimeException("");
		}

	}

	private boolean isLeftElemInBounds(long lhsTs,
									   long inverseLowerBound,
									   long inverseUpperBound) {

		boolean invLowerBoundInclusive = upperBoundInclusive;
		boolean invUpperBoundInclusive = lowerBoundInclusive;

		if (invLowerBoundInclusive && invUpperBoundInclusive) {
			return inverseLowerBound <= lhsTs && lhsTs <= inverseUpperBound;
		} else if (!invLowerBoundInclusive && invUpperBoundInclusive) {
			return inverseLowerBound < lhsTs && lhsTs <= inverseUpperBound;
		} else if (invLowerBoundInclusive && !invUpperBoundInclusive) {
			return inverseLowerBound <= lhsTs && lhsTs < inverseUpperBound;
		} else if (!invLowerBoundInclusive && !invUpperBoundInclusive) {
			return inverseLowerBound < lhsTs && lhsTs < inverseUpperBound;
		} else {
			// TODO: What to do here?
			throw new InvalidProgramException("");
		}
	}

	private List<Tuple3<T2, Long, Boolean>> getJoinCandidatesForLeftElement(long ts) throws Exception {

		long min = ts + lowerBound;
		long max = ts + upperBound;

		List<Tuple3<T2, Long, Boolean>> candidates = new LinkedList<>();

		// TODO: Adapt to granularity here
		for (long i = min; i <= max; i++) {
			List<Tuple3<T2, Long, Boolean>> fromBucket = rightBuffer.get(i);
			if (fromBucket != null) {
				candidates.addAll(fromBucket);
			}
		}

		return candidates;
	}

	private List<Tuple3<T1, Long, Boolean>> getJoinCandidatesForRightElement(long ts) throws Exception {

		long min = ts + inverseLowerBound;
		long max = ts + inverseUpperBound;

		List<Tuple3<T1, Long, Boolean>> candidates = new LinkedList<>();

		// TODO: Adapt to different bucket sizes here
		for (long i = min; i <= max; i++) {
			List<Tuple3<T1, Long, Boolean>> fromBucket = leftBuffer.get(i);
			if (fromBucket != null) {
				candidates.addAll(fromBucket);
			}
		}

		return candidates;
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

	@VisibleForTesting
	public MapState<Long, List<Tuple3<T1, Long, Boolean>>> getLeftBuffer() {
		return leftBuffer;
	}

	@VisibleForTesting
	public MapState<Long, List<Tuple3<T2, Long, Boolean>>> getRightBuffer() {
		return rightBuffer;
	}
}
