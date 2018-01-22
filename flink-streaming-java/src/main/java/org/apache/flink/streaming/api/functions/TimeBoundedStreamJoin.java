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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO;

// TODO: Make bucket granularity adaptable
// TODO: Allow user to specify which ts to pick
public class TimeBoundedStreamJoin<T1, T2> extends CoProcessFunction<T1, T2, Tuple2<T1, T2>> {

	private final long lowerBound;
	private final long upperBound;

	private final long inverseLowerBound;
	private final long inverseUpperBound;

	private final boolean lowerBoundInclusive;
	private final boolean upperBoundInclusive;

	private final long bucketGranularity = 1;

	private ValueState<Long> lastCleanupRightBuffer;
	private ValueState<Long> lastCleanupLeftBuffer;

	//	 TODO: Rename this?
	private MapState<Long, List<Tuple3<T1, Long, Boolean>>> leftBuffer;
	private MapState<Long, List<Tuple3<T2, Long, Boolean>>> rightBuffer;

	public TimeBoundedStreamJoin(long lowerBound,
								 long upperBound,
								 boolean lowerBoundInclusive,
								 boolean upperBoundInclusive) {

		this.lowerBound = lowerBound;
		this.upperBound = upperBound;

		this.inverseLowerBound = -1 * upperBound;
		this.inverseUpperBound = -1 * lowerBound;

		this.lowerBoundInclusive = lowerBoundInclusive;
		this.upperBoundInclusive = upperBoundInclusive;
		// TODO: Add logic for bounds inclusion
		// TODO: Add inverse logic for bounds inclusion
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

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
	public void processElement1(T1 value,
								Context ctx,
								Collector<Tuple2<T1, T2>> out) throws Exception {

		ctx.timerService().registerEventTimeTimer(ctx.timestamp());

		addToLeftBuffer(value, ctx.timestamp());
		List<Tuple3<T2, Long, Boolean>> candidates = getJoinCandidatesForLeftElement(ctx.timestamp());

		for (Tuple3<T2, Long, Boolean> candidate : candidates) {

			long leftTs = ctx.timestamp();
			long rightTs = candidate.f1;

			long lowerBound = leftTs + this.lowerBound;
			long upperBound = leftTs + this.upperBound;

			if (isRightElemInBounds(rightTs, lowerBound, upperBound)) {
				Tuple2<T1, T2> joinedTuple = Tuple2.of(value, candidate.f0);

				// TODO: Adapt timestamp strategy
				((TimestampedCollector<Tuple2<T1, T2>>) out).setAbsoluteTimestamp(ctx.timestamp());
				out.collect(joinedTuple);
			}
		}
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

	@Override
	public void processElement2(T2 value,
								Context ctx,
								Collector<Tuple2<T1, T2>> out) throws Exception {

		addToRightBuffer(value, ctx.timestamp());
		ctx.timerService().registerEventTimeTimer(ctx.timestamp());

		List<Tuple3<T1, Long, Boolean>> candidates = getJoinCandidatesForRightElement(ctx.timestamp());

		for (Tuple3<T1, Long, Boolean> candidate : candidates) {

			long rightTs = ctx.timestamp();
			long leftTs = candidate.f1;


			long lowerBound = rightTs + inverseLowerBound;
			long upperBound = rightTs + inverseUpperBound;

			if (isLeftElemInBounds(leftTs, lowerBound, upperBound)) {
				Tuple2<T1, T2> joinedTuple = Tuple2.of(candidate.f0, value);
				// TODO: Adapt timestamp strategy
				((TimestampedCollector<Tuple2<T1, T2>>) out).setAbsoluteTimestamp(candidate.f1);
				out.collect(joinedTuple);
			}
		}
	}

	@Override
	public void onTimer(long timestamp,
						OnTimerContext ctx,
						Collector<Tuple2<T1, T2>> out) throws Exception {

		// remove from both sides all those elements where the timestamp is less than the lower
		// bound, because they are not considered for joining anymore
		removeFromLhsUntil(timestamp + inverseLowerBound);
		removeFromRhsUntil(timestamp + lowerBound);
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
