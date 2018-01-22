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

	// TODO: Make Valuestate
	private ValueState<Long> lastCleanupLhs;
	private ValueState<Long> lastCleanupRhs;


	//	 TODO: Rename this?
	private MapState<Long, List<Tuple3<T1, Long, Boolean>>> lhs;
	private MapState<Long, List<Tuple3<T2, Long, Boolean>>> rhs;

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

		this.lhs = getRuntimeContext().getMapState(new MapStateDescriptor<>(
			"lhs",
			LONG_TYPE_INFO,
			TypeInformation.of(new TypeHint<List<Tuple3<T1, Long, Boolean>>>() {
			})
		));

		this.rhs = getRuntimeContext().getMapState(new MapStateDescriptor<>(
			"rhs",
			LONG_TYPE_INFO,
			TypeInformation.of(new TypeHint<List<Tuple3<T2, Long, Boolean>>>() {
			})
		));

		this.lastCleanupLhs = getRuntimeContext().getState(new ValueStateDescriptor<>(
			"lastCleanupLhs",
			LONG_TYPE_INFO
		));

		this.lastCleanupRhs = getRuntimeContext().getState(new ValueStateDescriptor<>(
			"lastCleanupRhs",
			LONG_TYPE_INFO
		));


	}

	@Override
	public void processElement1(T1 value,
								Context ctx,
								Collector<Tuple2<T1, T2>> out) throws Exception {

		ctx.timerService().registerEventTimeTimer(ctx.timestamp());

		addToLhsBuffer(value, ctx.timestamp());
		List<Tuple3<T2, Long, Boolean>> candidates = getJoinCandidatesForLhs(ctx.timestamp());

		for (Tuple3<T2, Long, Boolean> candidate : candidates) {

			long lhsTs = ctx.timestamp();
			long rhsTs = candidate.f1;

			long lowerBound = lhsTs + this.lowerBound;
			long upperBound = lhsTs + this.upperBound;

			if (isInBoundsRhs(rhsTs, lowerBound, upperBound)) {
				Tuple2<T1, T2> joinedTuple = Tuple2.of(value, candidate.f0);

				// TODO: Adapt timestamp strategy
				((TimestampedCollector<Tuple2<T1, T2>>) out).setAbsoluteTimestamp(ctx.timestamp());
				out.collect(joinedTuple);
			}
		}
	}

	private void removeFromLhsUntil(long maxCleanup) throws Exception {

		// setup state
		if (this.lastCleanupLhs.value() == null) {
			this.lastCleanupLhs.update(0L);
		}

		// remove elements from lhs in range [lastValue, maxCleanup]
		for (long i = lastCleanupLhs.value(); i <= maxCleanup; i++) {
			lhs.remove(i);
		}

		lastCleanupLhs.update(maxCleanup);
	}

	private void removeFromRhsUntil(long maxCleanup) throws Exception {

		// setup state
		if (this.lastCleanupRhs.value() == null) {
			this.lastCleanupRhs.update(0L);
		}

		// remove elements from rhs in range [lastValue, maxCleanup]
		for (long i = lastCleanupRhs.value(); i <= maxCleanup; i++) {
			rhs.remove(i);
		}

		lastCleanupRhs.update(maxCleanup);
	}

	private boolean isInBoundsRhs(long rhsTs,
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

	private boolean isInBoundsLhs(long lhsTs,
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

	private List<Tuple3<T2, Long, Boolean>> getJoinCandidatesForLhs(long ts) throws Exception {

		long min = ts + lowerBound;
		long max = ts + upperBound;

		List<Tuple3<T2, Long, Boolean>> candidates = new LinkedList<>();

		// TODO: Adapt to granularity here
		for (long i = min; i <= max; i++) {
			List<Tuple3<T2, Long, Boolean>> fromBucket = rhs.get(i);
			if (fromBucket != null) {
				candidates.addAll(fromBucket);
			}
		}

		return candidates;
	}

	private List<Tuple3<T1, Long, Boolean>> getJoinCandidatesForRhs(long ts) throws Exception {

		long min = ts + inverseLowerBound;
		long max = ts + inverseUpperBound;

		List<Tuple3<T1, Long, Boolean>> candidates = new LinkedList<>();

		// TODO: Adapt to different bucket sizes here
		for (long i = min; i <= max; i++) {
			List<Tuple3<T1, Long, Boolean>> fromBucket = lhs.get(i);
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

		addToRhsBuffer(value, ctx.timestamp());
		ctx.timerService().registerEventTimeTimer(ctx.timestamp());

		List<Tuple3<T1, Long, Boolean>> candidates = getJoinCandidatesForRhs(ctx.timestamp());

		for (Tuple3<T1, Long, Boolean> candidate : candidates) {

			long rhsTs = ctx.timestamp();
			long lhsTs = candidate.f1;


			long lowerBound = rhsTs + inverseLowerBound;
			long upperBound = rhsTs + inverseUpperBound;

			if (isInBoundsLhs(lhsTs, lowerBound, upperBound)) {
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

	private void addToLhsBuffer(T1 value, long ts) throws Exception {
		long bucket = calculateBucket(ts);
		Tuple3<T1, Long, Boolean> elem = Tuple3.of(
			value, // actual value
			ts,    // actual timestamp
			false  // has been joined
		);

		List<Tuple3<T1, Long, Boolean>> elemsInBucket = lhs.get(bucket);
		if (elemsInBucket == null) {
			elemsInBucket = new ArrayList<>();
		}
		elemsInBucket.add(elem);
		lhs.put(bucket, elemsInBucket);
	}

	private void addToRhsBuffer(T2 value, long ts) throws Exception {
		long bucket = calculateBucket(ts);
		Tuple3<T2, Long, Boolean> elem = Tuple3.of(
			value, // actual value
			ts,    // actual timestamp
			false  // has been joined
		);

		List<Tuple3<T2, Long, Boolean>> elemsInBucket = rhs.get(bucket);
		if (elemsInBucket == null) {
			elemsInBucket = new ArrayList<>();
		}
		elemsInBucket.add(elem);
		rhs.put(bucket, elemsInBucket);
	}

	private long calculateBucket(long ts) {
		return Math.floorDiv(ts, bucketGranularity);
	}

	public long getWatermarkDelay() {
		// TODO: Adapt this to when we use the rhs or min timestamp
		return (upperBound < 0) ? 0 : upperBound;
	}

	@VisibleForTesting
	public MapState<Long, List<Tuple3<T1, Long, Boolean>>> getLhs() {
		return lhs;
	}

	@VisibleForTesting
	public MapState<Long, List<Tuple3<T2, Long, Boolean>>> getRhs() {
		return rhs;
	}
}
