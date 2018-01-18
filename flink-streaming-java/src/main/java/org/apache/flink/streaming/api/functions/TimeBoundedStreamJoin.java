package org.apache.flink.streaming.api.functions;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.util.Collector;

import java.util.*;

public class TimeBoundedStreamJoin<T1, T2> extends CoProcessFunction<T1, T2, Tuple2<T1, T2>> {

	private final long lowerBound;
	private final long upperBound;

	private final long inverseLowerBound;
	private final long inverseUpperBound;

	private final boolean lowerBoundInclusive;
	private final boolean upperBoundInclusive;

	private final long bucketGranularity = 1;

	// TODO: Make Valuestate
	private long lastCleanupLhs = 0;
	private long lastCleanupRhs = 0;


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
			TypeInformation.of(Long.class),
			TypeInformation.of(new TypeHint<List<Tuple3<T1, Long, Boolean>>>() {
			})
		));

		this.rhs = getRuntimeContext().getMapState(new MapStateDescriptor<>(
			"rhs",
			TypeInformation.of(Long.class),
			TypeInformation.of(new TypeHint<List<Tuple3<T2, Long, Boolean>>>() {
			})
		));
	}

	@Override
	public void processElement1(T1 value,
								Context ctx,
								Collector<Tuple2<T1, T2>> out) throws Exception {

		bufferLhs(value, ctx.timestamp());
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


		if (ctx.timerService().currentWatermark() == Long.MIN_VALUE) {
			return;
		} else {
			long maxCleanup = ctx.timerService().currentWatermark() - getWatermarkDelay();
			cleanupLhs(maxCleanup);
			printBuffers();
		}
	}

	private void cleanupLhs(long maxCleanup) throws Exception {

		if (maxCleanup < lastCleanupLhs) {
			return;
		}

		for (long i = lastCleanupLhs; i < maxCleanup; i++) {
			lhs.remove(i);
		}

		lastCleanupLhs = maxCleanup;
	}

	private void cleanupRhs(long maxCleanup) throws Exception {

		if (maxCleanup < lastCleanupLhs) {
			return;
		}

		for (long i = lastCleanupRhs; i < maxCleanup; i++) {
			rhs.remove(i);
		}

		lastCleanupLhs = maxCleanup;
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

		// TODO: Adapt to granularity here
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
		bufferRhs(value, ctx.timestamp());

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

		if (ctx.timerService().currentWatermark() == Long.MIN_VALUE) {
			return;
		} else {
			long maxCleanup = ctx.timerService().currentWatermark() - getWatermarkDelay();
			cleanupRhs(maxCleanup);
			printBuffers();
		}
	}

	private void printBuffers() throws Exception {
		System.out.println("|-- lhs --");
		Iterator<Map.Entry<Long, List<Tuple3<T1, Long, Boolean>>>> it = lhs.iterator();
		while (it.hasNext()) {
			Map.Entry<Long, List<Tuple3<T1, Long, Boolean>>> e = it.next();
			System.out.print("| " + e.getKey() + " -> ");
			for (Tuple3 tup : e.getValue()) {
				System.out.println(tup.f1);
			}
		}
		System.out.println("|-- end lhs --\n");

		System.out.println("|-- rhs --");

		Iterator<Map.Entry<Long, List<Tuple3<T2, Long, Boolean>>>> it1 = rhs.iterator();
		while (it1.hasNext()) {
			Map.Entry<Long, List<Tuple3<T2, Long, Boolean>>> e = it1.next();
			System.out.print("| " + e.getKey() + " -> ");
			for (Tuple3 tup : e.getValue()) {
				System.out.println(tup.f1);
			}
		}

		System.out.println("|-- end rhs --\n");

	}

	private void bufferLhs(T1 value, long ts) throws Exception {
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

	private void bufferRhs(T2 value, long ts) throws Exception {
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
		// TODO: Adapt this
		return (upperBound < 0) ? 0 : upperBound;
	}
}
