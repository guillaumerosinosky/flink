package org.apache.flink.streaming.api.functions;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.util.Collector;

/**
 * TODO: Add JavaDoc.
 * @param <I>
 * @param <O>
 */
public abstract class JoinedProcessFunction<I, O> extends AbstractRichFunction {

	/**
	 * TODO: Add JavaDoc.
	 * @param value
	 * @param ctx
	 * @param out
	 * @throws Exception
	 */
	public abstract void processElement(I value, TimeBoundedStreamJoinOperator.Context ctx, Collector<O> out) throws Exception;

}
