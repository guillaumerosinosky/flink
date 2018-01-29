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
	public abstract void processElement(I value, Context ctx, Collector<O> out) throws Exception;

	/**
	 * The Context that gets passed to processElement.
	 */
	public abstract class Context {

		/**
		 * @return The timestamp of the left element that resulted in a join
		 */
		public abstract long getLeftTimestamp();

		/**
		 * @return The timestamp of the right element that resulted in a join
		 */
		public abstract long getRightTimestamp();

		/**
		 * @return The timestamp of the joined tuple
		 */
		public abstract long getTimestamp();
	}
}
