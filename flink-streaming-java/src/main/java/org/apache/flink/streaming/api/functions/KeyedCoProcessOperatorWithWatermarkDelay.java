package org.apache.flink.streaming.api.functions;

import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;

public class KeyedCoProcessOperatorWithWatermarkDelay<KEY, IN1, IN2, OUT>
	extends KeyedCoProcessOperator<KEY, IN1, IN2, OUT> {

	private long watermarkDelay;

	public KeyedCoProcessOperatorWithWatermarkDelay(CoProcessFunction<IN1, IN2, OUT> flatMapper,
													long watermarkDelay) {
		super(flatMapper);
		if (watermarkDelay < 0) {
			throw new IllegalArgumentException("The watermark delay should be non-negative");
		}
		this.watermarkDelay = watermarkDelay;
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		if (timeServiceManager != null) {
			timeServiceManager.advanceWatermark(mark);
		}

		if (watermarkDelay == 0) {
			emitWithoutDelay(mark);
		} else {
			emitWithDelay(mark);
		}
	}

	private void emitWithDelay(Watermark mark) {
		output.emitWatermark(new Watermark(mark.getTimestamp() - watermarkDelay));
	}

	private void emitWithoutDelay(Watermark mark) {
		output.emitWatermark(mark);
	}
}
