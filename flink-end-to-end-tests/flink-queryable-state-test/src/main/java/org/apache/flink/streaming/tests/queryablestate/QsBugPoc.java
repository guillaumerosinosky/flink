/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.tests.queryablestate;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.util.Random;

/**
 * Streaming application that creates an {@link Email} pojo with random ids and increasing
 * timestamps and passes it to a stateful {@link org.apache.flink.api.common.functions.FlatMapFunction},
 * where it is exposed as queryable state.
 */
public class QsBugPoc {

	public static final String QUERYABLE_STATE_NAME = "state";
	public static final String STATE_NAME = "state";

	public static void main(final String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		StateBackend stateBackend = new RocksDBStateBackend("file:///tmp/deleteme-rocksdb");
		env.setStateBackend(stateBackend);
		env.enableCheckpointing(10000);
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);

		env.addSource(new EmailSource())
			.keyBy(new KeySelector<Email, String>() {

				private static final long serialVersionUID = -1480525724620425363L;

				@Override
				public String getKey(Email value) throws Exception {
					return value.getDate();
				}
			})
			.flatMap(new MyFlatMap());

		env.execute();
	}

	private static class EmailSource implements SourceFunction<Email> {

		private static final long serialVersionUID = -7286937645300388040L;

		private Random random;
		private boolean isRunning = true;

		@Override
		public void run(SourceContext<Email> ctx) throws Exception {
			// Sleep for 10 seconds on start to allow time to copy jobid
			for (int i = 0; i < 100; i++) {
				if (!isRunning) {
					break;
				}

				Thread.sleep(100);
			}

			while (isRunning) {
				final EmailId emailId = new EmailId(Integer.toString(getRandom().nextInt()));
				final Instant timestamp = Instant.now().minus(Duration.ofDays(1));
				final String foo = String.format("foo #%d", getRandom().nextInt(100));
				final LabelSurrogate label = new LabelSurrogate(LabelSurrogate.Type.BAR, "bar");
				ctx.collect(new Email(emailId, timestamp, foo, label));
				Thread.sleep(getRandom().nextInt(100));
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}

		private Random getRandom() {
			if (random == null) {
				random = new Random();
			}

			return random;
		}
	}

	private static class MyFlatMap extends RichFlatMapFunction<Email, Object> {
		private transient MapState<EmailId, EmailInformation> state;

		@Override
		public void flatMap(Email value, Collector<Object> out) throws Exception {
			state.put(value.getEmailId(), new EmailInformation(value));
		}

		@Override
		public void open(Configuration parameters) {
			MapStateDescriptor<EmailId, EmailInformation> stateDescriptor = new MapStateDescriptor<>(
				STATE_NAME,
				TypeInformation.of(new TypeHint<EmailId>() {
				}),
				TypeInformation.of(new TypeHint<EmailInformation>() {
				})
			);

			stateDescriptor.setQueryable(QUERYABLE_STATE_NAME);

			state = getRuntimeContext().getMapState(stateDescriptor);
		}
	}
}
