package com.ververica.flinktraining.exercises.datastream_java.windows;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class HourlyTipsExercise extends ExerciseBase {

	private static final int MAX_EVENT_DELAY      = 60;
	private static final int SERVING_SPEED_FACTOR = 600;
	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		String inputPath = params.get("input", pathToFareData);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(parallelism);

		DataStream<TaxiFare> fares = env.addSource(
				fareSourceOrTest(new TaxiFareSource(
						inputPath, MAX_EVENT_DELAY, SERVING_SPEED_FACTOR)));

		DataStream<Tuple3<Long, Long, Float>> hourlyMax = fares
				.keyBy(f -> f.driverId)
				.timeWindow(Time.hours(1))
				.process(new SumTipsPerDriver())
				.timeWindowAll(Time.hours(1))
				.maxBy(2);

		printOrTest(hourlyMax);
		env.execute("Hourly Tips (Java 8)");
	}

	public static class SumTipsPerDriver
			extends ProcessWindowFunction<
			TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {

		@Override
		public void process(
				Long driverId,
				Context ctx,
				Iterable<TaxiFare> fares,
				Collector<Tuple3<Long, Long, Float>> out) {

			float tipsSum = 0f;
			for (TaxiFare f : fares) {
				tipsSum += f.tip;
			}
			out.collect(new Tuple3<>(ctx.window().getEnd(), driverId, tipsSum));
		}
	}
}
