package com.ververica.flinktraining.exercises.datastream_java.state;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class RidesAndFaresExercise extends ExerciseBase {

	private static final int MAX_EVENT_DELAY     = 60;
	private static final int SERVING_SPEEDFACTOR = 1800;

	public static void main(String[] args) throws Exception {

		ParameterTool p  = ParameterTool.fromArgs(args);
		String rides    = p.get("rides", pathToRideData);
		String fares    = p.get("fares", pathToFareData);

		StreamExecutionEnvironment env =
				StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);
		env.enableCheckpointing(10_000L);

		DataStream<TaxiRide> rideStream = env
				.addSource(rideSourceOrTest(
						new TaxiRideSource(rides, MAX_EVENT_DELAY, SERVING_SPEEDFACTOR)))
				.filter((TaxiRide ride) -> (ride.isStart && (ride.rideId % 1000 != 0)))
				.keyBy(r -> r.rideId);

		DataStream<TaxiFare> fareStream = env
				.addSource(fareSourceOrTest(
						new TaxiFareSource(fares, MAX_EVENT_DELAY, SERVING_SPEEDFACTOR)))
				.keyBy(f -> f.rideId);

		DataStream<Tuple2<TaxiRide, TaxiFare>> enriched = rideStream
				.connect(fareStream)
				.flatMap(new EnrichmentFunction())
				.uid("enrichment");

		printOrTest(enriched);
		env.execute("Join Rides with Fares (Java 8 safe)");
	}

	public static class EnrichmentFunction
			extends RichCoFlatMapFunction<TaxiRide, TaxiFare,
			Tuple2<TaxiRide, TaxiFare>> {

		private static final long serialVersionUID = 1L;

		private transient ValueState<TaxiRide> rideState;
		private transient ValueState<TaxiFare> fareState;

		@Override
		public void open(Configuration parameters) {

			TypeInformation<TaxiRide> rideType =
					TypeExtractor.getForClass(TaxiRide.class);
			TypeInformation<TaxiFare> fareType =
					TypeExtractor.getForClass(TaxiFare.class);

			rideState = getRuntimeContext().getState(
					new ValueStateDescriptor<>("ride", rideType));

			fareState = getRuntimeContext().getState(
					new ValueStateDescriptor<>("fare", fareType));
		}

		@Override
		public void flatMap1(
				TaxiRide ride,
				Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {

			TaxiFare fare = fareState.value();
			if (fare != null) {
				fareState.clear();
				out.collect(Tuple2.of(ride, fare));
			} else {
				rideState.update(ride);
			}
		}

		@Override
		public void flatMap2(
				TaxiFare fare,
				Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {

			TaxiRide ride = rideState.value();
			if (ride != null) {
				rideState.clear();
				out.collect(Tuple2.of(ride, fare));
			} else {
				fareState.update(fare);
			}
		}
	}
}
