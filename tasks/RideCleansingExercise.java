package com.ververica.flinktraining.exercises.datastream_java.basics;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Оставляет только поездки, начинающиеся И заканчивающиеся в пределах NYC.
 *
 * Опции командной строки:
 *   --input  <path>   CSV‑файл с поездками  (по умолчанию demo‑датасет)
 */
public class RideCleansingExercise extends ExerciseBase {

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		String input = params.get("input", ExerciseBase.pathToRideData);

		final int maxEventDelay      = 60;
		final int servingSpeedFactor = 600;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<TaxiRide> rides = env.addSource(
				rideSourceOrTest(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor)));

		DataStream<TaxiRide> nycRides = rides
				.filter(ride ->
						GeoUtils.isInNYC(ride.startLon, ride.startLat) &&
								GeoUtils.isInNYC(ride.endLon,   ride.endLat));

		printOrTest(nycRides);

		env.execute("Taxi‑Ride Cleansing");
	}
}
