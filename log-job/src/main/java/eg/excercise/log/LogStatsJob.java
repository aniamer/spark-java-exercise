package eg.excercise.log;

import static eg.excercise.log.helpers.HelperFunctions.countLogEntryByMinuteKey;
import static eg.excercise.log.helpers.HelperFunctions.countLogEntryPerHour;
import static eg.excercise.log.helpers.HelperFunctions.isDebug;

import java.util.List;
import java.util.Map;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import eg.excercise.log.input.LogItem;
import eg.excercise.log.output.LogStats;
import scala.Tuple2;
/**
 * a class that represents an RDD job that parses a daily log file and produces stats about the log content
 *  
 * @author Mohamed Amer
 *
 */
public class LogStatsJob {

	public static LogStats runJob(JavaRDD<String> inputRDD, final Accumulator<Integer> corruptLogEntries) {
		// parse and validates the log entries
		JavaRDD<LogItem> clean = inputRDD.map(x -> {
			return LogEntryValidator.parseLogEntry(x);
		})
				// filters only the matched entries
				.filter(f -> {
					if (f == null) {
						corruptLogEntries.add(1);
						return false;
					} else {
						return true;
					}
				});
		clean.cache();
		JavaRDD<LogItem> filtered = clean
				// only debug entries will pass
				.filter(isDebug())
				// caching the rdd to memory for further usage
				.cache();

		JavaPairRDD<String, Double> debugLogsPerMinute = filtered
				// mapping a each entry as follows (1855:SampleClass4,1.0)
				// where the key is the minute:class and the value is
				// ClassName
				.mapToPair(countLogEntryByMinuteKey())
				// the sum of the mapped entries by the previously mentioned
				// key the result will look like
				// (1855:SampleClass4,11.0)
				.reduceByKey((a, b) -> a + b);

		// caching the rdd to memory for further usage
		debugLogsPerMinute.cache();
		JavaPairRDD<String, Double> debugLogsPerHour = filtered
				// mapping a each entry as follows (18:SampleClass4,1.0)
				// where the key is the hour:class and the value is ClassName
				.mapToPair(countLogEntryPerHour())
				// the sum of the mapped entries by the previously mentioned
				// key the result will look like (18:SampleClass4,15.0)
				.reduceByKey((a, b) -> a + b);
		debugLogsPerHour.cache();
		List<Tuple2<String, Double>> topClassPerMinute = debugLogsPerMinute
				// switching the key value pair so it would look like
				// (11.0,1855:SampleClass4) then sort the resulted map by
				// key in descending order
				.mapToPair(f -> new Tuple2<Double, String>(f._2, f._1)).sortByKey(false)
				// switching the key value pair back so it would look like
				// (1855:SampleClass4,11.0) then take the top 3
				.mapToPair(f -> new Tuple2<String, Double>(f._2, f._1)).take(3);

		List<Tuple2<String, Double>> topClassesPerHour = debugLogsPerHour
				// switching the key value pair so it would look like
				// (15.0,18:SampleClass4) then sort the resulted map by key
				// in descending order
				.mapToPair(f -> new Tuple2<Double, String>(f._2, f._1)).sortByKey(false)
				// switching the key value pair back so it would look like
				// (18:SampleClass4,11.0) then take the top 3
				.mapToPair(f -> new Tuple2<String, Double>(f._2, f._1)).take(3);

		
		
		LogStats logStats = new LogStats();

		// get the distribution of all the log level types using the first
		Map<String, Long> countByValue = clean.map(f -> f.level).countByValue();

		logStats.countByValue = countByValue;

		logStats.topClassPerMinute = topClassPerMinute;
		logStats.topClassesPerHour = topClassesPerHour;
		logStats.corruptLogEntries = corruptLogEntries.value();
		return logStats;

	}

}
