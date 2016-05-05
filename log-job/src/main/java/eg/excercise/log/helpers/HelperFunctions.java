package eg.excercise.log.helpers;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import eg.excercise.log.input.LogItem;
import scala.Tuple2;

public class HelperFunctions {

	/**
	 * 
	 * @return PairFunction<LogItem, String, Double> that creates a KV tuple where the key is hour:class and the value is a count of 1.0
	 */
	public static PairFunction<LogItem, String, Double> countLogEntryPerHour() {
		return f -> {
			String[] timeSplits = f.time.split(":");
			return new Tuple2<String, Double>(timeSplits[0] + ":" + f.component, 1.0);
		};
	}
	/**
	 * 
	 * @return a PairFunction<LogItem, String, Double> that creates a KV tuple where the key is min:class
	 */
	public static PairFunction<LogItem, String, Double> countLogEntryByMinuteKey() {
		return f -> {
			String[] timeSplits = f.time.split(":");
			return new Tuple2<String, Double>(timeSplits[0] + timeSplits[1] + ":" + f.component, 1.0);
		};
	}
	/**
	 * 
	 * @return a Function<LogItem, Boolean> that checks if a log entry is of a DEBUG level
	 */
	public static Function<LogItem, Boolean> isDebug() {
		return f -> f.level.equals("[DEBUG]");
	}
	
}
