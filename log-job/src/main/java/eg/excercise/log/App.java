package eg.excercise.log;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eg.excercise.log.output.LogStats;

/**
 * Log job entry point
 */
public class App {
	private static Logger logger = LoggerFactory.getLogger(App.class);

	public static void main(String[] args) {
		String filePath = null;
		SparkConf sparkConf = new SparkConf();
		
		if (args.length > 0 && args[0] != null)
			filePath = args[0];
		else {
			logger.error("filePath not provided please provide the file path as an argument to the job");
			return;
		}
		// checks if the job was called via spark-submit
		if (sparkConf.get("spark.master", null) == null) {

			sparkConf.setMaster("local[4]").setAppName("Log aggregator");

		}
		// initializing the context in a try will release the resources of the
		// context once the job is done
		try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
			// get the input text file using 1 partition for simplicity
			JavaRDD<String> inputRDD = sparkContext.textFile(filePath, 1);
			final Accumulator<Integer> corruptLogEntries = sparkContext.accumulator(0);
			LogStats logStats = LogStatsJob.runJob(inputRDD,corruptLogEntries);

			// logging results
			logger.info(logStats.toString());
			
		}

	}

}
