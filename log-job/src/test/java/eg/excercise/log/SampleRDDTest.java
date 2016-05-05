package eg.excercise.log;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;

import eg.excercise.log.helpers.LogStatsJsonWriter;
import eg.excercise.log.output.LogStats;


public class SampleRDDTest extends SharedJavaSparkContext{
	
	@Test
	public void test() throws InterruptedException {
	
		JavaRDD<String> textFile = jsc().textFile("src/test/resources/sample.log");
		final Accumulator<Integer> accumulator = jsc().accumulator(0);
		LogStats logStats = LogStatsJob.runJob(textFile,accumulator);
		
		System.out.println(logStats);
		LogStatsJsonWriter.writeLogStatsJson(logStats);
	}


}
