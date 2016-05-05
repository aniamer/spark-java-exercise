package eg.excercise.log.helpers;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Before;
import org.junit.Test;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;

import eg.excercise.log.input.LogItem;
import scala.Tuple2;

public class HelperFunctionsTest extends SharedJavaSparkContext {
	List<LogItem> logs = new ArrayList<LogItem>(10);
	List<Tuple2<String, Double>> mapMinuteResult = new ArrayList<Tuple2<String, Double>>();
	List<Tuple2<String, Double>> mapHourResult = new ArrayList<Tuple2<String, Double>>();
	JavaRDD<LogItem> itemsRDD ;
	@Before
	public void setUp() {
		// input data
		logs.add(new LogItem("2012-02-03", "20:11:35", "SampleClass4", "[DEBUG]", " verbose detail for id 66797536"));
		logs.add(new LogItem("2012-02-03", "20:11:35", "SampleClass4", "[DEBUG]", " verbose detail for id 66797536"));
		logs.add(new LogItem("2012-02-03", "20:11:37", "SampleClass7", "[DEBUG]", " verbose detail for id 66797536"));
		logs.add(new LogItem("2012-02-03", "20:11:37", "SampleClass8", "[DEBUG]", " verbose detail for id 66797536"));
		logs.add(new LogItem("2012-02-03", "20:11:39", "SampleClass8", "[DEBUG]", " verbose detail for id 66797536"));
		logs.add(new LogItem("2012-02-03", "20:11:30", "SampleClass8", "[DEBUG]", " verbose detail for id 66797536"));
		logs.add(new LogItem("2012-02-03", "20:11:41", "SampleClass3", "[DEBUG]", " verbose detail for id 66797536"));
		logs.add(new LogItem("2012-02-03", "20:11:42", "SampleClass1", "[DEBUG]", " verbose detail for id 66797536"));

		// result data

		mapMinuteResult.add(new Tuple2<String, Double>("2011:SampleClass4", 1.0));
		mapMinuteResult.add(new Tuple2<String, Double>("2011:SampleClass4", 1.0));
		mapMinuteResult.add(new Tuple2<String, Double>("2011:SampleClass7", 1.0));
		mapMinuteResult.add(new Tuple2<String, Double>("2011:SampleClass8", 1.0));
		mapMinuteResult.add(new Tuple2<String, Double>("2011:SampleClass8", 1.0));
		mapMinuteResult.add(new Tuple2<String, Double>("2011:SampleClass8", 1.0));
		mapMinuteResult.add(new Tuple2<String, Double>("2011:SampleClass3", 1.0));
		mapMinuteResult.add(new Tuple2<String, Double>("2011:SampleClass1", 1.0));

		mapHourResult.add(new Tuple2<String, Double>("20:SampleClass4", 1.0));
		mapHourResult.add(new Tuple2<String, Double>("20:SampleClass4", 1.0));
		mapHourResult.add(new Tuple2<String, Double>("20:SampleClass7", 1.0));
		mapHourResult.add(new Tuple2<String, Double>("20:SampleClass8", 1.0));
		mapHourResult.add(new Tuple2<String, Double>("20:SampleClass8", 1.0));
		mapHourResult.add(new Tuple2<String, Double>("20:SampleClass8", 1.0));
		mapHourResult.add(new Tuple2<String, Double>("20:SampleClass3", 1.0));
		mapHourResult.add(new Tuple2<String, Double>("20:SampleClass1", 1.0));
		itemsRDD = jsc().parallelize(logs);
	}

	@Test
	public void testCountLogEntryByMinuteKey() {
		
		JavaPairRDD<String, Double> mappedPairs = itemsRDD.mapToPair(HelperFunctions.countLogEntryByMinuteKey());
		List<Tuple2<String, Double>> collect = mappedPairs.collect();

		assertThat(collect, equalTo(mapMinuteResult));

	}

	@Test
	public void testCountLogEntryByHourKey() {
		JavaPairRDD<String, Double> mappedPairs = itemsRDD.mapToPair(HelperFunctions.countLogEntryPerHour());
		List<Tuple2<String, Double>> collect = mappedPairs.collect();

		assertThat(collect, equalTo(mapHourResult));

	}


}
