package eg.excercise.log;
import org.apache.spark.Accumulator;
import org.junit.Before;
import org.junit.Test;

import eg.excercise.log.input.LogItem;

public class LogItemTest {
	Accumulator<Integer> mock ;
	@Before
	public void setUp(){
		
	}
	@Test
	public void test() {
		LogItem item = LogEntryValidator.parseLogEntry("2012-02-03 18:35:34 SampleClass4 [FATAL] system problem at id 1991281254");
		System.out.println(item.date+" "+item.time+" "+item.component+" "+item.level+" "+item.message);
	}

}
