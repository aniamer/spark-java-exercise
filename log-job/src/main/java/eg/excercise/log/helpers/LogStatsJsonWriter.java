package eg.excercise.log.helpers;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;

import eg.excercise.log.output.LogStats;

public class LogStatsJsonWriter {
	private static final ObjectMapper mapper = new ObjectMapper();
	
	public static void writeLogStatsJson(LogStats logStats){
		try {
			mapper.writeValue(new File("logStats.txt"), logStats);
		} catch (IOException e) {		
			e.printStackTrace();
		}
	}
}
