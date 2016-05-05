package eg.excercise.log.output;

import java.util.List;
import java.util.Map;

import scala.Tuple2;

public class LogStats {
	public Map<String, Long> countByValue;
	public List<Tuple2<String, Double>> topClassPerMinute;
	public List<Tuple2<String, Double>> topClassesPerHour;
	private final static StringBuffer stringBuffer = new StringBuffer();
	public Integer corruptLogEntries;
	@Override
	public String toString() {
		stringBuffer.setLength(0);
		stringBuffer.append("\nTop "+topClassPerMinute.size()+" debug level generating classes(s) per minute \n");
		for(Tuple2<String, Double> object:topClassPerMinute){
			String[] minClass = object._1.split(":");

			stringBuffer.append(minClass[1]+"\t"+object._2+"\n");
		}
		stringBuffer.append("Top "+topClassesPerHour.size()+" debug level generating classes(s) per hour \n");
		for(Tuple2<String, Double> object:topClassesPerHour){
			String[] hourClass = object._1.split(":");

			stringBuffer.append(hourClass[1]+"\t"+object._2+"\n");
		}
		stringBuffer.append("Log level distribution through the provided file"+
				countByValue+"\n");
		stringBuffer.append("number of corrupt entries is "+corruptLogEntries+"\n");
		return stringBuffer.toString();
	}
}
