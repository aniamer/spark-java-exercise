package eg.excercise.log;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.Accumulator;

import eg.excercise.log.input.LogItem;

public class LogEntryValidator {
	private static final Pattern regexPattern = Pattern
			.compile("(\\d{4}-\\d{2}-\\d{2})\\s(\\d{2}:\\d{2}:\\d{2})\\s(\\w*)\\s(\\[\\w*\\])(.*)");

	/**
	 * validates a log line against the pattern in case of success returns the
	 * parsed object otherwise returns null
	 **/
	public static LogItem parseLogEntry(String line) {
		Matcher matcher = regexPattern.matcher(line);
		LogItem logItem = null;

		if (matcher.find()) {
			logItem = new LogItem(matcher.group(1), matcher.group(2), matcher.group(3), matcher.group(4),
					matcher.group(5));
		}
		

		return logItem;
	}

}
