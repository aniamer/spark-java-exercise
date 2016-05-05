package eg.excercise.log.input;

import java.io.Serializable;

public class LogItem implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -6192906548902274883L;
	public String date;
	public String time;
	public String component;
	public String level;
	public String message;
	
	
	
	public LogItem(String date, String time, String component, String level, String message) {
		this.date = date;
		this.time = time;
		this.component = component;
		this.level = level;
		this.message = message;
	}
	@Override
	public String toString() {
	
		return this.date+"\t"+this.time+"\t"+this.component+"\t"+this.level+"\t"+this.message;
	}
}
